"""
messages.py — Gorgias /messages extractor (Time-Based Incremental)

MAJOR CHANGE: Instead of relying on cursor (which points to old data),
we now use time-based filtering to fetch only recent messages.

Each run fetches messages from the last 2 hours to ensure we catch everything.
"""

import os
import json
import time
import random
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -------------------- AWS clients --------------------
_ddb = boto3.resource("dynamodb")
_s3 = boto3.client("s3")
_sm = boto3.client("secretsmanager")

# -------------------- Env --------------------
GORGIAS_BASE_URL = os.environ.get("GORGIAS_BASE_URL", "https://healf-uk.gorgias.com/api").rstrip("/")
STATE_TABLE = os.environ["STATE_TABLE"]
S3_BUCKET = os.environ.get("S3_BUCKET", "sources-data")
S3_PREFIX_BASE = os.environ.get("S3_PREFIX_BASE", "gorgias").strip("/")

STREAM_NAME = "messages"
ENDPOINT = "/messages"

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "100"))
PAGES_PER_INVOCATION = int(os.environ.get("PAGES_PER_INVOCATION", "50"))

# Time window for incremental fetches (in hours)
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "2"))

# Rate limit + retries
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "8"))
BACKOFF_BASE_SECONDS = float(os.environ.get("BACKOFF_BASE_SECONDS", "1.0"))
BACKOFF_MAX_SECONDS = float(os.environ.get("BACKOFF_MAX_SECONDS", "30.0"))
MIN_SUCCESS_DELAY_SECONDS = float(os.environ.get("MIN_SUCCESS_DELAY_SECONDS", "0.2"))
THROTTLE_RATIO = float(os.environ.get("THROTTLE_RATIO", "0.70"))
THROTTLE_MAX_SLEEP_SECONDS = float(os.environ.get("THROTTLE_MAX_SLEEP_SECONDS", "2.0"))

# Lambda time handling
REQUEST_TIMEOUT = (10, 60)
TIME_LEFT_BUFFER_MS = int(os.environ.get("TIME_LEFT_BUFFER_MS", "15000"))

# Lease lock
LEASE_SECONDS = int(os.environ.get("LEASE_SECONDS", "180"))
LEASE_RENEW_EVERY_PAGES = int(os.environ.get("LEASE_RENEW_EVERY_PAGES", "5"))
CHECKPOINT_EVERY_PAGES = int(os.environ.get("CHECKPOINT_EVERY_PAGES", "5"))

# Secrets
GORGIAS_EMAIL_SECRET = os.environ.get("GORGIAS_EMAIL_SECRET", "gorgias_email")
GORGIAS_API_KEY_SECRET = os.environ.get("GORGIAS_API_KEY_SECRET", "gorgias_api_key")

TABLE = _ddb.Table(STATE_TABLE)


# -------------------- Helpers --------------------
def _utc_now_ts() -> int:
    return int(time.time())

def _utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _hours_ago_iso(hours: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(hours=hours)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _safe_json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return None

def _get_secret_value(secret_id: str) -> str:
    resp = _sm.get_secret_value(SecretId=secret_id)
    secret = (resp.get("SecretString") or "").strip()
    parsed = _safe_json_loads(secret)
    if isinstance(parsed, dict):
        for k in ["GORGIAS_EMAIL", "GORGIAS_API_KEY", "value", "secret", "token", "api_key"]:
            if k in parsed and isinstance(parsed[k], str) and parsed[k].strip():
                return parsed[k].strip().strip('"').strip("'")
    return secret.strip().strip('"').strip("'")

def _gorgias_auth() -> Tuple[str, str]:
    email = _get_secret_value(GORGIAS_EMAIL_SECRET)
    api_key = _get_secret_value(GORGIAS_API_KEY_SECRET)
    if not email or not api_key:
        raise ValueError("Missing Gorgias credentials")
    return email, api_key

def _time_left_ok(context, buffer_ms: int = TIME_LEFT_BUFFER_MS) -> bool:
    if context is None:
        return True
    try:
        return context.get_remaining_time_in_millis() > buffer_ms
    except Exception:
        return True

def _parse_limit_header(headers: Dict[str, str]) -> Optional[Tuple[int, int]]:
    h = headers.get("X-Gorgias-Account-Api-Call-Limit") or headers.get("x-gorgias-account-api-call-limit")
    if not h:
        return None
    try:
        used_s, limit_s = h.split("/", 1)
        used = int(used_s.strip())
        limit = int(limit_s.strip())
        if limit <= 0:
            return None
        return used, limit
    except Exception:
        return None

def _throttle_on_success(headers: Dict[str, str]) -> None:
    parsed = _parse_limit_header(headers)
    sleep_s = MIN_SUCCESS_DELAY_SECONDS if MIN_SUCCESS_DELAY_SECONDS > 0 else 0.0
    if parsed:
        used, limit = parsed
        ratio = used / limit
        if ratio >= THROTTLE_RATIO:
            ramp = (ratio - THROTTLE_RATIO) / max(1e-9, (1.0 - THROTTLE_RATIO))
            sleep_s = max(sleep_s, min(THROTTLE_MAX_SLEEP_SECONDS, 0.2 + ramp * THROTTLE_MAX_SLEEP_SECONDS))
    if sleep_s > 0:
        time.sleep(sleep_s)

def _write_jsonl_to_s3(stream: str, job_start_id: str, page: int, records: List[Dict[str, Any]], run_ts: str) -> str:
    if not records:
        return ""
    dt = _utc_today_str()
    # Include run timestamp to avoid overwriting files from different runs
    key = f"{S3_PREFIX_BASE}/{stream}/dt={dt}/job={job_start_id}/run={run_ts}/page={page:06d}.json"
    body = "".join(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n" for r in records)
    _s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))
    logger.info(f"[{stream}] wrote s3://{S3_BUCKET}/{key} rows={len(records)}")
    return key

# -------------------- DDB State / Lease --------------------
def _ddb_get(job_start_id: str) -> Optional[Dict[str, Any]]:
    resp = TABLE.get_item(Key={"job_start_id": job_start_id})
    return resp.get("Item")

def _ddb_acquire_lease(job_start_id: str, request_id: str) -> bool:
    now = _utc_now_ts()
    lease_until = now + LEASE_SECONDS
    try:
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            UpdateExpression="SET lease_until=:lu, lease_owner=:lo, in_flight=:t, updated_at=:now",
            ConditionExpression="attribute_not_exists(lease_until) OR lease_until <= :now OR lease_owner = :lo",
            ExpressionAttributeValues={
                ":lu": lease_until,
                ":lo": request_id,
                ":t": True,
                ":now": now,
            },
        )
        return True
    except ClientError as e:
        if e.response["Error"].get("Code") == "ConditionalCheckFailedException":
            return False
        raise

def _ddb_renew_lease(job_start_id: str, request_id: str) -> None:
    now = _utc_now_ts()
    lease_until = now + LEASE_SECONDS
    try:
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            UpdateExpression="SET lease_until=:lu, updated_at=:now",
            ConditionExpression="lease_owner = :lo",
            ExpressionAttributeValues={":lu": lease_until, ":now": now, ":lo": request_id},
        )
    except ClientError as e:
        if e.response["Error"].get("Code") == "ConditionalCheckFailedException":
            return
        raise


def _ddb_checkpoint(job_start_id: str, status: str, page: int, cursor: Optional[str],
                    note: str = "", last_error: str = "", lease_owner: Optional[str] = None,
                    last_fetch_time: Optional[str] = None) -> None:
    """Checkpoint the current state to DynamoDB."""
    now = _utc_now_ts()

    set_parts = [
        "#status=:s", "#page=:p", "#updated_at=:now", 
        "in_flight=:f", "#lease_until=:z"
    ]
    names = {
        "#status": "status", "#page": "page", "#updated_at": "updated_at",
        "#lease_until": "lease_until"
    }
    vals = {
        ":s": status, ":p": page, ":now": now, 
        ":f": False, ":z": 0
    }

    if cursor:
        set_parts.append("#cursor=:c")
        names["#cursor"] = "cursor"
        vals[":c"] = cursor
    
    if note:
        set_parts.append("#note=:n")
        names["#note"] = "note"
        vals[":n"] = note[:2000]

    if last_error:
        set_parts.append("#last_error=:e")
        names["#last_error"] = "last_error"
        vals[":e"] = last_error[:2000]

    if last_fetch_time:
        set_parts.append("#last_fetch_time=:lft")
        names["#last_fetch_time"] = "last_fetch_time"
        vals[":lft"] = last_fetch_time

    remove_parts = []
    if lease_owner:
        remove_parts.append("#lease_owner")
        names["#lease_owner"] = "lease_owner"
        vals[":me"] = lease_owner

    expr = "SET " + ", ".join(set_parts)
    if remove_parts:
        expr += " REMOVE " + ", ".join(remove_parts)

    params = {
        "Key": {"job_start_id": job_start_id},
        "UpdateExpression": expr,
        "ExpressionAttributeNames": names,
        "ExpressionAttributeValues": vals,
    }
    if lease_owner:
        params["ConditionExpression"] = "attribute_not_exists(#lease_owner) OR #lease_owner = :me"

    TABLE.update_item(**params)


def _ddb_done(job_start_id: str, note: str = "", last_fetch_time: Optional[str] = None) -> None:
    """Mark the job as DONE and record when we last fetched."""
    _ddb_checkpoint(
        job_start_id, 
        status="DONE", 
        page=0,
        cursor=None,
        note=note, 
        last_error="",
        last_fetch_time=last_fetch_time
    )
    logger.info(f"[{STREAM_NAME}] Marked DONE, last_fetch_time={last_fetch_time}")


def _ddb_error(job_start_id: str, page: int, cursor: Optional[str], err: str) -> None:
    _ddb_checkpoint(job_start_id, "ERROR", page=page, cursor=cursor, note="error", last_error=err)


# -------------------- API Fetcher --------------------
def _request_with_backoff(session: requests.Session, method: str, url: str, params: Dict[str, Any], auth: Tuple[str, str], bearer_key: str) -> requests.Response:
    backoff = BACKOFF_BASE_SECONDS
    last_resp = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.request(method, url, params=params, auth=auth, timeout=REQUEST_TIMEOUT)
            last_resp = r
            
            if r.status_code == 401:
                r = session.request(method, url, params=params, headers={"Authorization": f"Bearer {bearer_key}"}, timeout=REQUEST_TIMEOUT)
                last_resp = r

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except:
                        sleep_s = backoff
                else:
                    sleep_s = backoff + random.random()
                
                logger.warning(f"[{STREAM_NAME}] 429 rate limited. attempt={attempt}. sleeping {sleep_s:.2f}s")
                time.sleep(sleep_s)
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
                continue
            
            if r.status_code >= 500:
                sleep_s = backoff + random.random()
                logger.warning(f"[{STREAM_NAME}] {r.status_code} server error. attempt={attempt}. sleeping {sleep_s:.2f}s")
                time.sleep(sleep_s)
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
                continue
            
            if r.status_code >= 400:
                logger.error(f"[{STREAM_NAME}] API Error {r.status_code}: {r.text[:500]}")
                r.raise_for_status()

            return r

        except requests.exceptions.RequestException as e:
            logger.warning(f"[{STREAM_NAME}] Network error: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)

    raise RuntimeError(f"Exceeded max retries. Last status: {last_resp.status_code if last_resp else 'None'}")


def _fetch_page(session, cursor: Optional[str], since_datetime: str):
    """
    Fetches a single page of messages.
    Uses time-based filtering to only get recent messages.
    """
    email, api_key = _gorgias_auth()
    url = f"{GORGIAS_BASE_URL}{ENDPOINT}"
    
    params = {
        "limit": PAGE_SIZE,
        "order_by": "created_datetime:asc",  # Get oldest first within window
    }
    
    # Always filter by time - this is the KEY change
    params["created_datetime[gte]"] = since_datetime
    
    # Use cursor for pagination within the time window
    if cursor:
        params["cursor"] = cursor

    logger.info(f"[{STREAM_NAME}] Fetching since={since_datetime} cursor={cursor[:30] if cursor else 'START'}...")

    r = _request_with_backoff(session, "GET", url, params, (email, api_key), api_key)
    headers = dict(r.headers)
    _throttle_on_success(headers)

    j = r.json()
    items = j.get("data") or j.get("messages") or []
    
    meta = j.get("meta") or {}
    next_cursor = meta.get("next_cursor") or meta.get("cursor_next")
    
    return items, next_cursor, headers


# -------------------- Main Handler --------------------
def handler(event, context):
    request_id = getattr(context, "aws_request_id", "no_context")
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    logger.info(f"[{STREAM_NAME}] START request_id={request_id} run_ts={run_ts}")
    
    # Parse input
    record = (event.get("Records") or [None])[0] if isinstance(event, dict) else None
    body = {}
    if record and isinstance(record, dict) and record.get("body"):
        try:
            body = json.loads(record["body"])
        except:
            body = {"raw_body": record["body"]}
    elif isinstance(event, dict):
        body = event

    job_start_id = body.get("job_start_id")
    if not job_start_id:
        logger.error(f"[{STREAM_NAME}] Missing job_start_id")
        return {"ok": False, "reason": "missing_job_start_id"}

    # Check State
    state = _ddb_get(job_start_id)
    if not state:
        logger.warning(f"[{STREAM_NAME}] Missing state for {job_start_id}")
        return {"ok": False, "reason": "missing_state"}
    
    if state.get("status") != "RUNNING":
        logger.info(f"[{STREAM_NAME}] Skipping - status={state.get('status')}")
        return {"ok": True, "skipped": True, "status": state.get("status")}

    # Acquire Lease
    if not _ddb_acquire_lease(job_start_id, request_id):
        logger.info(f"[{STREAM_NAME}] Lease busy")
        return {"ok": True, "skipped": True, "reason": "lease_busy"}

    logger.info(f"[{STREAM_NAME}] Lease acquired")

    # Calculate time window for fetching
    # Always look back LOOKBACK_HOURS to catch any messages we might have missed
    since_datetime = _hours_ago_iso(LOOKBACK_HOURS)
    current_time = _utc_now_iso()
    
    logger.info(f"[{STREAM_NAME}] Fetching messages since {since_datetime}")

    # For time-based fetching, we always start fresh with no cursor
    # The cursor is only used for pagination within a single run
    cursor = None
    page = 1
    
    processed = 0
    total_written = 0
    session = requests.Session()
    
    try:
        while processed < PAGES_PER_INVOCATION and _time_left_ok(context):
            if processed > 0 and processed % LEASE_RENEW_EVERY_PAGES == 0:
                _ddb_renew_lease(job_start_id, request_id)

            try:
                items, next_cursor, headers = _fetch_page(session, cursor, since_datetime)
            except RuntimeError as e:
                msg = str(e)
                logger.warning(f"[{STREAM_NAME}] Transient failure: {msg}")
                _ddb_checkpoint(job_start_id, "RUNNING", page, cursor, note=f"paused: {msg}")
                return {"ok": True, "done": False, "reason": "transient_error"}

            if not items:
                # No more messages in time window - we're done
                _ddb_done(job_start_id, f"completed - fetched {total_written} messages since {since_datetime}", last_fetch_time=current_time)
                return {"ok": True, "done": True, "reason": "no_more_messages", "total_written": total_written}

            # Write items
            _write_jsonl_to_s3(STREAM_NAME, job_start_id, page, items, run_ts)
            total_written += len(items)

            processed += 1
            cursor = next_cursor
            page += 1

            # No more pages
            if not cursor:
                _ddb_done(job_start_id, f"completed - fetched {total_written} messages since {since_datetime}", last_fetch_time=current_time)
                return {"ok": True, "done": True, "reason": "end_of_results", "total_written": total_written}

            # Checkpoint
            if processed % CHECKPOINT_EVERY_PAGES == 0:
                _ddb_checkpoint(job_start_id, "RUNNING", page, cursor, note=f"page {page}, wrote {total_written}")
                logger.info(f"[{STREAM_NAME}] Checkpoint at page {page}")

        # Pausing (time/page limit)
        logger.info(f"[{STREAM_NAME}] Pausing at page {page} (wrote {total_written} this run)")
        _ddb_checkpoint(job_start_id, "RUNNING", page, cursor, note=f"paused at page {page}")
        return {"ok": True, "done": False, "total_written": total_written}

    except Exception as e:
        logger.exception(f"[{STREAM_NAME}] Fatal Error")
        _ddb_error(job_start_id, page, cursor, str(e))
        raise