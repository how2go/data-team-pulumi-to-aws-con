"""
Shopify daily orders Lambda (synchronous, single-invocation).

Invoked once per day at 03:00 UTC by EventBridge.

Modes:
  - Daily (default): no env vars -> fetches 2 days ago full UTC day
  - Backfill: set BACKFILL_START + BACKFILL_END env vars to override window

Pipeline (all in one invocation):
  1. Bulk #1: orders + line items (time-windowed)
  2. Bulk #2: products + publications (whole catalogue, no time filter)
  3. Bulk #3: fulfillment orders + merchant requests
     IMPORTANT: fetched from order_start_dt -> now (UTC), not order_end_dt
  4. Stitch the three JSONL blobs in memory
  5. Write one CSV to S3

CSV schema (18 columns, exact order):
    order_id, order_name, order_date, order_total, order_total_currency,
    attribution, gxo_request_at, line_item_id, sku, item_title, quantity,
    price, price_currency, variant_id, product_id, product_status,
    published_publications, scheduled_publications
"""

import csv
import io
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SHOP_NAME = os.environ.get("SHOP_NAME", "how2go")
API_VERSION = "2026-04"

OUTPUT_PREFIX = "shopify-data/shopify-orders"

GRAPHQL_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/graphql.json"
AUTH_URL = f"https://{SHOP_NAME}.myshopify.com/admin/oauth/access_token"

POLL_INTERVAL_SECONDS = 15
POLL_TIMEOUT_SECONDS = 12 * 60  # 12 minutes per bulk op
DAILY_LAG_DAYS = 2

s3 = boto3.client("s3")

CSV_FIELDS = [
    "order_id",
    "order_name",
    "order_date",
    "order_total",
    "order_total_currency",
    "attribution",
    "gxo_request_at",
    "line_item_id",
    "sku",
    "item_title",
    "quantity",
    "price",
    "price_currency",
    "variant_id",
    "product_id",
    "product_status",
    "published_publications",
    "scheduled_publications",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _today_utc_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_window() -> Tuple[str, str, str]:
    """
    Returns (start_dt, end_dt, mode).

    If BACKFILL_START + BACKFILL_END env vars are BOTH set -> backfill mode.
    Otherwise -> daily mode using the full UTC day from DAILY_LAG_DAYS ago.
    """
    bf_start = os.environ.get("BACKFILL_START")
    bf_end = os.environ.get("BACKFILL_END")

    if bf_start and bf_end:
        return bf_start, bf_end, "backfill"

    target_day = (datetime.now(timezone.utc) - timedelta(days=DAILY_LAG_DAYS)).date()
    return (
        f"{target_day.isoformat()}T00:00:00Z",
        f"{target_day.isoformat()}T23:59:59Z",
        "daily",
    )


def _safe_get(d: Any, *keys: str, default: Any = None) -> Any:
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


# ---------------------------------------------------------------------------
# Shopify auth + GraphQL
# ---------------------------------------------------------------------------

def _get_token(client_id: str, client_secret: str) -> str:
    resp = requests.post(
        AUTH_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
        timeout=60,
    )
    resp.raise_for_status()

    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"Failed to get Shopify token: {resp.text[:500]}")
    return token


def _gql(headers: Dict[str, str], query: str, variables: Optional[dict] = None) -> dict:
    payload: Dict[str, Any] = {"query": query}
    if variables is not None:
        payload["variables"] = variables

    resp = requests.post(GRAPHQL_URL, json=payload, headers=headers, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    if data.get("errors"):
        raise RuntimeError(f"GraphQL errors: {json.dumps(data['errors'])[:1000]}")
    return data


# ---------------------------------------------------------------------------
# Bulk query definitions
# ---------------------------------------------------------------------------

def _orders_bulk_query(start_dt: str, end_dt: str) -> str:
    return f"""
    {{
      orders(
        query: "created_at:>='{start_dt}' AND created_at:<='{end_dt}'",
        sortKey: CREATED_AT
      ) {{
        edges {{
          node {{
            id
            name
            createdAt
            totalPriceSet {{ shopMoney {{ amount currencyCode }} }}
            customerJourneySummary {{ firstVisit {{ source }} }}
            lineItems {{
              edges {{
                node {{
                  id
                  title
                  sku
                  quantity
                  originalUnitPriceSet {{ shopMoney {{ amount currencyCode }} }}
                  variant {{
                    id
                    product {{ id }}
                  }}
                }}
              }}
            }}
          }}
        }}
      }}
    }}
    """


def _products_publications_bulk_query() -> str:
    return """
    {
      products(sortKey: ID) {
        edges {
          node {
            id
            status
            resourcePublicationsV2(onlyPublished: false) {
              edges {
                node {
                  isPublished
                  publication { id name }
                }
              }
            }
          }
        }
      }
    }
    """


def _fulfillment_bulk_query(start_dt: str, end_dt: str) -> str:
    return f"""
    {{
      fulfillmentOrders(
        query: "updated_at:>='{start_dt}' AND updated_at:<='{end_dt}'",
        includeClosed: true,
        sortKey: UPDATED_AT
      ) {{
        edges {{
          node {{
            id
            order {{ id }}
            merchantRequests(kind: FULFILLMENT_REQUEST) {{
              edges {{
                node {{
                  id
                  sentAt
                }}
              }}
            }}
          }}
        }}
      }}
    }}
    """


# ---------------------------------------------------------------------------
# Bulk operation handling
# ---------------------------------------------------------------------------

def _cancel_any_running_bulk(headers: Dict[str, str]) -> None:
    """
    Cancel any in-flight bulk op left over from a prior failed run.
    """
    data = _gql(headers, "{ currentBulkOperation { id status } }")
    op = _safe_get(data, "data", "currentBulkOperation")
    if not op:
        return

    status = op.get("status")
    if status in {"CREATED", "RUNNING"}:
        print(f"Cancelling stale bulk op {op.get('id')} (status={status})")
        _gql(
            headers,
            f"""
            mutation {{
              bulkOperationCancel(id: "{op['id']}") {{
                bulkOperation {{ id status }}
                userErrors {{ message }}
              }}
            }}
            """,
        )


def _run_bulk(headers: Dict[str, str], inner_query: str, label: str) -> List[dict]:
    print(f"{label}: starting bulk operation...")

    mutation = """
    mutation RunBulk($query: String!) {
      bulkOperationRunQuery(query: $query) {
        bulkOperation { id status }
        userErrors { field message }
      }
    }
    """

    data = _gql(headers, mutation, {"query": inner_query})
    errs = _safe_get(data, "data", "bulkOperationRunQuery", "userErrors", default=[]) or []

    if errs:
        msg = json.dumps(errs)
        if "already in progress" in msg.lower():
            print(f"{label}: adopting in-flight bulk op")
            bulk_id = _safe_get(
                _gql(headers, "{ currentBulkOperation { id } }"),
                "data", "currentBulkOperation", "id",
            )
        else:
            raise RuntimeError(f"{label}: userErrors: {msg}")
    else:
        bulk_id = _safe_get(data, "data", "bulkOperationRunQuery", "bulkOperation", "id")

    if not bulk_id:
        raise RuntimeError(f"{label}: no bulk id")

    print(f"{label}: bulk id = {bulk_id}")

    poll_query = """
    query GetBulk($id: ID!) {
      bulkOperation(id: $id) {
        id
        status
        errorCode
        objectCount
        fileSize
        url
        partialDataUrl
      }
    }
    """

    deadline = time.time() + POLL_TIMEOUT_SECONDS
    url = ""

    while time.time() < deadline:
        op = _safe_get(_gql(headers, poll_query, {"id": bulk_id}), "data", "bulkOperation")
        if not op:
            raise RuntimeError(f"{label}: bulkOperation {bulk_id} not found")

        status = op.get("status")
        print(f"{label}: status={status} objects={op.get('objectCount')} bytes={op.get('fileSize')}")

        if status == "COMPLETED":
            url = op.get("url") or op.get("partialDataUrl") or ""
            break

        if status in {"FAILED", "CANCELED", "EXPIRED"}:
            partial = op.get("partialDataUrl")
            if partial:
                print(f"{label}: terminal {status}, using partialDataUrl")
                url = partial
                break
            raise RuntimeError(f"{label}: ended {status} errorCode={op.get('errorCode')}")

        time.sleep(POLL_INTERVAL_SECONDS)
    else:
        raise RuntimeError(f"{label}: timed out after {POLL_TIMEOUT_SECONDS}s")

    if not url:
        print(f"{label}: empty result set")
        return []

    print(f"{label}: downloading JSONL...")
    resp = requests.get(url, timeout=600)
    resp.raise_for_status()

    nodes: List[dict] = []
    for raw in resp.text.splitlines():
        if not raw.strip():
            continue
        try:
            nodes.append(json.loads(raw))
        except json.JSONDecodeError:
            pass

    print(f"{label}: fetched {len(nodes):,} JSONL nodes")
    return nodes


# ---------------------------------------------------------------------------
# Stitch: build lookups and produce CSV rows
# ---------------------------------------------------------------------------

def _build_product_lookups(
    nodes: List[dict],
) -> Tuple[Dict[str, str], Dict[str, List[str]], Dict[str, List[str]]]:
    product_status: Dict[str, str] = {}
    published: Dict[str, List[str]] = {}
    scheduled: Dict[str, List[str]] = {}

    for node in nodes:
        if "__parentId" not in node and "id" in node and "status" in node:
            pid = node["id"]
            product_status[pid] = node.get("status") or ""
            published.setdefault(pid, [])
            scheduled.setdefault(pid, [])
            continue

        if "__parentId" in node and "publication" in node:
            pid = node["__parentId"]
            pname = _safe_get(node, "publication", "name") or ""
            if not pname:
                continue

            if node.get("isPublished") is True:
                published.setdefault(pid, []).append(pname)
            else:
                scheduled.setdefault(pid, []).append(pname)

    for m in (published, scheduled):
        for pid in list(m.keys()):
            m[pid] = sorted(set(m[pid]))

    return product_status, published, scheduled


def _build_gxo_lookup(nodes: List[dict]) -> Dict[str, str]:
    """
    From fulfillment JSONL, build order_id -> earliest merchant-request sentAt.
    """
    fo_to_order: Dict[str, str] = {}
    fo_to_earliest_sent: Dict[str, str] = {}

    for node in nodes:
        if "__parentId" not in node and "id" in node and "order" in node:
            oid = _safe_get(node, "order", "id")
            if oid:
                fo_to_order[node["id"]] = oid

        elif "__parentId" in node and "sentAt" in node:
            fo_id = node["__parentId"]
            sent = node.get("sentAt")
            if not sent:
                continue

            prev = fo_to_earliest_sent.get(fo_id)
            if prev is None or sent < prev:
                fo_to_earliest_sent[fo_id] = sent

    order_to_gxo: Dict[str, str] = {}
    for fo_id, order_id in fo_to_order.items():
        sent = fo_to_earliest_sent.get(fo_id)
        if not sent:
            continue

        prev = order_to_gxo.get(order_id)
        if prev is None or sent < prev:
            order_to_gxo[order_id] = sent

    return order_to_gxo


def _build_csv_rows(
    order_nodes: List[dict],
    product_status: Dict[str, str],
    published: Dict[str, List[str]],
    scheduled: Dict[str, List[str]],
    order_to_gxo: Dict[str, str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    current_order: Dict[str, Any] = {}
    current_order_id: Optional[str] = None

    for node in order_nodes:
        if "__parentId" not in node and "id" in node and "name" in node and "createdAt" in node:
            current_order = node
            current_order_id = node["id"]
            continue

        if "__parentId" in node and "quantity" in node and ("sku" in node or "title" in node):
            order = current_order if node.get("__parentId") == current_order_id else {}
            order_id = order.get("id")
            product_id = _safe_get(node, "variant", "product", "id")

            rows.append({
                "order_id": order_id,
                "order_name": order.get("name"),
                "order_date": order.get("createdAt"),
                "order_total": _safe_get(order, "totalPriceSet", "shopMoney", "amount"),
                "order_total_currency": _safe_get(order, "totalPriceSet", "shopMoney", "currencyCode"),
                "attribution": _safe_get(order, "customerJourneySummary", "firstVisit", "source") or "Direct",
                "gxo_request_at": order_to_gxo.get(order_id, ""),
                "line_item_id": node.get("id"),
                "sku": node.get("sku"),
                "item_title": node.get("title"),
                "quantity": node.get("quantity"),
                "price": _safe_get(node, "originalUnitPriceSet", "shopMoney", "amount"),
                "price_currency": _safe_get(node, "originalUnitPriceSet", "shopMoney", "currencyCode"),
                "variant_id": _safe_get(node, "variant", "id"),
                "product_id": product_id,
                "product_status": product_status.get(product_id, "") if product_id else "",
                "published_publications": ", ".join(published.get(product_id, [])) if product_id else "",
                "scheduled_publications": ", ".join(scheduled.get(product_id, [])) if product_id else "",
            })

    return rows


def _write_csv_to_s3(bucket: str, run_date: str, rows: List[Dict[str, Any]]) -> str:
    key = f"{OUTPUT_PREFIX}/{run_date}/{run_date}.csv"

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_FIELDS)
    writer.writeheader()
    writer.writerows(rows)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    print(f"Wrote s3://{bucket}/{key} ({len(rows):,} rows)")
    return key


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(event, context):
    bucket = os.environ["S3_BUCKET_NAME"]
    client_id = os.environ["SHOPIFY_CLIENT_ID"]
    client_secret = os.environ["SHOPIFY_ACCESS_TOKEN"]

    run_date = _today_utc_date_str()
    start_dt, end_dt, mode = _resolve_window()

    # GXO FIX:
    # orders window stays tight
    # fulfillment window extends from order start -> now
    fulfillment_end_dt = _now_utc_iso()

    print(
        f"Starting orders sync: mode={mode} run_date={run_date} "
        f"order_window={start_dt} -> {end_dt} "
        f"fulfillment_window={start_dt} -> {fulfillment_end_dt}"
    )

    token = _get_token(client_id, client_secret)
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }

    _cancel_any_running_bulk(headers)
    time.sleep(2)

    order_nodes = _run_bulk(headers, _orders_bulk_query(start_dt, end_dt), "orders")
    product_nodes = _run_bulk(headers, _products_publications_bulk_query(), "products")
    fulfill_nodes = _run_bulk(
        headers,
        _fulfillment_bulk_query(start_dt, fulfillment_end_dt),
        "fulfillment",
    )

    print("Building product lookups...")
    product_status, published, scheduled = _build_product_lookups(product_nodes)
    print(f"  products: {len(product_status):,}")

    print("Building GXO lookup...")
    order_to_gxo = _build_gxo_lookup(fulfill_nodes)
    print(f"  orders with GXO ts: {len(order_to_gxo):,}")

    print("Building CSV rows...")
    rows = _build_csv_rows(order_nodes, product_status, published, scheduled, order_to_gxo)

    if not rows:
        print("No rows produced; skipping S3 write.")
        return {
            "status": "no_data",
            "mode": mode,
            "run_date": run_date,
            "order_window": f"{start_dt} -> {end_dt}",
            "fulfillment_window": f"{start_dt} -> {fulfillment_end_dt}",
        }

    key = _write_csv_to_s3(bucket, run_date, rows)

    return {
        "status": "success",
        "mode": mode,
        "run_date": run_date,
        "order_window": f"{start_dt} -> {end_dt}",
        "fulfillment_window": f"{start_dt} -> {fulfillment_end_dt}",
        "file": key,
        "rows": len(rows),
    }