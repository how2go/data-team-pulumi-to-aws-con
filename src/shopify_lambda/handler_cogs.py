"""
Shopify daily COGS snapshot Lambda.

Invoked once a day at 04:00 UTC by an EventBridge rule. Each invocation
captures a point-in-time snapshot of the entire Shopify product catalogue
and writes one CSV to S3.

Output key:
    s3://{S3_BUCKET_NAME}/shopify-data/shopify-cogs/{run_date}/{run_date}.csv

  where run_date is the UTC date the Lambda RAN. The snapshot reflects
  Shopify's catalogue at the moment the bulk operation completed.

Pipeline (synchronous, single invocation):
    1. Bulk query #1: products + variants (id, sku, price, unitCost, vendor, status)
    2. Bulk query #2: products + publications (resourcePublicationsV2)
    3. Stitch in memory: every variant gets its parent product's publications
    4. Write one CSV to S3

Why two bulk queries:
    Shopify GraphQL bulk operations have strict connection-nesting rules.
    Combining variants + resourcePublicationsV2 in one query violates them.
    Two sequential bulk operations (each ~2-5 min for Healf's catalogue)
    fits comfortably in one 15-min Lambda invocation.

Why no state machine (unlike handler_orders):
    A full catalogue snapshot is small (~50K products at Healf) and finishes
    well within Lambda's 15-minute timeout. No checkpointing needed.

Env vars required:
    SHOPIFY_CLIENT_ID        (public app key)
    SHOPIFY_ACCESS_TOKEN     (client secret for client_credentials grant)
    S3_BUCKET_NAME           (output goes here)
    SHOP_NAME                (optional; defaults to 'how2go')

CSV columns:
    snapshot_date, product_id, product_name, brand_name,
    variant_id, sku, current_selling_price, current_cogs,
    product_status, published_publications, scheduled_publications
"""

import csv
import io
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SHOP_NAME = os.environ.get("SHOP_NAME", "how2go")
API_VERSION = "2026-04"

OUTPUT_PREFIX = "shopify-data/shopify-cogs"

GRAPHQL_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/graphql.json"
AUTH_URL = f"https://{SHOP_NAME}.myshopify.com/admin/oauth/access_token"

# Polling cadence for bulk-op completion.
POLL_INTERVAL_SECONDS = 30
POLL_TIMEOUT_SECONDS = 12 * 60  # 12 minutes per bulk op (leaves headroom)

# ---------------------------------------------------------------------------
# AWS clients (module-scope so Lambda container reuse is cheap)
# ---------------------------------------------------------------------------

s3 = boto3.client("s3")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _today_utc_date_str() -> str:
    """Folder + filename for today's snapshot, e.g. '2026-04-21'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


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
# Shopify auth
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
        raise RuntimeError(f"Failed to get Shopify token: {resp.text[:200]}")
    return token


# ---------------------------------------------------------------------------
# Bulk operation lifecycle: start, poll, download
# ---------------------------------------------------------------------------

def _start_bulk_query(headers: Dict[str, str], inner_query: str) -> str:
    """Kick off a bulk operation, return the operation id."""
    mutation = """
    mutation RunBulk($query: String!) {
      bulkOperationRunQuery(query: $query) {
        bulkOperation { id status }
        userErrors { field message }
      }
    }
    """
    payload = {"query": mutation, "variables": {"query": inner_query}}
    resp = requests.post(GRAPHQL_URL, json=payload, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if data.get("errors"):
        raise RuntimeError(f"GraphQL errors starting bulk: {json.dumps(data['errors'])[:500]}")

    errs = _safe_get(data, "data", "bulkOperationRunQuery", "userErrors", default=[]) or []
    if errs:
        msg = json.dumps(errs)
        # If a bulk op is already in progress (e.g. left over from a previous
        # invocation), adopt it instead of erroring.
        if "already in progress" in msg.lower():
            print(f"Bulk already in progress; adopting current op. ({msg})")
            return _current_bulk_id(headers)
        raise RuntimeError(f"bulkOperationRunQuery userErrors: {msg}")

    bulk_id = _safe_get(data, "data", "bulkOperationRunQuery", "bulkOperation", "id")
    if not bulk_id:
        raise RuntimeError("No bulk operation id returned")
    return bulk_id


def _current_bulk_id(headers: Dict[str, str]) -> str:
    """Return the id of the in-flight bulk op, if any."""
    payload = {"query": "{ currentBulkOperation { id status } }"}
    resp = requests.post(GRAPHQL_URL, json=payload, headers=headers, timeout=60)
    resp.raise_for_status()
    bid = _safe_get(resp.json(), "data", "currentBulkOperation", "id")
    if not bid:
        raise RuntimeError("currentBulkOperation returned no id")
    return bid


def _wait_for_bulk(headers: Dict[str, str], bulk_id: str, label: str) -> str:
    """
    Poll the bulk op every POLL_INTERVAL_SECONDS until it reaches a terminal
    state. Returns the download URL on COMPLETED, raises on FAILED/CANCELED.
    """
    poll_query = """
    query GetBulk($id: ID!) {
      bulkOperation(id: $id) {
        id status errorCode createdAt completedAt
        objectCount fileSize url partialDataUrl
      }
    }
    """
    deadline = time.time() + POLL_TIMEOUT_SECONDS

    while time.time() < deadline:
        payload = {"query": poll_query, "variables": {"id": bulk_id}}
        resp = requests.post(GRAPHQL_URL, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
        op = _safe_get(resp.json(), "data", "bulkOperation")

        if not op:
            raise RuntimeError(f"{label}: bulkOperation {bulk_id} not found")

        status = op.get("status")
        print(f"{label}: status={status} objects={op.get('objectCount')} bytes={op.get('fileSize')}")

        if status == "COMPLETED":
            url = op.get("url") or op.get("partialDataUrl")
            if not url:
                # Empty result set: completed but no download URL.
                print(f"{label}: completed but no URL (empty result set).")
                return ""
            return url

        if status in {"FAILED", "CANCELED", "EXPIRED"}:
            partial = op.get("partialDataUrl")
            if partial:
                print(f"{label}: terminal status {status} but partialDataUrl present; using it.")
                return partial
            raise RuntimeError(f"{label}: bulk op ended {status} errorCode={op.get('errorCode')}")

        # CREATED / RUNNING -- keep waiting.
        time.sleep(POLL_INTERVAL_SECONDS)

    raise RuntimeError(f"{label}: timed out after {POLL_TIMEOUT_SECONDS}s waiting for bulk op")


def _download_jsonl(url: str) -> List[dict]:
    """Download a Shopify bulk-result JSONL URL and parse it into a list of dicts."""
    if not url:
        return []
    resp = requests.get(url, timeout=600)
    resp.raise_for_status()
    nodes: List[dict] = []
    for raw in resp.text.splitlines():
        if not raw.strip():
            continue
        try:
            nodes.append(json.loads(raw))
        except json.JSONDecodeError as e:
            print(f"Skipping malformed line: {e}")
    return nodes


# ---------------------------------------------------------------------------
# Bulk query definitions
# ---------------------------------------------------------------------------

PRODUCTS_VARIANTS_QUERY = """
{
  products {
    edges {
      node {
        id
        vendor
        title
        status
        variants {
          edges {
            node {
              id
              sku
              price
              inventoryItem {
                unitCost {
                  amount
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

PRODUCTS_PUBLICATIONS_QUERY = """
{
  products(sortKey: ID) {
    edges {
      node {
        id
        resourcePublicationsV2(onlyPublished: false) {
          edges {
            node {
              isPublished
              publishDate
              publication { id name }
            }
          }
        }
      }
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Stitch + write
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "snapshot_date",
    "product_id", "product_name", "brand_name",
    "variant_id", "sku",
    "current_selling_price", "current_cogs",
    "product_status",
    "published_publications", "scheduled_publications",
]


def _build_publications_lookup(nodes: List[dict]) -> tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """
    From the products+publications JSONL nodes, build two dicts:
      product_id -> sorted list of published publication names
      product_id -> sorted list of scheduled (not yet published) publication names
    """
    published: Dict[str, List[str]] = {}
    scheduled: Dict[str, List[str]] = {}

    for node in nodes:
        # Parent product node: ensure key exists even if no publications follow.
        if "__parentId" not in node and "id" in node and "resourcePublicationsV2" not in node:
            published.setdefault(node["id"], [])
            scheduled.setdefault(node["id"], [])
            continue
        # Parent with inline publications field would be unusual but handle it.
        if "__parentId" not in node and "id" in node:
            published.setdefault(node["id"], [])
            scheduled.setdefault(node["id"], [])
            continue
        # Child publication node.
        if "__parentId" in node and "publication" in node:
            pid = node["__parentId"]
            pname = _safe_get(node, "publication", "name") or ""
            if not pname:
                continue
            if node.get("isPublished") is True:
                published.setdefault(pid, []).append(pname)
            else:
                scheduled.setdefault(pid, []).append(pname)

    # Dedupe + sort.
    for m in (published, scheduled):
        for pid in list(m.keys()):
            m[pid] = sorted(set(m[pid]))

    return published, scheduled


def _build_csv_rows(
    products_nodes: List[dict],
    published: Dict[str, List[str]],
    scheduled: Dict[str, List[str]],
    snapshot_date: str,
) -> List[Dict[str, Any]]:
    """
    Walk the products+variants JSONL, joining each variant against its parent
    product (from a rolling cache) and the publications lookups.
    """
    products_cache: Dict[str, Dict[str, Any]] = {}
    rows: List[Dict[str, Any]] = []

    for node in products_nodes:
        # Parent product node: cache its title/vendor/status.
        if "__parentId" not in node and "title" in node:
            products_cache[node["id"]] = {
                "vendor": node.get("vendor") or "",
                "title": node.get("title") or "",
                "status": node.get("status") or "",
            }
            continue

        # Child variant node.
        if "__parentId" in node and ("sku" in node or "price" in node):
            parent_id = node.get("__parentId")
            product = products_cache.get(parent_id, {})

            inv_item = node.get("inventoryItem") or {}
            unit_cost = (inv_item.get("unitCost") or {}).get("amount")

            rows.append({
                "snapshot_date": snapshot_date,
                "product_id": parent_id,
                "product_name": product.get("title", ""),
                "brand_name": product.get("vendor", ""),
                "variant_id": node.get("id"),
                "sku": node.get("sku") or "",
                "current_selling_price": node.get("price") or "",
                "current_cogs": unit_cost if unit_cost is not None else "",
                "product_status": product.get("status", ""),
                "published_publications": ", ".join(published.get(parent_id, [])),
                "scheduled_publications": ", ".join(scheduled.get(parent_id, [])),
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
    print(f"Starting COGS snapshot for run_date={run_date}")

    # 1. Auth.
    token = _get_token(client_id, client_secret)
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }

    # 2. Bulk #1: products + variants (the COGS data).
    print("Bulk #1: products + variants...")
    bulk_id = _start_bulk_query(headers, PRODUCTS_VARIANTS_QUERY)
    print(f"  bulk id: {bulk_id}")
    pv_url = _wait_for_bulk(headers, bulk_id, label="products+variants")
    products_nodes = _download_jsonl(pv_url)
    print(f"  fetched {len(products_nodes):,} JSONL nodes")

    # 3. Bulk #2: products + publications (the visibility/channel data).
    print("Bulk #2: products + publications...")
    bulk_id = _start_bulk_query(headers, PRODUCTS_PUBLICATIONS_QUERY)
    print(f"  bulk id: {bulk_id}")
    pp_url = _wait_for_bulk(headers, bulk_id, label="products+publications")
    pub_nodes = _download_jsonl(pp_url)
    print(f"  fetched {len(pub_nodes):,} JSONL nodes")

    # 4. Stitch in memory.
    print("Stitching publications lookup...")
    published, scheduled = _build_publications_lookup(pub_nodes)
    print(
        f"  products with publications data: {len(published):,}; "
        f"published entries: {sum(len(v) for v in published.values()):,}; "
        f"scheduled entries: {sum(len(v) for v in scheduled.values()):,}"
    )

    print("Building CSV rows...")
    rows = _build_csv_rows(products_nodes, published, scheduled, run_date)
    if not rows:
        print("No variant rows produced; skipping S3 write.")
        return {"status": "no_data", "run_date": run_date}

    # 5. Write CSV to S3.
    key = _write_csv_to_s3(bucket, run_date, rows)
    return {"status": "success", "run_date": run_date, "file": key, "rows": len(rows)}