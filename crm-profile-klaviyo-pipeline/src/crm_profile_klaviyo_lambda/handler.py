import os
import io
import csv
import logging
import base64
from datetime import datetime, timezone

import boto3
import snowflake.connector
from cryptography.hazmat.primitives import serialization

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Source: full snapshot of the CRM profile mart, ordered by customer id.
SOURCE_TABLE = "healf.healf_bi.mart_crm_profile_klaviyo"
ORDER_BY_COLUMN = "CUSTOMER_ID"

# Destination layout: s3://<bucket>/crm-customers/<YYYY-MM-DD>/file_<N>.csv
S3_PREFIX = "crm-customers"
DEFAULT_BATCH_SIZE = 100_000


def _get_private_key_bytes() -> bytes:
    key_base64 = os.environ["SNOWFLAKE_PRIVATE_KEY"]
    key_bytes = base64.b64decode(key_base64)
    p_key = serialization.load_pem_private_key(key_bytes, password=None)
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _get_snowflake_connection():
    kwargs = {
        "user": os.environ["SNOWFLAKE_USER"],
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
        "private_key": _get_private_key_bytes(),
        "ocsp_fail_open": True,   # don't hang if OCSP cert-check times out (common in Lambda)
        "login_timeout": 60,      # fail fast with a clear error instead of hanging 15 min
    }
    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        kwargs["role"] = role
    return snowflake.connector.connect(**kwargs)


def _ensure_prefix_exists(s3_client, bucket, prefix):
    """Make sure the `crm-customers/` folder exists; create a marker if it doesn't.

    S3 has no real folders — a prefix only "exists" once an object lives under it.
    We check for anything under the prefix and, if empty, write a zero-byte folder
    marker so the folder is present/visible even before the first run's files land.
    """
    folder_key = prefix if prefix.endswith("/") else prefix + "/"
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder_key, MaxKeys=1)
    if resp.get("KeyCount", 0) == 0:
        s3_client.put_object(Bucket=bucket, Key=folder_key)
        logger.info("Folder s3://%s/%s did not exist — created it", bucket, folder_key)
    else:
        logger.info("Folder s3://%s/%s already exists", bucket, folder_key)


def _write_batch_to_s3(s3_client, bucket, key, columns, rows, loaded_at):
    """Serialize one batch to CSV (with header) and upload it as a single object.

    Appends a `loaded_at` column to every row with the Lambda run timestamp (UTC).
    All rows in a run share the same value so you can always trace which export
    produced a given row.
    """
    columns_with_ts = list(columns) + ["loaded_at"]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns_with_ts)
    for row in rows:
        writer.writerow(list(row) + [loaded_at])
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )


def main(event, context):
    # Date partition + all timestamps are UTC.
    run_utc = datetime.now(timezone.utc)
    date_folder = run_utc.strftime("%Y-%m-%d")
    loaded_at = run_utc.strftime("%Y-%m-%d %H:%M:%S")  # e.g. 2026-06-17 08:00:00

    bucket = os.environ["S3_BUCKET_NAME"]
    batch_size = int(os.environ.get("BATCH_SIZE", DEFAULT_BATCH_SIZE))

    logger.info(
        "CRM profile export started at %s -> s3://%s/%s/%s/ (batch_size=%d)",
        run_utc.isoformat(), bucket, S3_PREFIX, date_folder, batch_size,
    )

    s3_client = boto3.client("s3")

    # Ensure the crm-customers/ folder exists before we start writing into it.
    _ensure_prefix_exists(s3_client, bucket, S3_PREFIX)

    conn = _get_snowflake_connection()

    total_rows = 0
    file_index = 0
    try:
        with conn.cursor() as cur:
            # Render every TIMESTAMP/DATE column in UTC regardless of its stored tz.
            cur.execute("ALTER SESSION SET TIMEZONE = 'UTC'")

            cur.execute(f"SELECT * FROM {SOURCE_TABLE} ORDER BY {ORDER_BY_COLUMN} ASC")
            columns = [c[0] for c in cur.description]

            # Stream the result set in customer_id order, one file per batch,
            # so memory stays bounded even for very large tables.
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                file_index += 1
                key = f"{S3_PREFIX}/{date_folder}/file_{file_index}.csv"
                _write_batch_to_s3(s3_client, bucket, key, columns, rows, loaded_at)
                total_rows += len(rows)
                logger.info(
                    "Wrote %d rows to s3://%s/%s (cumulative %d)",
                    len(rows), bucket, key, total_rows,
                )
    finally:
        conn.close()

    if file_index == 0:
        logger.warning("Source returned 0 rows — no files written for %s", date_folder)

    msg = (
        f"Exported {total_rows} rows to "
        f"s3://{bucket}/{S3_PREFIX}/{date_folder}/ in {file_index} file(s)"
    )
    logger.info(msg)
    return {"statusCode": 200, "body": msg}
