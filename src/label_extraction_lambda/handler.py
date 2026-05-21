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

SOURCE_TABLE = "healf.label_extraction.int_shopify_metadata__enriched_product_variants"


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
    }
    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        kwargs["role"] = role
    return snowflake.connector.connect(**kwargs)


def main(event, context):
    utc_now = datetime.now(timezone.utc)
    folder_name = utc_now.strftime("%Y-%m-%d_%H:%M:%S")
    logger.info("Lambda triggered at %s UTC", folder_name)

    conn = _get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {SOURCE_TABLE}")
            columns = [col[0] for col in cur.description]
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        logger.info("No new variant id today")
        return {"statusCode": 200, "body": "No new variant id today"}

    logger.info("Fetched %d rows from %s", len(rows), SOURCE_TABLE)

    # Write CSV using stdlib — no pandas/numpy needed
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(columns)
    writer.writerows(rows)

    bucket = os.environ["S3_BUCKET_NAME"]
    s3_key = f"edible/{folder_name}/enriched_variants.csv"

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    logger.info("Uploaded %d rows to s3://%s/%s", len(rows), bucket, s3_key)
    return {"statusCode": 200, "body": f"Uploaded {len(rows)} rows to s3://{bucket}/{s3_key}"}
