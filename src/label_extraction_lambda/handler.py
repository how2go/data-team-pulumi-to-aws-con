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

DB_SCHEMA = "healf.label_extraction"

TABLES = [
    {
        "table": f"{DB_SCHEMA}.int_shopify_metadata__enriched_product_variants",
        "s3_folder": "edible",
        "filename": "enriched_variants.csv",
    },
    {
        "table": f"{DB_SCHEMA}.int_shopify_metadata_non_edible__apparel_accessories",
        "s3_folder": "apparel_accessories",
        "filename": "apparel_accessories.csv",
    },
    {
        "table": f"{DB_SCHEMA}.int_shopify_metadata_non_edible__beauty_devices",
        "s3_folder": "beauty_devices",
        "filename": "beauty_devices.csv",
    },
    {
        "table": f"{DB_SCHEMA}.int_shopify_metadata_non_edible__devices_wearables",
        "s3_folder": "devices_wearables",
        "filename": "devices_wearables.csv",
    },
    {
        "table": f"{DB_SCHEMA}.int_shopify_metadata_non_edible__fitness_equipment",
        "s3_folder": "fitness_equipment",
        "filename": "fitness_equipment.csv",
    },
    {
        "table": f"{DB_SCHEMA}.int_shopify_metadata_non_edible__haircare",
        "s3_folder": "haircare",
        "filename": "haircare.csv",
    },
    {
        "table": f"{DB_SCHEMA}.int_shopify_metadata_non_edible__home_environment",
        "s3_folder": "home_environment",
        "filename": "home_environment.csv",
    },
    {
        "table": f"{DB_SCHEMA}.int_shopify_metadata_non_edible__kitchenware",
        "s3_folder": "kitchenware",
        "filename": "kitchenware.csv",
    },
    {
        "table": f"{DB_SCHEMA}.int_shopify_metadata_non_edible__therapy_recovery",
        "s3_folder": "therapy_recovery",
        "filename": "therapy_recovery.csv",
    },
    {
        "table": f"{DB_SCHEMA}.int_shopify_metadata_non_edible__topicals_skincare",
        "s3_folder": "topicals_skincare",
        "filename": "topicals_skincare.csv",
    },
    {
        "table": f"{DB_SCHEMA}.mart_variant_ingredient_lookup",
        "s3_folder": "ingredient_lookup_edible",
        "filename": "ingredient_lookup_edible.csv",
    },
]


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


def _fetch_and_upload(cur, s3_client, bucket, folder_name, table_cfg):
    table = table_cfg["table"]
    s3_key = f"{table_cfg['s3_folder']}/{folder_name}/{table_cfg['filename']}"

    cur.execute(f"SELECT * FROM {table}")
    columns = [col[0] for col in cur.description]
    rows = cur.fetchall()

    if not rows:
        logger.info("No rows in %s — skipping upload", table)
        return 0

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(columns)
    writer.writerows(rows)

    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    logger.info("Uploaded %d rows from %s to s3://%s/%s", len(rows), table, bucket, s3_key)
    return len(rows)


def main(event, context):
    utc_now = datetime.now(timezone.utc)
    folder_name = utc_now.strftime("%Y-%m-%d_%H:%M:%S")
    logger.info("Lambda triggered at %s UTC", folder_name)

    bucket = os.environ["S3_BUCKET_NAME"]
    s3_client = boto3.client("s3")

    conn = _get_snowflake_connection()
    results = []
    try:
        with conn.cursor() as cur:
            for table_cfg in TABLES:
                count = _fetch_and_upload(cur, s3_client, bucket, folder_name, table_cfg)
                results.append({"table": table_cfg["s3_folder"], "rows": count})
    finally:
        conn.close()

    summary = ", ".join(f"{r['table']}={r['rows']}" for r in results)
    logger.info("Done: %s", summary)
    return {"statusCode": 200, "body": f"Uploaded at {folder_name}: {summary}"}
