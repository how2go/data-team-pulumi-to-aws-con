import os
import json
import base64
from io import StringIO

import boto3
import requests
import pandas as pd

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization


s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")


def get_json_secret(secret_id: str) -> dict:
    resp = secrets.get_secret_value(SecretId=secret_id)
    if "SecretString" not in resp or not resp["SecretString"]:
        raise ValueError(f"SecretString empty for secret: {secret_id}")
    return json.loads(resp["SecretString"])


def get_serpapi_key() -> str:
    secret_id = os.environ["SERPAPI_SECRET_ID"]  # "serpapi_key"
    data = get_json_secret(secret_id)
    if "serpapi_key" not in data:
        raise KeyError("SerpAPI secret must contain JSON key: serpapi_key")
    return data["serpapi_key"]


def get_private_key_der_from_sm() -> bytes:
    secret_id = os.environ["SNOWFLAKE_PRIVATE_KEY_SECRET_ID"]  # "snowflake_private_key"
    data = get_json_secret(secret_id)

    if "snowflake_private_key_base64" not in data:
        raise KeyError("Snowflake key secret must contain JSON key: snowflake_private_key_base64")

    key_b64 = data["snowflake_private_key_base64"]
    key_bytes = base64.b64decode(key_b64)

    p_key = serialization.load_pem_private_key(key_bytes, password=None)

    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def safe_filename(value: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(value))


def upload_df_to_s3(df: pd.DataFrame, bucket: str, start_date: pd.Timestamp, brand: str) -> str:
    start_folder = start_date.strftime("%d-%m-%Y")  # folder per run-date
    run_ts = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")  # unique suffix

    buf = StringIO()
    df.to_csv(buf, index=False)

    key = f"google_trends/{start_folder}/{safe_filename(brand)}_interest_over_time_{run_ts}.csv"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    return key


def fetch_serpapi_trends(q: str, timeframe: str, api_key: str) -> pd.DataFrame:
    url = "https://serpapi.com/search.json"
    params = {
        "engine": "google_trends",
        "q": q,
        "data_type": "TIMESERIES",
        "date": timeframe,
        "geo": "GB",
        "api_key": api_key,
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    iot = data.get("interest_over_time", {}).get("timeline_data", [])
    if not iot:
        return pd.DataFrame()

    rows = []
    for entry in iot:
        rows.append({
            "DATE": entry["timestamp"],
            "SEARCH_INTEREST": entry["values"][0]["extracted_value"],
        })
    return pd.DataFrame(rows)


def handler(event, context):
    # Pull secrets
    api_key = get_serpapi_key()
    s3_bucket = os.environ["S3_BUCKET"]

    # Date window: last 9 to last 3 days
    today_date = pd.Timestamp.today()
    start_date = today_date - pd.Timedelta(days=9)
    end_date = today_date - pd.Timedelta(days=3)
    timeframe = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"

    # Build one Snowflake connection for entire invocation
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ.get("SNOWFLAKE_ROLE", "PC_THOUGHTSPOT_ROLE"),
        private_key=get_private_key_der_from_sm(),
    )

    ok = 0
    fail = 0

    try:
        records = event.get("Records", [])
        if not records:
            print("No Records in event")
            return {"status": "no_records"}

        for record in records:
            try:
                body = json.loads(record["body"])
                brand_name = body["BRAND"]
                entity_id = body.get("ENTITY_ID")

                search_term = entity_id if entity_id and str(entity_id).lower() != "none" else brand_name
                print(f"SerpAPI: BRAND={brand_name}, TERM={search_term}, timeframe={timeframe}")

                df_iot = fetch_serpapi_trends(search_term, timeframe, api_key)

                if not df_iot.empty:
                    df_iot["DATE"] = pd.to_datetime(df_iot["DATE"], unit="s")
                    df_iot.set_index("DATE", inplace=True)
                    df_iot = df_iot.resample("D").mean().round(0).astype("Int64").reset_index()
                    df_iot["DATE"] = df_iot["DATE"].dt.strftime("%Y-%m-%d")
                else:
                    df_iot = pd.DataFrame([{
                        "DATE": start_date.strftime("%Y-%m-%d"),
                        "SEARCH_INTEREST": None
                    }])

                df_iot["BRAND"] = brand_name
                df_iot["METRIC_TYPE"] = "interest_over_time"
                df_iot["RELATED_QUERY"] = None

                clean_df = df_iot[["DATE", "BRAND", "METRIC_TYPE", "RELATED_QUERY", "SEARCH_INTEREST"]]

                # S3 output
                s3_key = upload_df_to_s3(clean_df, s3_bucket, start_date, brand_name)
                print(f"Uploaded: s3://{s3_bucket}/{s3_key}")

                # Snowflake ingestion
                write_pandas(conn, clean_df, "GOOGLE_TRENDS")
                print(f"Snowflake OK: {brand_name}")

                ok += 1

            except Exception as e:
                fail += 1
                print(f"FAILED record: {e}")

    finally:
        conn.close()

    return {"status": "batch_complete", "ok": ok, "fail": fail}