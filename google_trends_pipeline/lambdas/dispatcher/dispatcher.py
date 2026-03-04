import os
import json
import base64
import boto3
import snowflake.connector
from cryptography.hazmat.primitives import serialization

sqs = boto3.client("sqs")
secrets = boto3.client("secretsmanager")

QUEUE_URL = os.environ["SQS_QUEUE_URL"]


def get_json_secret(secret_id: str) -> dict:
    resp = secrets.get_secret_value(SecretId=secret_id)
    if "SecretString" not in resp or not resp["SecretString"]:
        raise ValueError(f"SecretString empty for secret: {secret_id}")
    return json.loads(resp["SecretString"])


def get_private_key_der_from_sm() -> bytes:
    secret_id = os.environ["SNOWFLAKE_PRIVATE_KEY_SECRET_ID"]  # "snowflake_private_key"
    data = get_json_secret(secret_id)

    if "snowflake_private_key_base64" not in data:
        raise KeyError("Snowflake secret must contain JSON key: snowflake_private_key_base64")

    key_b64 = data["snowflake_private_key_base64"]
    key_bytes = base64.b64decode(key_b64)
    p_key = serialization.load_pem_private_key(key_bytes, password=None)

    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def handler(event, context):
    print("❄️ Connecting to Snowflake to fetch Entity IDs...")

    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="HEALF",
        schema="GOOGLE_ADS",
        role="PC_THOUGHTSPOT_ROLE",
        private_key=get_private_key_der_from_sm(),
    )

    query = """
    SELECT 
        BRAND, 
        MAX(CASE WHEN TYPE = 'Topic' AND RANK = 1 THEN ENTITY_ID ELSE NULL END) as ENTITY_ID
    FROM HEALF.GOOGLE_ADS.GOOGLE_TRENDS_BRANDS_ENTITY_IDS 
    GROUP BY BRAND
    LIMIT 7;
    """

    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()  # list of tuples
    finally:
        conn.close()

    brands = []
    for brand, entity_id in rows:
        brands.append({"BRAND": brand, "ENTITY_ID": entity_id})

    print(f"✅ Found {len(brands)} brands. Pushing to SQS...")

    sent = 0
    for brand in brands:
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(brand),
        )
        sent += 1

    print(f"🚀 Successfully dispatched {sent} messages to SQS.")
    return {"status": "success", "count": sent}