import os
import json
import base64
import boto3
import snowflake.connector
import pandas as pd
from cryptography.hazmat.primitives import serialization

# Initialize SQS client
sqs = boto3.client('sqs')
QUEUE_URL = os.environ['SQS_QUEUE_URL']

def get_private_key():
    key_base64 = os.environ["SNOWFLAKE_PRIVATE_KEY"]
    key_bytes = base64.b64decode(key_base64)
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
        private_key=get_private_key(),
    )

    # 1. Pull the brand entity IDs prioritized by Rank 1 Topic
    # We use MAX to collapse nulls and pick the Entity ID if it exists
    query = """
    SELECT 
        BRAND, 
        MAX(CASE WHEN TYPE = 'Topic' AND RANK = 1 THEN ENTITY_ID ELSE NULL END) as ENTITY_ID
    FROM HEALF.GOOGLE_ADS.GOOGLE_TRENDS_BRANDS_ENTITY_IDS 
    GROUP BY BRAND limit 7;
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # Convert to list of dicts for SQS
    brands = df.to_dict(orient='records')
    print(f"✅ Found {len(brands)} brands. Pushing to SQS...")

    # 2. Push each brand as a message to SQS
    for brand in brands:
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(brand)
        )

    print(f"🚀 Successfully dispatched {len(brands)} messages to SQS.")
    return {"status": "success", "count": len(brands)}