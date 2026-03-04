import os
import json
import time
import pandas as pd
from pytrends.request import TrendReq
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import base64
from cryptography.hazmat.primitives import serialization

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
    pt = TrendReq(hl='en-GB', tz=0)
    today_date = pd.Timestamp.today()
    # Correct offsets for Feb 23 - Mar 01
    start_date = today_date - pd.Timedelta(days=9) 
    end_date = today_date - pd.Timedelta(days=3)
    TIMEFRAME = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"

    for record in event['Records']:
        brand_data = json.loads(record['body'])
        brand_name = brand_data['BRAND']
        entity_id = brand_data.get('ENTITY_ID')
        
        # Fallback to Brand Name if Entity ID is missing
        search_term = entity_id if entity_id and str(entity_id).lower() != 'none' else brand_name
        
        try:
            pt.build_payload([search_term], timeframe=TIMEFRAME, geo='GB')
            df_iot = pt.interest_over_time()
            
            if not df_iot.empty:
                df_iot.index = pd.to_datetime(df_iot.index)
                df_iot = df_iot.resample('D').mean().round(0).astype(int)
                df_iot['DATE'] = df_iot.index.strftime('%Y-%m-%d')
                df_iot = df_iot.reset_index(drop=True)
                df_iot = df_iot.rename(columns={search_term: 'SEARCH_INTEREST'})
            else:
                df_iot = pd.DataFrame([{'DATE': start_date.strftime('%Y-%m-%d'), 'SEARCH_INTEREST': None}])

            df_iot['BRAND'] = brand_name
            df_iot['METRIC_TYPE'] = 'interest_over_time'
            df_iot['RELATED_QUERY'] = None
            
            clean_df = df_iot[['DATE', 'BRAND', 'METRIC_TYPE', 'RELATED_QUERY', 'SEARCH_INTEREST']]
            
            conn = snowflake.connector.connect(
                user=os.environ["SNOWFLAKE_USER"],
                account=os.environ["SNOWFLAKE_ACCOUNT"],
                warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
                database="HEALF",
                schema="GOOGLE_ADS",
                role="PC_THOUGHTSPOT_ROLE",
                private_key=get_private_key(),
            )
            write_pandas(conn, clean_df, "GOOGLE_TRENDS")
            conn.close()
            print(f"✅ Success: {brand_name}")
        except Exception as e:
            print(f"❌ Error for {brand_name}: {e}")
                    
    return {"status": "batch_complete"}