import os
import json
import time
import random
import warnings
import pandas as pd
from pytrends.request import TrendReq
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import base64
from cryptography.hazmat.primitives import serialization

warnings.simplefilter(action='ignore', category=FutureWarning)

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
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept-Language': 'en-GB,en;q=0.9'
    }
    pt = TrendReq(hl='en-GB', tz=0, requests_args={'headers': headers})
    
    GEO = 'GB'
    today_date = pd.Timestamp.today()
    start_date = today_date - pd.Timedelta(days=9) 
    end_date = today_date - pd.Timedelta(days=3)
    TIMEFRAME = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"

    for record in event['Records']:
        brand_data = json.loads(record['body'])
        brand_name = brand_data['BRAND']
        # Use Entity ID if available, otherwise fallback to Brand Name 
        search_term = brand_data['ENTITY_ID'] if brand_data['ENTITY_ID'] else brand_name
        
        print(f"🚀 Processing: {brand_name} | Term: {search_term}")

        for attempt in range(1, 4):
            try:
                pt.build_payload([search_term], timeframe=TIMEFRAME, geo=GEO)
                df_iot = pt.interest_over_time()
                
                if not df_iot.empty:
                    # Resampling and Cleaning Logic [cite: 467, 469]
                    df_iot.index = pd.to_datetime(df_iot.index)
                    df_iot = df_iot.resample('D').mean().round(0).astype(int)
                    df_iot['DATE'] = df_iot.index.strftime('%Y-%m-%d')
                    df_iot = df_iot.reset_index(drop=True)
                    df_iot = df_iot.rename(columns={search_term: 'SEARCH_INTEREST'})
                else:
                    # If no data, create a null entry for tracking 
                    print(f"⚠️ No data returned for {brand_name}. Recording null.")
                    df_iot = pd.DataFrame([{
                        'DATE': start_date.strftime('%Y-%m-%d'),
                        'SEARCH_INTEREST': None
                    }])

                # Common fields
                df_iot['BRAND'] = brand_name
                df_iot['METRIC_TYPE'] = 'interest_over_time'
                df_iot['RELATED_QUERY'] = None
                
                clean_df = df_iot[['DATE', 'BRAND', 'METRIC_TYPE', 'RELATED_QUERY', 'SEARCH_INTEREST']]
                
                # 3. Write to Snowflake [cite: 932]
                conn = snowflake.connector.connect(
                    user=os.environ["SNOWFLAKE_USER"],
                    account=os.environ["SNOWFLAKE_ACCOUNT"],
                    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
                    database="HEALF",
                    schema="GOOGLE_ADS",
                    role="PC_THOUGHTSPOT_ROLE",
                    private_key=get_private_key(),
                )
                
                # Use write_pandas for efficient bulk upload
                write_pandas(conn, clean_df, "GOOGLE_TRENDS")
                conn.close()
                break 

            except Exception as e:
                print(f"⚠️ Error: {e}")
                time.sleep(5)
                    
    return {"status": "batch_complete"}