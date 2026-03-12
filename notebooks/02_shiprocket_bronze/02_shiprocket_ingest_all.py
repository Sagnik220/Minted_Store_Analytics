# Databricks notebook source
# 02_shiprocket_ingest_all.py - Ingests Shiprocket endpoints to Bronze

# MAGIC %run ../00_setup/00_config
# MAGIC %run ../00_setup/00_utils

import requests
import json
from datetime import datetime
import time

run_id = f"run_{int(time.time())}"
token = get_shiprocket_token(SHIPROCKET_CONFIG)

if not token:
    raise Exception("Failed to authenticate to Shiprocket API")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

for endpoint in SHIPROCKET_ENDPOINTS:
    print(f"Ingesting Shiprocket -> {endpoint}")
    url = f"https://apiv2.shiprocket.in/v1/external/{endpoint}"
    
    # Basic pagination implementation
    all_data = []
    page = 1
    while True:
        response = requests.get(url, headers=headers, params={"per_page": 100, "page": page})
        if response.status_code != 200:
            break
            
        data = response.json()
        items = data.get('data', [])
        if not items:
            break
            
        all_data.extend(items)
        page += 1
        time.sleep(0.5)
        
    if all_data:
        # Write to Bronze
        df = spark.createDataFrame([(json.dumps(d), datetime.now(), run_id) for d in all_data], 
                                   ["_raw", "_ingested_at", "_run_id"])
                                   
        # Clean table name
        safe_endpoint = endpoint.replace('/', '_')
        table_name = f"minted_bronze.shiprocket_{safe_endpoint}"
        
        df.write \
          .format("delta") \
          .mode("append") \
          .option("mergeSchema", "true") \
          .saveAsTable(table_name)
          
        print(f"Successfully wrote {len(all_data)} rows to {table_name}")
        log_audit(spark, run_id, "bronze", f"shiprocket_{safe_endpoint}", len(all_data), "SUCCESS")
