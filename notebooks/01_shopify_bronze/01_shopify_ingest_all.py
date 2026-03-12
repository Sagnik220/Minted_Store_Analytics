# Databricks notebook source
# 01_shopify_ingest_all.py - Ingests Shopify API endpoints to Bronze

# MAGIC %run ../00_setup/00_config
# MAGIC %run ../00_setup/00_utils

import json
from datetime import datetime

run_id = f"run_{int(time.time())}"

for endpoint in SHOPIFY_ENDPOINTS:
    print(f"Ingesting Shopify -> {endpoint}")
    
    # 1. Fetch data (simplified incremental logic)
    # In production, fetch `last_processed_at` from `minted_bronze.ingestion_watermarks`
    data = fetch_shopify_paginated(endpoint, SHOPIFY_CONFIG)
    
    if data:
        # 2. Write to Bronze Delta Table (Raw JSON string)
        df = spark.createDataFrame([(json.dumps(d), datetime.now(), run_id) for d in data], 
                                   ["_raw", "_ingested_at", "_run_id"])
        
        table_name = f"minted_bronze.shopify_{endpoint}"
        
        df.write \
          .format("delta") \
          .mode("append") \
          .option("mergeSchema", "true") \
          .saveAsTable(table_name)
          
        print(f"Successfully wrote {len(data)} rows to {table_name}")
        log_audit(spark, run_id, "bronze", endpoint, len(data), "SUCCESS")
    else:
        print(f"No new data for {endpoint}")
