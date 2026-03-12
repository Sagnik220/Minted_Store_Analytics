# Databricks notebook source
# 00_utils.py - Shared HTTP client, paginators, watermarks, and audit functions

import requests
import time
from datetime import datetime
import json
from pyspark.sql.functions import current_timestamp, lit

def fetch_shopify_paginated(endpoint, shopify_config, updated_at_min=None):
    """
    Fetches data from Shopify Admin API handling cursor-based pagination.
    """
    url = f"https://{shopify_config['shop_name']}.myshopify.com/admin/api/{shopify_config['api_version']}/{endpoint}.json"
    headers = {
        "X-Shopify-Access-Token": shopify_config['access_token'],
        "Content-Type": "application/json"
    }
    
    params = {"limit": 250}
    if updated_at_min:
        params["updated_at_min"] = updated_at_min

    results = []
    while url:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        key = endpoint.split('/')[0] # extract entity name
        if key in data:
            results.extend(data[key])
            
        # Handle Link header for Cursor Pagination
        link_header = response.headers.get("Link", None)
        url = None
        params = None # after first call, params are in the cursor URL
        if link_header and 'rel="next"' in link_header:
            links = link_header.split(', ')
            for link in links:
                if 'rel="next"' in link:
                    url = link[link.find('<')+1 : link.find('>')]
                    break
        time.sleep(0.5) # respect rate limit

    return results

def get_shiprocket_token(shiprocket_config):
    """
    Fetches JWT token from Shiprocket API.
    In a real scenario, cache this to DBFS and refresh only if expired.
    """
    url = "https://apiv2.shiprocket.in/v1/external/auth/login"
    payload = {
        "email": shiprocket_config['email'],
        "password": shiprocket_config['password']
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json().get('token')
    return None

def log_audit(spark, run_id, layer, entity, row_count, status):
    """
    Logs pipeline runs to the audit table.
    """
    audit_df = spark.createDataFrame([{
        "run_id": run_id,
        "layer": layer,
        "entity": entity,
        "row_count": row_count,
        "status": status,
        "timestamp": datetime.now()
    }])
    
    audit_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("minted_bronze.ingestion_audit_log")
