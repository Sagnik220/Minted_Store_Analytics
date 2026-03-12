# Databricks notebook source
# 00_config.py - Central configuration for Minted Store Analytics

try:
    shopify_shop_domain = dbutils.secrets.get(scope="mintedstore", key="SHOP_DOMAIN")
    shopify_access_token = dbutils.secrets.get(scope="mintedstore", key="SHOPIFY_ADMIN_TOKEN")
    shopify_storefront_token = dbutils.secrets.get(scope="mintedstore", key="STOREFRONT_ACCESS_TOKEN")
    
    shiprocket_email = dbutils.secrets.get(scope="mintedstore", key="SHIPROCKET_EMAIL")
    shiprocket_password = dbutils.secrets.get(scope="mintedstore", key="SHIPROCKET_PASSWORD")
except Exception:
    # Fallback for local testing - DO NOT PUT REAL SECRETS HERE
    shopify_shop_domain = "mintedstore.in"
    shopify_access_token = "LOCAL_TEST_TOKEN"
    shopify_storefront_token = "LOCAL_TEST_TOKEN"
    
    shiprocket_email = "LOCAL_TEST_EMAIL"
    shiprocket_password = "LOCAL_TEST_PASSWORD"

SHOPIFY_CONFIG = {
    "shop_name": shopify_shop_domain.split('.')[0], # Extract 'mintedstore'
    "access_token": shopify_access_token,
    "storefront_token": shopify_storefront_token,
    "api_version": "2024-01"
}

SHIPROCKET_CONFIG = {
    "email": shiprocket_email,
    "password": shiprocket_password,
    "base_url": "https://apiv2.shiprocket.in/v1/external",
    "start_date": "2025-01-01",
    "per_page": 50,
    "database": "mintedstore_landing"
}

# DBFS internal paths for Delta Lake storage
DBFS_PATHS = {
    "bronze": "dbfs:/minted_dw/bronze",
    "silver": "dbfs:/minted_dw/silver",
    "gold":   "dbfs:/minted_dw/gold",
    "raw_documents": "dbfs:/minted_dw/raw_documents",
    "checkpoints": "dbfs:/minted_dw/checkpoints",
    "secrets": "dbfs:/minted_dw/secrets" # for caching JWT tokens
}

# Endpoints mapping
SHOPIFY_ENDPOINTS = [
    "orders", "products", "customers", 
    "inventory_levels", "price_rules", 
    "discount_codes"
]

SHIPROCKET_ENDPOINTS = [
    "orders", "shipments", "couriers", 
    "ndr/all", "returns"
]
