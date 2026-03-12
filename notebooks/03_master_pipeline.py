# Databricks notebook source
# 03_master_pipeline.py - Orchestrates all layers end-to-end

print("=== Starting Minted Store Analytics Pipeline ===")

# Run Setup & Bronze
print("--- [1/3] BRONZE INGESTION ---")
dbutils.notebook.run("./01_shopify_bronze/01_shopify_ingest_all", 3600)
dbutils.notebook.run("./02_shiprocket_bronze/02_shiprocket_ingest_all", 3600)

# Run Silver
print("--- [2/3] SILVER TRANSFORMS ---")
dbutils.notebook.run("./03_silver/03_silver_transforms", 3600)

# Run Gold
print("--- [3/3] GOLD AGGREGATES ---")
dbutils.notebook.run("./04_gold/04_gold_aggregates", 3600)

print("=== Pipeline Execution Complete ===")
