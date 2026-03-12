# Databricks notebook source
# 00_init_databases.py - One-time initialization of databases and tables

# MAGIC %run ./00_config

spark.sql("CREATE DATABASE IF NOT EXISTS minted_bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS minted_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS minted_gold")

# Create Audit Log Table
spark.sql("""
CREATE TABLE IF NOT EXISTS minted_bronze.ingestion_audit_log (
    run_id STRING,
    layer STRING,
    entity STRING,
    row_count INT,
    status STRING,
    timestamp TIMESTAMP
) USING DELTA
""")

# Create Watermarks Table
spark.sql("""
CREATE TABLE IF NOT EXISTS minted_bronze.ingestion_watermarks (
    source STRING,
    entity STRING,
    last_processed_at TIMESTAMP
) USING DELTA
""")

print("Successfully created Databases and Meta tables for Medallion Architecture.")
