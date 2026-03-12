# Databricks notebook source
# 00_init_databases.py - One-time initialization of databases and tables

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
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

# Document Tracking Tables
spark.sql("""
CREATE TABLE IF NOT EXISTS minted_bronze.raw_documents (
    file_path STRING,
    file_name STRING,
    extracted_text STRING,
    ingested_at TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS minted_silver.expenses (
    document_id STRING,
    expense_date DATE,
    vendor_name STRING,
    category STRING,
    amount DOUBLE,
    raw_text STRING,
    processed_at TIMESTAMP
) USING DELTA
""")

print("Successfully created Databases and Meta tables for Medallion Architecture.")
