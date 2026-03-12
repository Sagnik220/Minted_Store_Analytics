# Databricks notebook source
# 04_gold_aggregates.py - Aggregates mapped to core business domains

# MAGIC %run ../00_setup/00_config

from pyspark.sql.functions import sum, count, countDistinct, when, col, date_format

# 1. Sales Daily Aggregate
print("Running Gold: Sales Daily Aggregate")
spark.sql("""
CREATE OR REPLACE TABLE minted_gold.sales_daily AS
SELECT 
    date_format(created_at_ist, 'yyyy-MM-dd') as sales_date,
    SUM(total_price) as daily_revenue,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(total_price) / COUNT(DISTINCT order_id) as average_order_value
FROM minted_silver.shopify_orders
GROUP BY 1
""")

# 2. Customer RFM
print("Running Gold: Customer RFM Scoring")
spark.sql("""
CREATE OR REPLACE TABLE minted_gold.customer_rfm AS
WITH rfm_base AS (
    SELECT 
        customer_email,
        MAX(created_at_ist) as last_purchase_date,
        COUNT(DISTINCT order_id) as frequency,
        SUM(total_price) as monetary
    FROM minted_silver.shopify_orders
    WHERE customer_email IS NOT NULL
    GROUP BY customer_email
)
SELECT 
    customer_email,
    datediff(current_date(), last_purchase_date) as recency_days,
    frequency,
    monetary,
    CASE 
        WHEN monetary >= 10000 THEN 'Whale'
        WHEN monetary >= 5000 THEN 'High Value'
        WHEN monetary >= 1000 THEN 'Mid Value'
        ELSE 'Low Value'
    END as customer_tier
FROM rfm_base
""")

print("Gold Aggregations Complete.")
