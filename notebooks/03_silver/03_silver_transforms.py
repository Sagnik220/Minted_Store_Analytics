# Databricks notebook source
# 03_silver_transforms.py - Transforms Bronze to typed, cleaned Silver tables

from pyspark.sql.functions import col, from_json, explode, to_timestamp, current_timestamp
from pyspark.sql.types import *

# 1. Shopify Orders
print("Transforming Shopify Orders to Silver")
raw_orders_schema = StructType([
    StructField("id", LongType(), True),
    StructField("email", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("total_price", StringType(), True),
    StructField("line_items", ArrayType(StructType([
        StructField("id", LongType(), True),
        StructField("product_id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", StringType(), True)
    ])), True)
])

# Read Bronze
bronze_orders = spark.read.table("minted_bronze.shopify_orders")

# Parse JSON and create typed columns
parsed_orders = bronze_orders.withColumn("data", from_json(col("_raw"), raw_orders_schema)) \
    .select(
        col("data.id").alias("order_id"),
        col("data.email").alias("customer_email"),
        to_timestamp(col("data.created_at")).alias("created_at_ist"),
        col("data.total_price").cast("double").alias("total_price"),
        col("data.line_items")
    ) \
    .dropDuplicates(["order_id"])

# Upsert (MERGE INTO) or Save to Silver
parsed_orders.write.format("delta").mode("overwrite").saveAsTable("minted_silver.shopify_orders")

# Line Items Fact Table (Exploded)
print("Transforming Shopify Line Items to Silver")
line_items = parsed_orders.select("order_id", explode("line_items").alias("item")) \
    .select(
        "order_id",
        col("item.id").alias("line_item_id"),
        col("item.product_id"),
        col("item.title"),
        col("item.quantity"),
        col("item.price").cast("double")
    )

line_items.write.format("delta").mode("overwrite").saveAsTable("minted_silver.shopify_order_line_items")

print("Silver Transformation Complete")
