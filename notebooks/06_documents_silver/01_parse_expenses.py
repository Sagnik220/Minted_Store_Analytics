# Databricks notebook source
# 01_parse_expenses.py - Parses raw text from Bronze into structured Silver entities

# MAGIC %run ../00_setup/00_config

import re
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# Regex patterns for common invoice structures
date_pattern = re.compile(r'(?i)(?:date|dated)[\s:]*([\d]{1,2}[/-][\d]{1,2}[/-][\d]{2,4})')
amount_pattern = re.compile(r'(?i)(?:total|amount due|grand total)[\s:]*(?:rs\.?|inr|₹)?\s*([\d,]+\.?\d*)')

# Heuristics for categorization
PACKAGING_VENDORS = ['pack', 'box', 'tape', 'paper', 'corrugated']
IMPORT_DUTY_VENDORS = ['customs', 'duty', 'fedex', 'dhl', 'clearance', 'cbic']

def parse_expense_text(text, filename):
    """
    Takes raw document text and attempts to parse out the structured details.
    """
    text_lower = str(text).lower()
    
    # 1. Standardize Categories
    category = "Other Expense"
    if any(v in text_lower for v in PACKAGING_VENDORS):
        category = "Packaging & Supplies"
    elif any(v in text_lower for v in IMPORT_DUTY_VENDORS):
        category = "Import Duty & Taxes"
        
    # 2. Extract Date
    expense_date = None
    date_match = date_pattern.search(text)
    if date_match:
        date_str = date_match.group(1).replace('-', '/')
        try:
            # try parsing basic DD/MM/YYYY
            expense_date = datetime.strptime(date_str, '%d/%m/%Y').date()
        except ValueError:
            pass # fallback to None if unmatched format

    # 3. Extract Amount
    amount = 0.0
    amount_match = amount_pattern.search(text)
    if amount_match:
        try:
            amt_str = amount_match.group(1).replace(',', '')
            amount = float(amt_str)
        except ValueError:
            pass
            
    # 4. Dummy Vendor Extraction (In real prod, use Named Entity Recognition / LLM)
    # Using filename or first line as fallback
    vendor_name = filename.split('.')[0]
    first_line = str(text).strip().split('\n')[0]
    if len(first_line) > 3 and len(first_line) < 50:
        vendor_name = first_line

    return (vendor_name.upper(), category, amount, expense_date)

# Create Schema for UDF
parse_schema = StructType([
    StructField("vendor_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("expense_date", DateType(), True)
])

# Register UDF
@udf(returnType=parse_schema)
def extract_entities_udf(text, filename):
    return parse_expense_text(text, filename)

print("Reading raw documents from Bronze...")
bronze_docs = spark.read.table("minted_bronze.raw_documents")

if bronze_docs.count() > 0:
    # Process text
    processed_df = bronze_docs.withColumn("parsed", extract_entities_udf(col("extracted_text"), col("file_name"))) \
        .select(
            col("file_path").alias("document_id"),
            col("parsed.expense_date").alias("expense_date"),
            col("parsed.vendor_name").alias("vendor_name"),
            col("parsed.category").alias("category"),
            col("parsed.amount").alias("amount"),
            col("extracted_text").alias("raw_text"),
            col("ingested_at").alias("processed_at")
        )

    # Write to Silver
    table_name = "minted_silver.expenses"
    processed_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(table_name)
        
    print(f"Successfully parsed and wrote expenses to {table_name}")
    
    # Optional: Clear the bronze table so we don't double process next run? 
    # Better to use delta lake change data feed or watermarks in production.
else:
    print("No documents to process in Bronze.")
