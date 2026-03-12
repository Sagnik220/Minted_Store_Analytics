# Databricks notebook source
# MAGIC %pip install PyMuPDF

# COMMAND ----------
# 01_extract_text.py - Reads PDFs from DBFS and extracts raw text to Bronze

# MAGIC %run ../00_setup/00_config

import fitz  # PyMuPDF
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# 1. Ensure landing zone exists
dbutils.fs.mkdirs(DBFS_PATHS["raw_documents"])
print(f"Checking for documents in {DBFS_PATHS['raw_documents']}")

files = dbutils.fs.ls(DBFS_PATHS["raw_documents"])
extracted_data = []

# temporary local path to download DBFS file to read with fitz
local_temp_dir = "/tmp/raw_docs"
os.makedirs(local_temp_dir, exist_ok=True)

for f in files:
    # Filter for standard document types
    if not f.name.lower().endswith(('.pdf')): 
        continue
        
    print(f"Processing {f.name}...")
    local_path = os.path.join(local_temp_dir, f.name)
    
    # Copy from DBFS to local compute storage for PyMuPDF
    dbutils.fs.cp(f.path, f"file:{local_path}")
    
    extracted_text = ""
    try:
        # Open and extract text natively
        doc = fitz.open(local_path)
        for page in doc:
            extracted_text += page.get_text() + "\n"
        doc.close()
        
        extracted_data.append((
            f.path,
            f.name,
            extracted_text,
            datetime.now()
        ))
    except Exception as e:
        print(f"Error extracting {f.name}: {str(e)}")
        
    # Clean up local file
    os.remove(local_path)

if extracted_data:
    schema = StructType([
        StructField("file_path", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("extracted_text", StringType(), True),
        StructField("ingested_at", TimestampType(), True)
    ])
    
    df = spark.createDataFrame(extracted_data, schema=schema)
    
    # Write to Bronze
    table_name = "minted_bronze.raw_documents"
    df.write \
      .format("delta") \
      .mode("append") \
      .saveAsTable(table_name)
      
    print(f"Successfully extracted and wrote {len(extracted_data)} documents to {table_name}")
    
    # Move processed files to a processed folder to avoid re-ingestion
    processed_dir = f"{DBFS_PATHS['raw_documents']}_processed"
    dbutils.fs.mkdirs(processed_dir)
    for data in extracted_data:
        dbutils.fs.mv(data[0], f"{processed_dir}/{data[1]}")
        
else:
    print("No new documents found for extraction.")
