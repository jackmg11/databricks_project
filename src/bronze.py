# Databricks notebook source
# imports
import os
from pyspark.sql import functions as F

# COMMAND ----------

# output catalog and schema 
catalog = "windfarm"
bronze_schema = "bronze"

# COMMAND ----------

# input volume path and output table path
input = "/Volumes/workspace/files/raw_files"
output = f"{catalog}.{bronze_schema}.windfarm_raw"

# read all csv's into a dataframe 
df = spark.read.option("header", "true").csv(input)

# add meta data columns to dataframe
df = df.select(
            "*",
            F.current_timestamp().alias("ingest_timestamp"),
            F.col("_metadata.file_name").alias("source_file"),
            F.col("_metadata.file_path").alias("source_path")
)

# create empty table if it doesn't exist
if not spark.catalog.tableExists(output):
    empty_df = df.limit(0)
    empty_df.write.mode("overwrite").saveAsTable(output)

# first load check (if table is empty load all data otherwise take delta)
if spark.table(output).isEmpty():        
    bronze_df = df
else:
    # get current state of table and join with bronze DF to only get new records (based on turbine_id and timestamp) 
    current_state = spark.table(output)
    bronze_df = df.join(current_state, on=("turbine_id" , "timestamp"), how="left_anti")   

print(f"writing to {output}...")
# append data to table along with merge schema to allow any new columns if they arrive in CSVs
bronze_df.write.mode("append").option("mergeSchema", "true").saveAsTable(output)
print(f"write to {output} completed")
