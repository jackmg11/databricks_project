# Databricks notebook source
# all imports required for this notebook
from pyspark.sql import functions as F
from common_functions import dup_checker

# COMMAND ----------

# output catalog and schema 
catalog = "windfarm"
silver_schema = "silver"
gold_schema = "gold"

# COMMAND ----------

# read silver table, current gold table state along with gold path
silver_df = spark.read.table(f"{catalog}.{silver_schema}.cleaned_windfarm")
output = f"{catalog}.{gold_schema}.curated_windfarm"
current_state_gold = spark.table(output)

# create final gold df with aggregates
gold_df = silver_df.groupBy(
    "turbine_id",
    "date"
).agg(
    F.max("power_output").alias("maximum_power_output"),
    F.min("power_output").alias("minimum_power_output"),
    F.mean("power_output").alias("average_power_output")
)

# create empty table if it doesn't exist
if not spark.catalog.tableExists(output):
    empty_df = gold_df.limit(0)
    empty_df.write.mode("overwrite").saveAsTable(output)

# first load check (if table is empty load all data otherwise take delta)
if current_state_gold.isEmpty():        
    gold_df = gold_df
else:
    # get current state of gold table and join with bronze DF to only get new records (based on turbine_id and timestamp) 
    gold_df = gold_df.join(current_state_gold, on=("turbine_id" , "timestamp"), how="left_anti")   

# calls dup_checker function (unions new data and current state and checks if there are any duplicates)
dup_checker_silver = dup_checker(gold_df, current_state_gold, ("turbine_id", "timestamp"))

# raises assertion error if there are any dups
assert dup_checker_silver.count() == 0, "Duplicate records found in silver table"

print(f"writing to {output}...")
# append data to table along with merge schema to allow any new columns if they arrive in CSVs
gold_df.write.mode("append").option("mergeSchema", "true").saveAsTable(output)
print(f"write to {output} completed")

