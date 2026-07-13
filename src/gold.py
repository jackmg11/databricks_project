# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

catalog = "windfarm"
silver_schema = "silver"
gold_schema = "gold"

# COMMAND ----------

silver_df = spark.read.table(f"{catalog}.{silver_schema}.cleaned_windfarm")
output = f"{catalog}.{bronze_schema}.curated_windfarm"

gold_df = silver_df.
    GroupBy("turbine_id", "date")
    .agg(
        F.max("power_output").alias("maximum_power_output"),
        F.min("power_output").alias("minimum_power_output"),
        F.mean("power_output").alias("average_power_output"),
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

