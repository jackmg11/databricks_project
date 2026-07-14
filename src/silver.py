# Databricks notebook source
# all imports required for this notebook
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common_functions import dup_checker

# COMMAND ----------

# catalog and schema variables
catalog = "windfarm"
bronze_schema = "bronze"
silver_schema = "silver"
quarantine_schema = "quarantine"

# COMMAND ----------

# read bronze table and give output path of silver and quarantine
bronze_df = spark.read.table(f"{catalog}.{bronze_schema}.windfarm_raw")
output_silver = f"{catalog}.{silver_schema}.cleaned_windfarm"
output_quarantine = f"{catalog}.{quarantine_schema}.anomaly_records"

# getting the current state of the output tables before any writes
current_state_silver = spark.table(output_silver)
current_state_quarantine = spark.table(output_quarantine)

# removing bronze timestamp column added by our process
bronze_df = bronze_df.drop("ingest_timestamp")

# adding silver timestamp along with date column for derivation below
silver_df = bronze_df.select(
                "*",
                F.current_timestamp().alias("ingest_timestamp"),
                F.to_date("timestamp").alias("date")
            )

# defining the window
window = Window.partitionBy("turbine_id", "date")

# deriving anomaly_flag along with wind_direction_out_of_range flag
silver_df = (
    silver_df
    .withColumn(
        "mean_power",
        F.mean("power_output").over(window)
    )
    .withColumn(
        "std_power",
        F.stddev("power_output").over(window)
    )
    .withColumn(
        "anomaly_flag",
        F.when(
            (F.col("power_output") < F.col("mean_power") - 2 * F.col("std_power")) |
            (F.col("power_output") > F.col("mean_power") + 2 * F.col("std_power")),
            True
        ).otherwise(False)
    )
    .withColumn(
        "date",
        F.to_date("timestamp")
    )
    .withColumn(
        "wind_direction_out_of_range",
        F.when(
            (F.col("wind_direction") > 360) | (F.col("wind_direction") < 0),
            True
        ).otherwise(False)
    )
    .drop("mean_power", "std_power")
)

# defining quarantine data based on anomaly_flag
quarantine_df = silver_df.filter(
    ((F.col("anomaly_flag") == True) |
     (F.col("wind_direction_out_of_range") == True))
)

# defining silver data based on anomaly_flag 
silver_df = silver_df.filter(
    (F.col("anomaly_flag") == False) &
    (F.col("wind_direction_out_of_range") == False)
)

# creating silver table if it doesn't exist with the schema as is in the silver df 
if not spark.catalog.tableExists(output_silver):
    empty_df = silver_df.limit(0)
    empty_df.write.mode("overwrite").saveAsTable(output_silver)

# creating quarantine table if it doesn't exist with the schema as is in the silver df 
if not spark.catalog.tableExists(output_quarantine):
    empty_df = silver_df.limit(0)
    empty_df.write.mode("overwrite").saveAsTable(output_quarantine)

# if current_state_quarantine is empty then load all data
if current_state_quarantine.isEmpty():        
    quarantine_df = quarantine_df
else:
    # if current_state_quarantine is not empty then load only new data based on turbine_id and timestamp
    quarantine_df = quarantine_df.join(current_state_quarantine,
        on=("turbine_id", "timestamp"),
        how="left_anti"
    )   

# calls dup_checker function (unions new data and current state and checks if there are any duplicates)
dup_checker_quarantine = dup_checker(quarantine_df, current_state_quarantine, ("turbine_id", "timestamp"))

# raises assertion error if there are any dups
assert dup_checker_quarantine.count() == 0, "Duplicate records found in quarantine table"

# if current_state_silver is empty then load all data
if current_state_silver.isEmpty():        
    silver_df = silver_df
else:        
    # if current_state_silver is not empty then load only new data based on turbine_id and timestamp
    silver_df = silver_df.join(current_state_silver,
        on=("turbine_id", "timestamp"),
        how="left_anti"
    )   

# calls dup_checker function (unions new data and current state and checks if there are any duplicates)
dup_checker_silver = dup_checker(silver_df, current_state_silver, ("turbine_id", "timestamp"))

# raises assertion error if there are any dups
assert dup_checker_silver.count() == 0, "Duplicate records found in silver table"

print(f"writing to {quarantine_df}...")
quarantine_df.write.mode("append").option("mergeSchema", "true").saveAsTable(output_quarantine)
print(f"writing to {quarantine_df} completed")

print(f"writing to {output_silver}...")
silver_df.write.mode("append").option("mergeSchema", "true").saveAsTable(output_silver)
print(f"writing to {output_silver} completed")
