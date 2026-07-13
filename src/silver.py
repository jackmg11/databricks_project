# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

catalog = "windfarm"
bronze_schema = "bronze"
silver_schema = "silver"
quarantine_schema = "quarantine"

# COMMAND ----------

bronze_df = spark.read.table(f"{catalog}.{bronze_schema}.windfarm_raw")
output_silver = f"{catalog}.{silver_schema}.cleaned_windfarm"
output_quarantine = f"{catalog}.{bronze_schema}.anomaly_records"

bronze_df = bronze_df.drop("ingest_timestamp")

silver_df = bronze_df.select(
                "*",
                F.current_timestamp().alias("ingest_timestamp"),
                F.to_date("timestamp").alias("date")
            )

quarantine = silver_df.filter(
    (F.col("wind_direction") > 360) |
    (F.col("wind_direction") < 0)
)

window = Window.partitionBy("turbine_id", "date")

silver_df = (
    silver_df.withColumn("mean_power", F.mean("power_output").over(window))
      .withColumn("std_power", F.stddev("power_output").over(window))
      .withColumn(
          "anomaly_flag",
          F.when(
              (F.col("power_output") < F.col("mean_power") - 2 * F.col("std_power")) |
              (F.col("power_output") > F.col("mean_power") + 2 * F.col("std_power")),
              True
          ).otherwise(False)
      )
)

quarantine_df = silver_df.filter(
    F.col("anomaly_flag") == True
)

silver_df = silver_df.filter(
    F.col("anomaly_flag") == False
)

if not spark.catalog.tableExists(output_silver):
    empty_df = silver_df.limit(0)
    empty_df.write.mode("overwrite").saveAsTable(output_silver)

if not spark.catalog.tableExists(output_quarantine):
    empty_df = silver_df.limit(0)
    empty_df.write.mode("overwrite").saveAsTable(output_quarantine)

if spark.table(output_quarantine).isEmpty():        
    quarantine_df = quarantine_df
else:        
    current_state = spark.table(output_quarantine)
    quarantine_df = quarantine_df.join(current_state,
                on=("turbine_id", "timestamp"),
                how="left_anti"
    )   

if spark.table(output_silver).isEmpty():        
    silver_df = silver_df
else:        
    current_state = spark.table(output_silver)
    silver_df = silver_df.join(current_state,
                on=("turbine_id", "timestamp"),
                how="left_anti"
    )   

dup_checker = (
    silver_df.groupBy("turbine_id", "timestamp")
      .count()
      .filter(F.col("count") > 1)
)

assert dup_checker.count() == 0, "Duplicate records found in silver table"

print(f"writing to {quarantine_df}...")
silver_df.write.mode("append").option("mergeSchema", "true").saveAsTable(quarantine_df)
print(f"writing to {quarantine_df} completed")

print(f"writing to {output_silver}...")
silver_df.write.mode("append").option("mergeSchema", "true").saveAsTable(output_silver)
print(f"writing to {output_silver} completed")
