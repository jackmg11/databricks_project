# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

catalog = "windfarm"
silver_schema = "silver"
gold_schema = "gold"

# COMMAND ----------

silver_df = spark.read.table(f"{catalog}.{silver_schema}.cleaned_windfarm")

gold_df = silver_df.select(
    F.col("date"),
    F.col("turbine_id"),
)

