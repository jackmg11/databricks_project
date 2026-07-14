# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from src.common_functions import dup_checker
import random
from datetime import datetime, timedelta

# COMMAND ----------

# creating dummy data to be used in dup checker function
data = [
    ("T001", "2025-01-01 00:00:00", 120.5),
    ("T003", "2025-01-01 00:00:00", 95.4),
]

columns = ["turbine_id", "timestamp", "power_output"]

dummy_df = spark.createDataFrame(data, columns)


data = [
    ("T002", "2025-01-01 00:00:00", 120.5),
]

columns = ["turbine_id", "timestamp", "power_output"]

dummy_table = spark.createDataFrame(data, columns)

# COMMAND ----------

# calls dup_checker function (unions new data and current state and checks if there are any duplicates)
dup_checker_test = dup_checker(dummy_df, dummy_table, ("turbine_id", "timestamp"))

# raises assertion error if there are any dups
assert dup_checker_test.count() == 0, "Duplicate records found in test table"

# COMMAND ----------

# creating dummy data to be used in dup checker function
data = [
    ("2022-03-01 00:00:00", 1, 11.8, 169, 2.7),
    ("2022-03-01 00:00:00", 1, 11.8, 169, 2.7),
    ("2022-03-01 00:00:00", 2, 11.6, 24, 2.2),
    ("2022-03-01 00:00:00", 3, 13.8, 335, 2.3),
    ("2022-03-01 00:00:00", 4, 12.8, 238, 1.9)
]

columns = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]

dummy_df = spark.createDataFrame(data, columns)


data = [
    ("2022-03-01 00:00:00", 1, 11.8, 169, 2.7),
    ("2022-03-01 00:00:00", 1, 11.8, 169, 2.7),
    ("2022-03-01 00:00:00", 2, 11.6, 24, 2.2),
    ("2022-03-01 00:00:00", 3, 13.8, 335, 2.3),
    ("2022-03-01 00:00:00", 4, 12.8, 238, 1.9),
    ("2026-03-01 00:00:00", 75, 12.8, 238, 1.9),
]

columns = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]

dummy_table = spark.createDataFrame(data, columns)

# COMMAND ----------

# calls dup_checker function (unions new data and current state and checks if there are any duplicates)
dup_checker_test = dup_checker(dummy_df, dummy_table, ("turbine_id", "timestamp"))

# raises assertion error if there are not any dups
assert dup_checker_test.count() > 0, "no Duplicate records found in test table"

# COMMAND ----------

# creating a dummy dataframe random dummy data to be inserted into the volume at the start of each run this will test our delta check is working
data = [
    ((datetime.now() + timedelta(days=random.randint(-1000, 1000))).strftime("%Y-%m-%d %H:%M:%S"),
        1,
        11.8,
        169,
        2.7
    )
    for i in range(10)
]

display(data)

# COMMAND ----------

# creating final dataframe with dummy schema 
df = spark.createDataFrame(
    data,
    ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
)

# COMMAND ----------

# writing dummy df to volume
df.write \
    .mode("append") \
    .option("header", "true") \
    .csv("/Volumes/workspace/files/raw_files/")
