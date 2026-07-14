# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from src.common_functions import dup_checker

# COMMAND ----------

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


data = [
    ("T001", "2025-01-01 00:00:00", 120.5),
    ("T001", "2025-01-01 01:00:00", 118.2),
    ("T002", "2025-01-01 00:00:00", 135.8),
    ("T002", "2025-01-01 01:00:00", 140.1),
    ("T003", "2025-01-01 00:00:00", 95.4),
]

columns = ["turbine_id", "timestamp", "power_output"]

dummy_df = spark.createDataFrame(data, columns)


data = [
    ("T001", "2025-01-01 00:00:00", 120.5),
    ("T001", "2025-01-01 01:00:00", 118.2),
    ("T002", "2025-01-01 00:00:00", 135.8),
    ("T002", "2025-01-01 01:00:00", 140.1),
    ("T003", "2025-01-01 00:00:00", 95.4),
]

columns = ["turbine_id", "timestamp", "power_output"]

dummy_table = spark.createDataFrame(data, columns)

# COMMAND ----------

# calls dup_checker function (unions new data and current state and checks if there are any duplicates)
dup_checker_test = dup_checker(dummy_df, dummy_table, ("turbine_id", "timestamp"))

# raises assertion error if there are not any dups
assert dup_checker_test.count() > 0, "no Duplicate records found in test table"
