# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from src.common_functions import dup_checker
import random
from datetime import datetime, timedelta

# COMMAND ----------

windfarm_catalog = "windfarm"
test_dummy_schema = "default"
testing_output = f"{windfarm_catalog}.{test_dummy_schema}.test_data"
testing_current_state = spark.table(testing_output)
testing_input = "/Volumes/workspace/files/raw_files"

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

# creating final dataframe with dummy schema and appending random data 
df = spark.createDataFrame(
    data,
    ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
).unionByName(dummy_df)

# COMMAND ----------

# writing dummy df to volume
df.write \
    .mode("append") \
    .option("header", "true") \
    .csv("/Volumes/workspace/files/test/")

# COMMAND ----------

# creating testing table if it doesn't exist with the schema as is in the testing df 
if not spark.catalog.tableExists(testing_output):
    empty_df = df.limit(0)
    empty_df.write.mode("overwrite").saveAsTable(testing_output)

# if testing_current_state is empty then load all data (this should always be 15 records on the first load)
if testing_current_state.isEmpty():        
    df = df
else:
    # if testing_current_state is not empty then load only new data based on turbine_id and timestamp
    df = df.join(testing_current_state,
        on=["turbine_id", "timestamp"],
        how="left_anti"
    )   

# this count should only include new data 
df_count = df.count()

print(f"writing to {df_count} records to {testing_output}...")
# append data to table along with merge schema to allow any new columns if they arrive in CSVs
df.write.mode("append").option("mergeSchema", "true").saveAsTable(testing_output)
print(f"write to {testing_output} completed")
