from pyspark.sql import functions as F

def dup_checker(new_df, current_table, keys):
    """Returns a dataframe of duplicate records in the new dataframe"""
    return (
    new_df.unionByName(current_table).groupBy(*keys)
      .count()
      .filter(F.col("count") > 1))