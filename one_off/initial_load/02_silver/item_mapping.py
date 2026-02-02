# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

df = spark.read.format("json").option("header", True).load("/Volumes/runescape/00_landing/data_sources/mapping/item_mapping.json")


# COMMAND ----------

# todo filter to items that I care about
# limit > at least 40, maybe 70 or 125

df = df.select(
    col("id"),
    col("name"),
    col("highalch"),
    col("limit"),
    col("members")
    
)

# COMMAND ----------

# MAGIC %skip
# MAGIC df.display()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("runescape.02_silver.item_mapping")