# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col, flatten, array, expr, lit
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
from datetime import date
import json
from time import time

# COMMAND ----------

# https://learn.microsoft.com/en-us/azure/databricks/jobs/file-arrival-triggers
# Configuration
# TODO need to get the most recent file names?
file_location = "/Volumes/runescape/00_landing/data_sources/latest_prices_transformed/" # The same URL configured for the file arrival trigger.
checkpoint_location = "/Volumes/runescape/00_landing/data_sources/latest_prices_transformed_checkpoint/" # a separate URL (outside `file_location`) used to store the Auto Loader checkpoint, which enables exactly-once processing.

#Schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("time", LongType(), True),
    StructField("highorlow", StringType(), True)
])

# Read all the csv files written atomically in a directory
df = spark \
    .readStream \
    .schema(schema) \
    .format("parquet") \
    .load(file_location)

#df.write.mode("append").saveAsTable("runescape.01_bronze.latest_prices_raw")

df.writeStream \
    .format("console") \
    .trigger(availableNow=True) \
    .start()

# COMMAND ----------

# MAGIC %skip
# MAGIC # https://learn.microsoft.com/en-us/azure/databricks/jobs/file-arrival-triggers
# MAGIC # Configuration
# MAGIC # TODO need to get the most recent file names?
# MAGIC file_location = "/Volumes/runescape/00_landing/data_sources/latest_prices_transformed/" # The same URL configured for the file arrival trigger.
# MAGIC checkpoint_location = "/Volumes/runescape/00_landing/data_sources/latest_prices_transformed_checkpoint/" # a separate URL (outside `file_location`) used to store the Auto Loader checkpoint, which enables exactly-once processing.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC #Schema
# MAGIC schema = StructType([
# MAGIC     StructField("id", IntegerType(), True),
# MAGIC     StructField("price", IntegerType(), True),
# MAGIC     StructField("time", LongType(), True),
# MAGIC     StructField("highorlow", StringType(), True)
# MAGIC ])
# MAGIC
# MAGIC # Use Auto Loader to discover new files.
# MAGIC streamingQuery = (spark \
# MAGIC     .readStream \
# MAGIC     .schema(schema) \
# MAGIC     .format("parquet") \
# MAGIC     .load(file_location) \
# MAGIC     .writeStream \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", checkpoint_location) \
# MAGIC     .trigger(availableNow=True) \
# MAGIC     .toTable("runescape.01_bronze.latest_prices_raw") # TODO the data does not being written to the table
# MAGIC )

# COMMAND ----------

# MAGIC %skip
# MAGIC # https://learn.microsoft.com/en-us/azure/databricks/jobs/file-arrival-triggers
# MAGIC # Configuration
# MAGIC file_location = "/Volumes/runescape/00_landing/data_sources/latest_prices_transformed/" # The same URL configured for the file arrival trigger.
# MAGIC checkpoint_location = "/Volumes/runescape/00_landing/data_sources/latest_prices_transformed_checkpoint/" # a separate URL (outside `file_location`) used to store the Auto Loader checkpoint, which enables exactly-once processing.
# MAGIC sink_table = "runescape.01_bronze.latest_prices_raw" # Delta table to write to
# MAGIC
# MAGIC #Schema
# MAGIC schema = StructType([
# MAGIC     StructField("id", IntegerType(), True),
# MAGIC     StructField("price", IntegerType(), True),
# MAGIC     StructField("time", LongType(), True),
# MAGIC     StructField("highorlow", StringType(), True),
# MAGIC     StructField("scanTime", LongType(), True)
# MAGIC ])
# MAGIC
# MAGIC # Use Auto Loader to discover new files.
# MAGIC streamingQuery = (spark \
# MAGIC     .readStream
# MAGIC     .schema(schema)
# MAGIC     .format("console")
# MAGIC     .load(file_location)
# MAGIC     .writeStream
# MAGIC     .option("checkpointLocation", checkpoint_location) \
# MAGIC     
# MAGIC     .load()
# MAGIC )

# COMMAND ----------

# MAGIC %skip
# MAGIC query = wordCounts \
# MAGIC     .writeStream \
# MAGIC     .outputMode("complete") \
# MAGIC     .format("console") \
# MAGIC     .start()
# MAGIC
# MAGIC query.awaitTermination()

# COMMAND ----------

# MAGIC %skip
# MAGIC df_itemprice.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Should the data only write new values to the table below?