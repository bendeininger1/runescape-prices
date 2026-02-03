# Databricks notebook source
# TODO remove any unused 
from pyspark.sql.functions import current_timestamp, col, flatten, array, expr, lit
from datetime import datetime, date, timezone
from time import time
import urllib.request
import os
import shutil
import json
from delta.tables import DeltaTable

# COMMAND ----------

# Get the ingest_timestamp file path for the lastest json file
# used to get only the most recent data
ingest_timestamp = dbutils.jobs.taskValues.get(
    taskKey = "00_ingest_1h_prices",
    key="ingest_timestamp",
    debugValue="1769709951")
file_path = f"/Volumes/runescape/00_landing/data_sources/1h_prices/1h_prices_{ingest_timestamp}.json"

# COMMAND ----------

# open the file so we can transform it and save as parquet file
with open(file_path, 'r') as file:
    data = json.load(file)

item_data = data.get('data', {})
# unix timestamp for when the data was generated.
data_unix_timestamp = data.get('timestamp')

# COMMAND ----------

# Convert the data to a Spark DataFrame
df_1h_prices = spark.createDataFrame(
    [
        (
            int(item_id),
            item.get("avgHighPrice", 0),
            item.get("highPriceVolume", 0),
            item.get("avgLowPrice", 0),
            item.get("lowPriceVolume", 0),
        )
        for item_id, item in item_data.items()
    ],
    schema = "id: int, avg1HourHigh: int, avg1HourHighVolume: int, avg1HourLow: int, avg1HourLowVolume: int",
)
df_1h_prices = df_1h_prices.withColumn("time", lit(data_unix_timestamp).cast("int"))

# COMMAND ----------

df_1h_prices.display()

# COMMAND ----------

# Insert df_1h_prices into 'runescape.01_bronze.1h_prices' table

targetDF = DeltaTable.forName(spark, "runescape.01_bronze.1h_prices")
dfUpdates = df_1h_prices

targetDF.alias("t") .\
  merge(
    source = dfUpdates.alias("s"),
    condition = "t.time = s.time") .\
  whenNotMatchedInsertAll() .\
  execute()

# COMMAND ----------

# TODO insert prices into a latest_1h_prices table that has 1 record for each id (the most recent price data)

# COMMAND ----------

# MAGIC %skip
# MAGIC # Write the DataFrame to a Unity Catalog managed Delta table in the bronze schema, appending the new data
# MAGIC df_1h_prices.write.mode("append").saveAsTable("runescape.01_bronze.1h_prices")
