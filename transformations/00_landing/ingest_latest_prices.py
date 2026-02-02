# Databricks notebook source
# TODO remove any unused 
from pyspark.sql.functions import current_timestamp, col, flatten, array, expr, lit
from datetime import datetime, date, timezone
from time import time
import urllib.request
import os
import shutil
import json

# COMMAND ----------

url = 'https://prices.runescape.wiki/api/v1/osrs/latest'

#use headers so the wiki people dont hate me
headers = {
    'User-Agent': 'Bens RS latest prices user agent 1.0 @ghost9420 is my discord',
    'From': 'bendeininger2@gmail.com'  # This is another valid field
}
req = urllib.request.Request(
    url,
    headers=headers
)

# Open a connection and stream the remote file
response = urllib.request.urlopen(req)

# Define and create the local directory for this date's data
dir_path = "/Volumes/runescape/00_landing/data_sources/latest_prices"
os.makedirs(dir_path, exist_ok=True)

# get unix time for file name
unix_timestamp = int(time())

# Define the full path for the downloaded data file
file_path = f"/Volumes/runescape/00_landing/data_sources/latest_prices/latest_prices_{unix_timestamp}.json"

# Save the streamed content to the local file in binary mode
with open(file_path, "wb") as file:
    shutil.copyfileobj(response, file) # Copy data from response to file


# COMMAND ----------

# open the file so we can transform it and save as parquet file
with open(file_path, 'r') as file:
    data = json.load(file)

item_data = data.get('data', {})

# COMMAND ----------

# Convert the data to a Spark DataFrame
df_latest_prices = spark.createDataFrame(
    [
        (
            int(item_id),
            item.get("high", 0),
            item.get("highTime", 0),
            item.get("low", 0),
            item.get("lowTime", 0),
        )
        for item_id, item in item_data.items()
    ],
    schema = "id: int, high: int, highTime: bigint, low: int, lowTime: bigint",
)


# COMMAND ----------

# Union high and low price data into one dataframe
# The high and low data are unrelated to one another as they depend on thier respective times
# create df with High price data
df_latest_prices_high = df_latest_prices.select(
    "id",
    col("high").alias("price"),
    col("highTime").alias("time"))\
    .withColumn("highorlow", lit("high"))

# create df with Low price data
df_latest_prices_low = df_latest_prices.select(
    "id",
    col("low").alias("price"),
    col("lowTime").alias("time"))\
    .withColumn("highorlow", lit("low"))

# union both dataframes
df_combined_latest_prices = df_latest_prices_high.union(df_latest_prices_low)

# Add ingest_timestamp column for tracking when the RAW data was ingested
#df_combined_latest_prices = df_combined_latest_prices.withColumn("scanTime", lit(unix_timestamp).cast("int"))



# COMMAND ----------

# Filter data to only data from the last x seconds (or close to whatever interval the data is refreshed at)
# TODO decide if a filter should be placed here... probably not to prevent slowing down job
# Probably do the filter in the next step
df_combined_latest_prices = df_combined_latest_prices.filter((unix_timestamp - df_combined_latest_prices.time) < 180)

# COMMAND ----------

# Get number of records for debugging
print(df_combined_latest_prices.count())

# COMMAND ----------

# MAGIC %skip
# MAGIC # TODO get rid of this once things actually work
# MAGIC #temporary filter for debuging
# MAGIC #only get data for id 1161 (Adamant full helm)
# MAGIC df_combined_latest_prices = df_combined_latest_prices.\
# MAGIC     filter(df_combined_latest_prices.id== "1161")

# COMMAND ----------

# MAGIC %skip
# MAGIC df_latest_prices.display()

# COMMAND ----------

# Define and create the local directory for this date's data
output_dir_path = "/Volumes/runescape/00_landing/data_sources/latest_prices_transformed"
os.makedirs(output_dir_path, exist_ok=True)

# Define the full path for the downloaded data file
output_dir_path = f"/Volumes/runescape/00_landing/data_sources/latest_prices_transformed/latest_prices_transformed_{unix_timestamp}.parquet"

df_combined_latest_prices.write.parquet(output_dir_path)

# COMMAND ----------

# for debugging
print(output_dir_path)

# COMMAND ----------

# MAGIC %skip
# MAGIC # for debugging
# MAGIC df_combined_latest_prices.printSchema 