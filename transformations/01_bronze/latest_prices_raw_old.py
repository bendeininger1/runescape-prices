# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col, flatten, array, expr, lit
from dateutil.relativedelta import relativedelta
from datetime import date
import json
from time import time

# COMMAND ----------

# Get the ingest_timestamp file path for the lastest json file
# used to get only the most recent data
ingest_timestamp = dbutils.jobs.taskValues.get(
    taskKey = "00_ingest_latest_prices",
    key="ingest_timestamp",
    debugValue="1768416408")
file_path = f"/Volumes/runescape/00_landing/data_sources/latest_prices/latest_prices_{ingest_timestamp}.json"


# COMMAND ----------

print(ingest_timestamp)
print(file_path)

# COMMAND ----------

# Get RAW data from json file
with open(file_path, 'r') as file:
    data = json.load(file)

item_data = data.get('data', {})

# COMMAND ----------

# Convert the data to a Spark DataFrame
df_itemprice = spark.createDataFrame(
    [
        (
            item_id,
            item.get("high", 0),
            item.get("highTime", 0),
            item.get("low", 0),
            item.get("lowTime", 0),
        )
        for item_id, item in item_data.items()
    ],
    schema = ["id", "high", "highTime", "low", "lowTime"],
)

# cast id column as int
df_itemprice = df_itemprice.withColumn("id", df_itemprice["id"].cast("int"))


# COMMAND ----------

# MAGIC %skip
# MAGIC df_itemprice.display()

# COMMAND ----------

# Union high and low price data into one dataframe
# The high and low data are unrelated to one another as they depend on thier respective times
# create df with High price data
df_itemprice_high = df_itemprice.select(
    "id",
    col("high").alias("price"),
    col("highTime").alias("time"))\
    .withColumn("highorlow", lit("high"))

# create df with Low price data
df_itemprice_low = df_itemprice.select(
    "id",
    col("low").alias("price"),
    col("lowTime").alias("time"))\
    .withColumn("highorlow", lit("low"))

# union both dataframes
df_combined_itemprice = df_itemprice_high.union(df_itemprice_low)

# Add ingest_timestamp column for tracking when the RAW data was ingested
df_combined_itemprice = df_combined_itemprice.withColumn("scanTime", lit(ingest_timestamp).cast("int"))

# Filter data to only data from the last ten minutes (or close to whatever interval the data is refreshed at)
# TODO finalize the filter value
df_combined_itemprice = df_combined_itemprice.filter(
    (df_combined_itemprice.scanTime - df_combined_itemprice.time) < 600)



# COMMAND ----------

# MAGIC %skip
# MAGIC df_print = df_combined_itemprice.withColumn("time_diff",df_combined_itemprice.scanTime - df_combined_itemprice.time)
# MAGIC df_print.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Should the data only write new values to the table below?

# COMMAND ----------

# TODO disable this filter
# filter data to make sure this doesnt go crazy...
df_combined_itemprice = df_combined_itemprice.filter("id IN ('1731','1725','1317','1121','1073','1315','1161','1199')")

# COMMAND ----------

# Write the DataFrame to a Unity Catalog managed Delta table in the bronze schema, appending the new data
df_combined_itemprice.write.mode("append").saveAsTable("runescape.01_bronze.latest_prices_raw")

# COMMAND ----------

# MAGIC %skip
# MAGIC # set task value for ETL pipeline
# MAGIC dbutils.jobs.taskValues.set(key="unix_timestamp", value="unix_timestamp")