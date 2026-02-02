# Databricks notebook source
from pyspark.sql.functions import col, when, explode, map_values
from datetime import date
from dateutil.relativedelta import relativedelta
from delta.tables import *
from time import time

# COMMAND ----------

# Get data from 'runescape.01_bronze.latest_prices_raw' remove duplicates
df_latest_prices = spark.read.table("runescape.01_bronze.latest_prices_raw").dropDuplicates()
# TODO determine if every 10 minutes is good
# filter data to only last 15 mintutes and remove prices for items we will never be trading
# job will run this notebook every 10 minutes
unix_timestamp = int(time())
df_latest_prices = df_latest_prices.filter(f"(time > {unix_timestamp} - 600) and (price < 35000)")

# COMMAND ----------

# MAGIC %skip
# MAGIC df_latest_prices.display()

# COMMAND ----------

# Insert df_latest_prices_updates into runescape.02_silver.latest_prices_cleansed

targetDF = DeltaTable.forName(spark, "runescape.02_silver.latest_prices_cleansed")
dfUpdates = df_latest_prices

targetDF.alias("t") .\
  merge(
    source = dfUpdates.alias("s"),
    condition = "t.id = s.id AND t.time = s.time AND \
         t.highorlow = s.highorlow") .\
  whenNotMatchedInsertAll() .\
  execute()


# COMMAND ----------

# MAGIC %skip
# MAGIC df = spark.read.table("runescape.02_silver.latest_prices_cleansed")
# MAGIC df.display()