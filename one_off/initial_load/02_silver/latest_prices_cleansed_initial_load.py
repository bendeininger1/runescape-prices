# Databricks notebook source
from pyspark.sql.functions import col, when, explode, map_values
from datetime import date
from dateutil.relativedelta import relativedelta
from delta.tables import *
from time import time

# COMMAND ----------

# Get data from 'runescape.01_bronze.latest_prices_raw' remove duplicates
df_latest_prices = spark.read.table("runescape.01_bronze.latest_prices_raw").dropDuplicates()
# filter data to only last 15 mintutes and remove prices for items we will never be trading
# job will run this notebook every 10 minutes
unix_timestamp = int(time())
df_latest_prices = df_latest_prices.filter(f"(time > {unix_timestamp} - 600) and (price < 35000)")

# COMMAND ----------

# MAGIC %skip
# MAGIC df_latest_prices.display()

# COMMAND ----------

# Write cleansed data to a Unity Catalog managed Delta table in the silver schema
df_latest_prices.write.mode("overwrite").saveAsTable("runescape.02_silver.latest_prices_cleansed")

# COMMAND ----------

# MAGIC %skip
# MAGIC # Insert df_latest_prices_updates into runescape.02_silver.latest_prices_cleansed
# MAGIC
# MAGIC targetDF = DeltaTable.forName(spark, "runescape.02_silver.latest_prices_cleansed")
# MAGIC dfUpdates = df_latest_prices
# MAGIC
# MAGIC targetDF.alias("t") .\
# MAGIC   merge(
# MAGIC     source = dfUpdates.alias("s"),
# MAGIC     condition = "t.id = s.id AND t.time = s.time AND \
# MAGIC          t.highorlow = s.highorlow") .\
# MAGIC   whenNotMatchedInsertAll() .\
# MAGIC   execute()
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC df = spark.read.table("runescape.02_silver.latest_prices_cleansed")
# MAGIC df.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC # Write cleansed data to a Unity Catalog managed Delta table in the silver schema
# MAGIC df_itemprice.write.mode("append").saveAsTable("runescape.02_silver.latest_prices_cleansed")