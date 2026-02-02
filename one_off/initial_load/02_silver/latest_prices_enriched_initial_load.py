# Databricks notebook source
from pyspark.sql.functions import col
from time import time

# COMMAND ----------

# TODO filter out data
# Load cleansed latest price data from the Silver Layer 'runescape.02_silver.latest_prices_cleansed'
df_latest_prices = spark.read.table("runescape.02_silver.latest_prices_cleansed")


# TODO determine if every 10 minutes is good
# TODO is performance better if i just pull all data instead of filtering?
# then we can overwrite runescape.02_silver.latest_prices_enriched instead of merging...
# filter data to only last 15 mintutes to reduce performance impact
# job will run this notebook every 10 minutes
unix_timestamp = int(time())
df_latest_prices = df_latest_prices.filter(f"time > {unix_timestamp} - 600")

# Load item mapping data from the Silver Layer
df_item_mapping = spark.read.table("runescape.02_silver.item_mapping")

# COMMAND ----------

# MAGIC %skip
# MAGIC df_latest_prices.display()
# MAGIC df_item_mapping.display()
# MAGIC

# COMMAND ----------

# Join trips with pickup zone details (borough and zone name)
df_join = df_latest_prices.join(
    df_item_mapping,
    df_latest_prices.id == df_item_mapping.id,
    "left"
).select(
    df_latest_prices.id,
    df_latest_prices.price,
    df_latest_prices.time,
    df_latest_prices.highorlow,
    df_item_mapping.name,
    df_item_mapping.highalch,
    "limit", #Not sure why this only works with the string notation
    df_item_mapping.members
)



# COMMAND ----------

df_join.display()

# COMMAND ----------

# Write enriched dataset to a Unity catalog managed Delta table in the silver schema
df_join.write.mode("overwrite").saveAsTable("runescape.02_silver.latest_prices_enriched")