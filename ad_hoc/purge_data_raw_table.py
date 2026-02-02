# Databricks notebook source
# MAGIC %md
# MAGIC Purge any data in 'runescape.01_bronze.latest_prices_raw' 

# COMMAND ----------

from pyspark.sql.functions import *
from time import time

# COMMAND ----------

# Read data from 'runescape.01_bronze.latest_prices_raw'
df = spark.read.table("runescape.01_bronze.latest_prices_raw")
#df = df.filter("time > 1769029986") # exclude older data
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Overwrite data