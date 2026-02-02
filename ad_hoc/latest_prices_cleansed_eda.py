# Databricks notebook source
from pyspark.sql.functions import *
from time import time
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

# COMMAND ----------


# Read data from 'runescape.02_silver.latest_prices_cleansed'
df = spark.read.table("runescape.02_silver.latest_prices_cleansed")
df2 = df.filter("id IN ('1161')") #Adaamnt Full Helm
df2 = df2.filter("time > 1769029986") # exclude older data
df2 = df2.withColumn("NYCTime", from_unixtime('time'))
df2 = df2.sort("NYCTime", ascending=False)
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Check to make sure there are no duplicate data in runescape.02_silver.latest_prices_cleansed

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %skip
# MAGIC df.dropDuplicates()
# MAGIC df.count()