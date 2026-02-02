# Databricks notebook source
from pyspark.sql.functions import *
from time import time
# Set timezone
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

# COMMAND ----------

# Read data from 'runescape.01_bronze.latest_prices_raw'
df = spark.read.table("runescape.01_bronze.latest_prices_raw")

# COMMAND ----------

# Read data from 'runescape.01_bronze.latest_prices_raw'
df2 = df.filter("id IN ('1161')") #Adaamnt Full Helm
df2 = df2.filter("time > 1769029986") # exclude older data
df2 = df2.dropDuplicates()
df2 = df2.withColumn("NYCTime", from_unixtime('time'))
df2 = df2.sort("NYCTime", ascending=False)
df2.display()

# COMMAND ----------

df3 = df.filter("id IN ('1199')") #Adamant Kiteshield
df3 = df3.filter("time > 1769029986") # exclude older data
df3 = df3.dropDuplicates()
df3 = df3.withColumn("NYCTime", from_unixtime('time'))
df3 = df3.sort("NYCTime", ascending=False)
df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Check to see how many prices are above 35000, the most we would ever buy something for

# COMMAND ----------

# MAGIC %skip
# MAGIC # Read data from 'runescape.01_bronze.latest_prices_raw'
# MAGIC df = spark.read.table("runescape.01_bronze.latest_prices_raw")
# MAGIC df = df.filter("time > 1769029986") # exclude older data
# MAGIC df.count()
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC df = df.dropDuplicates()
# MAGIC df.count()
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC df2 = df.filter("price > 35000")
# MAGIC df2.count()/df.count()

# COMMAND ----------

# MAGIC %skip
# MAGIC df = df.filter("price < 35000")
# MAGIC df.count()
# MAGIC
# MAGIC