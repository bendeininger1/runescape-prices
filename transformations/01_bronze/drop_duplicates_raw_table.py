# Databricks notebook source
# MAGIC %md
# MAGIC Remove duplicates created when streaming in raw data

# COMMAND ----------

# Read data from 'runescape.01_bronze.latest_prices_raw'
df = spark.read.table("runescape.01_bronze.latest_prices_raw")
df.count()

# COMMAND ----------

df = df.dropDuplicates()
df.count()

# COMMAND ----------

# Write the DataFrame to a Unity Catalog managed Delta table in the bronze schema, overwritting the data
df.write.mode("overwrite").saveAsTable("runescape.01_bronze.latest_prices_raw")