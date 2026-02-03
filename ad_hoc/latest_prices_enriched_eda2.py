# Databricks notebook source
import pyspark.sql.functions as sf
from time import time
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

# COMMAND ----------

# Read data from 'runescape.02_silver.item_mapping'
df_map = spark.read.table("runescape.02_silver.item_mapping")

# COMMAND ----------

# Read data from 'runescape.02_silver.latest_prices_cleansed'
df = spark.read.table("runescape.02_silver.latest_prices_enriched")
df = df.withColumn("NYCTime", sf.from_unixtime('time'))
df.count()

# COMMAND ----------

# DBTITLE 1,Cell 14
# Read data from 'runescape.01_bronze.1h_prices' to get Volumes
# TODO replace this read with the new latest_1_prices table that will have the latest 1 hour price for each item
df_avg = spark.read.table("runescape.01_bronze.1h_prices")
# Filter to only use the most recent Volume data
max_time = df_avg.select(sf.max(df_avg.time))

#df_avg = df_avg.filter("time == max_time.first('time'")
df_avg = df_avg.join(max_time, df_avg.time == sf.col("max(time)"))
df_avg = df_avg.drop("max(time)").drop("time")
#df_avg.display()




# COMMAND ----------

# filter out low limit items
df = df.filter(sf.col("limit") >= 70)

# Data from the last hour only
#df = df.filter((sf.col("time") > sf.unix_timestamp() - 3600))

# COMMAND ----------


df = df.withColumn("high_Alch_Margin", sf.round((sf.col("highalch") - sf.col("price")) - 100,1))
df.display()

# COMMAND ----------

# DBTITLE 1,Cell 18
# MAGIC %skip
# MAGIC df10 = df9.groupBy("id","highorlow").agg(sf.round(sf.avg("price"), 2).alias("avg_price"), sf.round(sf.std("price"), 2).alias("std_price"))
# MAGIC #join with volume data
# MAGIC df10 = df10.join(df_avg, "id").withColumn("std/avg", sf.round(sf.col("std_price")/sf.col("avg_price"), 3))
# MAGIC df10 = df10.join(df_map,"id")
# MAGIC df10 = df10.withColumn("high_Alch_Margin", sf.round((sf.col("highalch") - sf.col("avg_price")) - 100,1))
# MAGIC df10 = df10.withColumn("price-1hour_high_price", sf.round(sf.col("avg_price") - sf.col("avg1HourHigh") , 2))
# MAGIC df10 = df10.withColumn("limit_X_std_price", sf.col("std_price")*sf.col("limit"))
# MAGIC
# MAGIC
# MAGIC
# MAGIC df10 = df10.filter("highorlow = 'low'")
# MAGIC df10 = df10.filter("std_price >= 2")
# MAGIC df10 = df10.filter("avg1HourHighVolume > 1000")
# MAGIC df10 = df10.sort("std/avg", ascending=False)
# MAGIC df10.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC df8.display()
