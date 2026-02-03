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

# MAGIC %skip
# MAGIC df2 = df.filter("id IN ('1161')") #Adamant full helm
# MAGIC df2 = df2.filter("time > 1769616000") # exclude older data
# MAGIC df2 = df2.sort("NYCTime", ascending=False)
# MAGIC # df2.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC # std of buy prices
# MAGIC df2.filter("highorlow = 'high'").select(std("price")).show()

# COMMAND ----------

# MAGIC %skip
# MAGIC # std of sell prices
# MAGIC df2.filter("highorlow = 'low'").select(std("price")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Check to make sure there are no duplicate data in runescape.02_silver.latest_prices_cleansed

# COMMAND ----------

# MAGIC %skip
# MAGIC df = spark.read.table("runescape.02_silver.latest_prices_enriched")
# MAGIC df.count()

# COMMAND ----------

# MAGIC %skip
# MAGIC df.dropDuplicates()
# MAGIC df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC See which items have the highest alch value assuming 1 nature rune cost 100

# COMMAND ----------

# MAGIC %skip
# MAGIC nature_rune_cost = 100
# MAGIC
# MAGIC df3 = df.filter("time > 1769029986") # exclude older data
# MAGIC df3 = df3.withColumn("alch_result", col("highalch") - lit(nature_rune_cost) - col("price"))
# MAGIC df3 = df3.sort("alch_result", ascending=False)
# MAGIC # df3.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC df4 = df.filter("id IN ('1161','1731','1725','1199','1073')")
# MAGIC df4 = df4.filter("time > 1769701000") # exclude older data
# MAGIC df4 = df4.sort("NYCTime", ascending=False)
# MAGIC # df4.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC df5 = df4.groupBy("id","highorlow").agg(round(avg("price"), 2).alias("avg_price"), round(std("price"), 2).alias("std_price"))
# MAGIC df5 = df5.sort("id")
# MAGIC # df5.display()

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

# MAGIC %md
# MAGIC TODO remove outliers (beyond 3 stdev?)

# COMMAND ----------

# filter out low limit items
df9 = df.filter(sf.col("limit") >= 70)

# Data from the last hour only
#df9 = df9.filter((sf.col("time") > sf.unix_timestamp() - 3600))

# COMMAND ----------

# DBTITLE 1,Cell 18
df10 = df9.groupBy("id","highorlow").agg(sf.round(sf.avg("price"), 2).alias("avg_price"), sf.round(sf.std("price"), 2).alias("std_price"))
#join with volume data
df10 = df10.join(df_avg, "id").withColumn("std/avg", sf.round(sf.col("std_price")/sf.col("avg_price"), 3))
df10 = df10.join(df_map,"id")
df10 = df10.withColumn("high_Alch_Margin", sf.round((sf.col("highalch") - sf.col("avg_price")) - 100,1))
df10 = df10.withColumn("price-1hour_high_price", sf.round(sf.col("avg_price") - sf.col("avg1HourHigh") , 2))
df10 = df10.withColumn("limit_X_std_price", sf.col("std_price")*sf.col("limit"))



df10 = df10.filter("highorlow = 'low'")
df10 = df10.filter("std_price >= 2")
df10 = df10.filter("avg1HourHighVolume > 1000")
df10 = df10.sort("std/avg", ascending=False)
df10.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC df8.display()
