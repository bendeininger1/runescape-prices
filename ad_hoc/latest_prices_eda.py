# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.table("runescape.02_silver.latest_prices_enriched").filter(col("price")< 10000)

# COMMAND ----------

# MAGIC %skip
# MAGIC df2 = df.filter(col("limit") > 100)
# MAGIC df2 = df2.filter(col("members")=="false")
# MAGIC df2.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC df3 = df.filter("id IN ('1731','1725','1317','1121','1073','1315','1161','1199') AND time >'1768419654'")
# MAGIC df3.display()

# COMMAND ----------

df3 = df.filter("id IN ('1731') AND time >'1768419654'")
df3.display()