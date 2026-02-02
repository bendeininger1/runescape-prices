# Databricks notebook source
selecselectsesesefasdfafrom pyspark.sql.functions import count, max, min, avg, sum, round
from datetime import date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# Get the first dat of the month two months ago
two_months_ago_start = date.today().replace(day=1) - relativedelta(months=2)

# COMMAND ----------

from pyspark.sql.functions import count, max, min, avg, sum, round

# COMMAND ----------

# Load the enriched trip dataset
# and filter to only include trips with a pickup datetime later than the start date from two months ago
df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched").filter(f"tpep_pickup_datetime > '{two_months_ago_start}'")

# COMMAND ----------

# Aggreegate trip data by pickup date with key metrics

# group records by calendar date
df = df.\
    groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date")).\
        agg(
            count("*").alias("total_trips"), # total number of trips per day
            round(avg("passenger_count"),1).alias("average_passengers"), # average number of passengers per trip
            round(avg("trip_distance"),1).alias("average_distance"), # average trip distance (miles)
            round(avg("fare_amount"),2).alias("average_fare_per_trip"), # average fare per trip ($)
            max("fare_amount").alias("max_fare"), # highest single-fare-trip
            min("fare_amount").alias("min_fare"), # lowest single-fare-trip
            round(sum("fare_amount"),2).alias("total_revenue") # total revenue for the day ($)
        )

# COMMAND ----------

# Write the daily summary to a Unity Catalog managed Delta table in the gold schema
df.write.mode("append").saveAsTable("nyctaxi.03_gold.daily_trip_summary")