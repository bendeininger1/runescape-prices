# Databricks notebook source
# TODO remove any unused 
from pyspark.sql.functions import current_timestamp, col, flatten, array, expr, lit
from datetime import datetime, date, timezone
from time import time
import urllib.request
import os
import shutil
import json

# COMMAND ----------

url = 'https://prices.runescape.wiki/api/v1/osrs/1h'

#use headers so the wiki people dont hate me
headers = {
    'User-Agent': 'Bens RS 1h prices user agent 1.0 @ghost9420 is my discord',
    'From': 'bendeininger2@gmail.com'  # This is another valid field
}
req = urllib.request.Request(
    url,
    headers=headers
)

# Open a connection and stream the remote file
response = urllib.request.urlopen(req)

# Define and create the local directory for this date's data
dir_path = "/Volumes/runescape/00_landing/data_sources/1h_prices"
os.makedirs(dir_path, exist_ok=True)

# get unix time for file name
unix_timestamp = int(time())

# Define the full path for the downloaded data file
file_path = f"/Volumes/runescape/00_landing/data_sources/1h_prices/1h_prices_{unix_timestamp}.json"

# Save the streamed content to the local file in binary mode
with open(file_path, "wb") as file:
    shutil.copyfileobj(response, file) # Copy data from response to file

# set task value for ETL pipeline
dbutils.jobs.taskValues.set(key="ingest_timestamp", value=unix_timestamp)