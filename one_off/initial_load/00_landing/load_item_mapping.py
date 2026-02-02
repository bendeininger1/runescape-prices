# Databricks notebook source
import urllib.request
import os
import shutil

url = 'https://prices.runescape.wiki/api/v1/osrs/mapping'

#use headers so the wiki people dont hate me
headers = {
    'User-Agent': 'Bens RS item mapping user agent 1.0 @ghost9420 is my discord',
    'From': 'bendeininger1@gmail.com'  # This is another valid field
}
req = urllib.request.Request(
    url, 
    headers=headers
)

# Open a connection and stream the remote file
response = urllib.request.urlopen(req)

# Define and create the local directory for this date's data
dir_path = "/Volumes/runescape/00_landing/data_sources/mapping"
os.makedirs(dir_path, exist_ok=True)

# Define the full path for the downloaded data file
local_path = "/Volumes/runescape/00_landing/data_sources/mapping/item_mapping.json"

# Save the streamed content to the local file in binary mode
with open(local_path, "wb") as f:
    shutil.copyfileobj(response, f) # Copy data from response to file