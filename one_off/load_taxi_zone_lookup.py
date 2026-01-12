# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC #Fetch Taxi Zone Lookup Table

# COMMAND ----------

import urllib.request
import shutil
import os

# COMMAND ----------

url = f"https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

response = urllib.request.urlopen(url)

dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/lookup"

os.makedirs(dir_path, exist_ok=True)

local_path = dir_path + "/taxi_zone_lookup.csv"

with open(local_path, 'wb') as f:
    shutil.copyfileobj(response, f)