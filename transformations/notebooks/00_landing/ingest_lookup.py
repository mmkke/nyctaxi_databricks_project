# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Taxi Zone Lookup Table

# COMMAND ----------

# MAGIC %md
# MAGIC Downlaod Taxi Zone lookup table. If successfully set the job task value `continue_downstream` to 'yes', otherwise 'no'. 

# COMMAND ----------

import urllib.request
import shutil
import os

try:
    url = f"https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    response = urllib.request.urlopen(url)

    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/lookup"

    os.makedirs(dir_path, exist_ok=True)

    local_path = dir_path + "/taxi_zone_lookup.csv"

    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)

    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print(f"Successfully Downloaded file {url}. ")
except Exception as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"Error downloading file {url}: {e}")

