# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Yellow Trips

# COMMAND ----------

import os
import urllib.request
import shutil
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from utils.functions import get_date_n_months_ago
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# Get date of download for two months previous
yyyy_mm = get_date_n_months_ago(n=2)

# Define dir path
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{yyyy_dd}"

# Define local path
local_path = dir_path + f"/yellow_tripdata_{yyyy_dd}.parquet"


# COMMAND ----------

try:
    #Check if local path exists
    dbutils.fs.ls(local_path)
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"File {local_path} already exists. \nSkipping download.")
except:
    try:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{yyyy_dd}.parquet"

        response = urllib.request.urlopen(url)

        dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{yyyy_dd}"

        os.makedirs(dir_path, exist_ok=True)

        local_path = dir_path + f"/yellow_tripdata_{yyyy_dd}.parquet"

        with open(local_path, 'wb') as f:
            shutil.copyfileobj(response, f)

        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print(f"Successfuly downloaded from {url} to {local_path}")
    except Exception as e:
        dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"Error downloading from {url} to {local_path}: {e}")

