# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC # Fetch Data

# COMMAND ----------

import urllib.request
import shutil
import os
import sys

PROJECT_ROOT = "/Workspace/Shared/nyctaxi_project"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from utils.functions import get_widget, parse_month_yyyy_mm, compute_month_range, read_month_params, generate_month_list

# COMMAND ----------

END_MONTH_DEFAULT = '2025-09'
MONTHS_PREV_DEFAULT = '2'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get data and store in landing volume.

# COMMAND ----------

# Read parameters start_date (YYYY-MM), end_date (YYYY-MM), months_prev (int)
start_date, end_date, months_prev = read_month_params(
                                                    end_month_default=END_MONTH_DEFAULT, 
                                                    months_prev_default=MONTHS_PREV_DEFAULT
                                                    )
# Compute months list (inclusive range)
months_to_process = generate_month_list(start_date, end_date)

print("Months to process:")
for yyyy_dd in months_to_process:
    print(f"    {yyyy_dd}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loop through months and download files, then save as Parquets

# COMMAND ----------

for i, yyyy_dd in enumerate(months_to_process):

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{yyyy_dd}.parquet"

    response = urllib.request.urlopen(url)

    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{yyyy_dd}"

    os.makedirs(dir_path, exist_ok=True)

    local_path = dir_path + f"/yellow_tripdata_{yyyy_dd}.parquet"

    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)

    print(f"""
          -------------------------
          Download: {i+1}/{len(months_to_process)}
          Month: {yyyy_dd}
          Saved: {url}
          To: {local_path}
          \n""")

if get_widget('job_id', '') != '':
    print(f"Job ID:      {dbutils.widgets.get('job_id')}")
    print(f"Job Run ID:  {dbutils.widgets.get('job_run_id')}")
    print(f"Job Name:    {dbutils.widgets.get('job_name')}")
    print()
print(f"Backfill {months_to_process[0]} -> {months_to_process[-1]} Complete")
