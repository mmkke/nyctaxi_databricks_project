# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC #Yellow Trips Raw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Raw Data Into Tables

# COMMAND ----------

import sys

PROJECT_ROOT = "/Workspace/Shared/nyctaxi_project"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    
from utils.table_checks import assert_table_exists
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import current_timestamp


# COMMAND ----------

# MAGIC %md
# MAGIC Select the date two months form the current date, and then read in the relevant file. 

# COMMAND ----------

# Get date of download for two months previous
two_months_ago = datetime.now() - relativedelta(months=2)
yyyy_dd = two_months_ago.strftime("%Y-%m")

# Define dir path
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{yyyy_dd}"

# Set dir
# Gets all parquet files (recursive)
df = spark.read.format("parquet").load(dir_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Add column for load date/time:

# COMMAND ----------

df = df.withColumn("processed_timestamp", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC Initial load of the data:

# COMMAND ----------

# Uses default format with is a delta lake table.
table_name = "nyctaxi.01_bronze.yellow_trips_raw"
df.write.mode("append").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Check table existance:

# COMMAND ----------

assert_table_exists(
    spark,
    table_name,
    require_readable=True,
    require_non_empty=True
)