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

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Set dir
# Gets all parquet files (recursive)
df = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Add column for load date/time:

# COMMAND ----------

df = df.withColumn("processed_timestamp", current_timestamp())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Initial load of the data:

# COMMAND ----------

# Uses default format with is a delta lake table.
table = "nyctaxi.01_bronze.yellow_trips_raw"
df.write.mode("overwrite").saveAsTable(table)

# COMMAND ----------

# MAGIC %md
# MAGIC Check table existance:

# COMMAND ----------

assert_table_exists(
    spark,
    table,
    require_readable=True,
    require_non_empty=True,
)
