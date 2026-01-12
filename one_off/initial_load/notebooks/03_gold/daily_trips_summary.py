# Databricks notebook source
# MAGIC %md
# MAGIC #Daily Trips Summary

# COMMAND ----------

import sys

PROJECT_ROOT = "/Workspace/Shared/nyctaxi_project"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from utils.table_checks import assert_table_exists

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Read in data

# COMMAND ----------

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ##Aggregations
# MAGIC

# COMMAND ----------

df_dts = df.\
        groupBy(F.col("tpep_pickup_datetime").cast("date").alias("pickup_date")).\
            agg(
                F.count("*").alias("total_trips"),
                F.round(F.avg("passenger_count"), 1).alias("avg_passengers_per_trip"),
                F.round(F.avg("trip_distance"), 1).alias("avg_distance_per_trip"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare_per_trip"),
                F.max("fare_amount").alias("max_fare"),
                F.min("fare_amount").alias("min_fare"),
                F.round(F.sum("fare_amount"), 2).alias("total_revenue")
            )

# COMMAND ----------

df_dts.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Write data table

# COMMAND ----------

table = "nyctaxi.03_gold.daily_trip_summary"
df_dts.write.mode("overwrite").saveAsTable(table)

# COMMAND ----------

# MAGIC %md
# MAGIC check table existence:

# COMMAND ----------

assert_table_exists(
    spark,
    table,
    require_readable=True,
    require_non_empty=True,
)