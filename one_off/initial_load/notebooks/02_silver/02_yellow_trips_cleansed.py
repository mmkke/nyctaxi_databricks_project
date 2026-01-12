# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC #Yellow Trips Cleansed
# MAGIC
# MAGIC ###Cleaning raw bronze layer data for use in silver layer.

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), "../../../../"))

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from pyspark.sql.functions import col, when, timestamp_diff, max, min, date_format, lit
from utils.functions import get_widget, compute_month_range, parse_month_yyyy_mm, read_month_params
from utils.table_checks import assert_table_exists

# COMMAND ----------

df = spark.read.table("nyctaxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter out any trips outside of date range:

# COMMAND ----------

start_month, end_month, months_prev =read_month_params(
                                                    end_month_default="2025-10",
                                                    months_prev_default="12"
                                                    )

print(f"""
Date Range
----------------------------------
      start_month:  {start_month} 
      end_month:    {end_month}
      interval:     {months_prev} months
      """)

# COMMAND ----------


df = df.filter(
    (date_format(col("tpep_pickup_datetime"), "yyyy-MM") >= start_month.strftime("%Y-%m")) &
    (date_format(col("tpep_dropoff_datetime"), "yyyy-MM") <= end_month.strftime("%Y-%m"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now check max/min:

# COMMAND ----------

df.agg(
    max(col("tpep_pickup_datetime")).alias("max_tpep_pickup_datetime"),
    min(col("tpep_pickup_datetime")).alias("min_tpep_pickup_datetime"),
    max(col("tpep_dropoff_datetime")).alias("max_tpep_dropoff_datetime"),
    min(col("tpep_dropoff_datetime")).alias("min_tpep_dropoff_datetime")
    ).display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations
# MAGIC
# MAGIC Refer to layers schema for details.
# MAGIC
# MAGIC ###Transformations:
# MAGIC ---
# MAGIC **Remove Encoding**
# MAGIC
# MAGIC Some categories use numerical encoding instead of string values.
# MAGIC - VendorID -> vendor
# MAGIC - RatecodeID -> rate_type
# MAGIC - payment_type -> payment_type
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Alias Names** 
# MAGIC
# MAGIC Convert some names to ensure consistent naming conventions.
# MAGIC - PULocationID -> pu_location_id
# MAGIC - DOLocationID -> do_location_id
# MAGIC - Airport_fee -> airport_fee
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.select(
    # VendorID -> vendor
    # Remove numericla encodings
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
    .when(col("VendorID") == 2, "Curb Mobility, LLC")
    .when(col("VendorID") == 6, "Myle Technologies Inc")
    .when(col("VendorID") == 7, "Helix")
    .otherwise("Unknown")
    .alias("vendor"),

    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",

    # add column for trip duration in minutes
    timestamp_diff(
        'MINUTE', df.tpep_pickup_datetime, df.tpep_dropoff_datetime
        ).alias("trip_duration_mins"),
    
    "passenger_count",
    "trip_distance",

    # RatecodeID -> rate_type
    # Remove numericla encodings
    when(col("RatecodeID") == 1, "Standard rate")
    .when(col("RatecodeID") == 2, "JFK")
    .when(col("RatecodeID") == 3, "Newark")
    .when(col("RatecodeID") == 4, "Nassau or Westchester")
    .when(col("RatecodeID") == 5, "Negotiated fare")
    .when(col("RatecodeID") == 6, "Group ride")
    .otherwise("Unknown")
    .alias("rate_type"),

    "store_and_fwd_flag",
    col("PULOcationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),
    
    # Remove numericla encodings
    when(col("payment_type") == 0, "Flex Fare trip")
    .when(col("payment_type") == 1, "Credit card")
    .when(col("payment_type") == 2, "Cash")
    .when(col("payment_type") == 3, "No charge")
    .when(col("payment_type") == 4, "Dispute")
    .when(col("payment_type") == 5, "Unknown")
    .when(col("payment_type") == 6, "Voided trip")
    .otherwise("Unknown")
    .alias("payment_type"),

    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    col("Airport_fee").alias("airport_fee"),
    "cbd_congestion_fee",
    "processed_timestamp"
)

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save as Table

# COMMAND ----------

table = "nyctaxi.02_silver.yellow_trips_cleansed"
df.write.mode("overwrite").saveAsTable(table)

# COMMAND ----------

# MAGIC %md
# MAGIC Read back to check:

# COMMAND ----------

assert_table_exists(
    spark,
    table,
    require_readable=True,
    require_non_empty=True,
)
