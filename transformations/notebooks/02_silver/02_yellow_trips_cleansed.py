# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC #Yellow Trips Cleansed
# MAGIC
# MAGIC ###Cleaning raw bronze layer data for use in silver layer.

# COMMAND ----------

from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, when, max, min

# COMMAND ----------

# In the format `yyyy-mm-dd` get the bounds of the target month
two_months_ago_start = datetime.now().replace(day=1) - relativedelta(months=2)
two_months_ago_end = datetime.now().replace(day=1) - relativedelta(months=1)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read table and filter out any trips outside of target date range:

# COMMAND ----------

df = spark.read.table("nyctaxi.01_bronze.yellow_trips_raw").filter(f"tpep_pickup_datetime >= '{two_months_ago_start}' AND tpep_pickup_datetime < '{two_months_ago_end}'")


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
# MAGIC ## Append to Table

# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi.02_silver.yellow_trips_cleansed")
