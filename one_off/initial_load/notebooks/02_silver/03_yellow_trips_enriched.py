# Databricks notebook source
# MAGIC %md
# MAGIC # Yellow Trips Enriched

# COMMAND ----------

# MAGIC %md
# MAGIC Read in `yellow_trips_cleansed` data table:

# COMMAND ----------

import sys

PROJECT_ROOT = "/Workspace/Shared/nyctaxi_project"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from utils.table_checks import assert_table_exists

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Load Tables

# COMMAND ----------

df_ytc = spark.read.table("nyctaxi.02_silver.yellow_trips_cleansed")
df_ytc.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Read in `taxi_zone_lookup` table:

# COMMAND ----------

df_lookup = spark.read.table("nyctaxi.02_silver.taxi_zone_lookup")
df_lookup.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ##Join Tables

# COMMAND ----------

# MAGIC %md
# MAGIC Join `df_ytc` and `df_lookup` on the pick-up location ID(`pu_location_id`) and the drop-ff location id (`do_location_id`). 

# COMMAND ----------

ytc   = df_ytc.alias("ytc")
lu_pu = df_lookup.alias("lu_pu")
lu_do = df_lookup.alias("lu_do")

df_final = (ytc
            .join(lu_pu, col("ytc.pu_location_id") == col("lu_pu.location_id"), "left")
            .join(lu_do, col("ytc.do_location_id") == col("lu_do.location_id"), "left")
            .select(
                col("ytc.vendor"),
                col("ytc.tpep_pickup_datetime"),
                col("ytc.tpep_dropoff_datetime"),
                col("ytc.trip_duration_mins"),
                col("ytc.passenger_count"),
                col("ytc.trip_distance"),
                col("ytc.rate_type"),
                col("ytc.store_and_fwd_flag"),
                
                # Add the new columns
                col("lu_pu.borough").alias("pu_borough"),
                col("lu_do.borough").alias("do_borough"),
                col("lu_pu.zone").alias("pu_zone"),
                col("lu_do.zone").alias("do_zone"),

                col("ytc.payment_type"),
                col("ytc.fare_amount"),
                col("ytc.extra"),
                col("ytc.mta_tax"),
                col("ytc.tip_amount"),
                col("ytc.tolls_amount"),
                col("ytc.improvement_surcharge"),
                col("ytc.total_amount"),
                col("ytc.congestion_surcharge"),
                col("ytc.airport_fee"),
                col("ytc.cbd_congestion_fee"),
                col("ytc.processed_timestamp")
                )
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Review

# COMMAND ----------

df_final.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write New Table

# COMMAND ----------

table = "nyctaxi.02_silver.yellow_trips_enriched"
df_final.write.mode("overwrite").saveAsTable(table)

# COMMAND ----------

# MAGIC %md
# MAGIC Check tables existence:

# COMMAND ----------

assert_table_exists(
    spark,
    table,
    require_readable=True,
    require_non_empty=True,
)