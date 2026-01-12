# Databricks notebook source
# MAGIC %md
# MAGIC ## Review count of monthly records and SCD2 updates.

# COMMAND ----------

from pyspark.sql.functions import sum, date_format, count

# COMMAND ----------

# MAGIC %md
# MAGIC Review raw data records by month:

# COMMAND ----------

spark.read.table("nyctaxi.01_bronze.yellow_trips_raw").\
    groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("pickup_month")).\
    agg(count("*").alias("total_records")).\
    orderBy("pickup_month").\
    display()

# COMMAND ----------

# MAGIC %md
# MAGIC Review cleansed data records by month:

# COMMAND ----------

spark.read.table("nyctaxi.02_silver.yellow_trips_cleansed").\
    groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("pickup_month")).\
    agg(count("*").alias("total_records")).\
    orderBy("pickup_month").\
    display()

# COMMAND ----------

spark.read.table("nyctaxi.03_gold.daily_trip_summary").\
    groupBy(date_format("pickup_date", "yyyy-MM").alias("pickup_month")).\
    agg(sum("total_trips").alias("total_trips")).\
    orderBy("pickup_month").\
    display()

# COMMAND ----------

# MAGIC %md
# MAGIC Review taxi zone lookup table for SCD2 updates:

# COMMAND ----------

spark.read.table("nyctaxi.02_silver.taxi_zone_lookup").filter(f"end_date IS NOT NULL").display()