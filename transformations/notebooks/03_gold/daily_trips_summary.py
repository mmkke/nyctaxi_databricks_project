# Databricks notebook source
# MAGIC %md
# MAGIC #Daily Trips Summary

# COMMAND ----------

from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Read in data

# COMMAND ----------

two_months_ago = datetime.now().replace(day=1) - relativedelta(months=2)
df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched").filter(f"tpep_pickup_datetime >= '{two_months_ago}'")


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
df_dts.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Append to Table

# COMMAND ----------

df_dts.write.mode("append").saveAsTable("nyctaxi.03_gold.daily_trip_summary")