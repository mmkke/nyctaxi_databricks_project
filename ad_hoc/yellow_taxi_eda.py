# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC #Yellow Taxi EDA
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Read in Data

# COMMAND ----------

# MAGIC %md
# MAGIC Yellow Trips Enriched:

# COMMAND ----------

df_silver = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")
df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Daily Trip Summaries:

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## KPIs

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Vendor Revenue:

# COMMAND ----------

df_silver.groupBy("vendor").\
            agg(
                F.sum("total_amount").alias("total_revenue")
                ).\
            orderBy("total_revenue", ascending=False).\
            display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ###Pickup Borough

# COMMAND ----------

df_silver.groupBy("pu_borough").\
            agg(
                F.count('*').alias("total_trips")
                ).\
            orderBy("total_trips", ascending=False).\
            display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ###Borough to Borough

# COMMAND ----------

df_silver.groupBy("pu_borough", "do_borough").\
            agg(
                F.count('*').alias("total_trips")
                ).\
            orderBy("total_trips", ascending=False).\
            display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ###Time Series Analysis

# COMMAND ----------

display(df_dts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pickup Time Analysis

# COMMAND ----------

df_silver.withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime"))).\
            groupBy("pickup_hour").\
            agg(
                F.count('*').alias("total_trips"),
                F.avg("trip_distance").alias("avg_distance_per_trip"),
                F.avg("fare_amount").alias("avg_fare_per_trip")).\
            orderBy("pickup_hour").\
            display()