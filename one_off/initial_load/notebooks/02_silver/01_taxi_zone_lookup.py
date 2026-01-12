# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC # Taxi Zone Lookup Table

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), "../../../../"))

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from utils.table_checks import assert_table_exists

from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ###Load csv:

# COMMAND ----------

df = spark.read.format("csv").options(header=True).load("/Volumes/nyctaxi/00_landing/data_sources/lookup/")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Set schema:
# MAGIC

# COMMAND ----------

df = df.select(
                col("LocationID").cast(IntegerType()).alias("location_id"),
                col("Borough").alias("borough"),
                col("Zone").alias("zone"),
                col("service_zone").alias("service_zone"),
                current_timestamp().alias("effective_date"),
                lit(None).cast(TimestampType()).alias("end_date")
            )
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Check DataFrame:

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write as a delta table:

# COMMAND ----------

table = "nyctaxi.02_silver.taxi_zone_lookup"
df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(table)

# COMMAND ----------

assert_table_exists(
    spark,
    table,
    require_readable=True,
    require_non_empty=True,
)
