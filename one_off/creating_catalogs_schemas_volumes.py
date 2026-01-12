# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC # Set Up
# MAGIC
# MAGIC ### Creating Project Catalogs, Schemas, and Volumes

# COMMAND ----------

# MAGIC %md
# MAGIC Set up the catalog:

# COMMAND ----------

catalog_sql = """
    CREATE CATALOG IF NOT EXISTS nyctaxi
    MANAGED LOCATION 'abfss://unity-catalog-storage@dbstorageuysu4xmttegry.dfs.core.windows.net/358315401255110'
    """

spark.sql(catalog_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC Create schemas for each layer:

# COMMAND ----------

spark.sql("create schema if not exists nyctaxi.00_landing")
spark.sql("create schema if not exists nyctaxi.01_bronze")
spark.sql("create schema if not exists nyctaxi.02_silver")
spark.sql("create schema if not exists nyctaxi.03_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC Create volume:

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS nyctaxi.00_landing.data_sources")