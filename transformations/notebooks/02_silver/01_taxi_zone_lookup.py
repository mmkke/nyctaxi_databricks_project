# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC # Taxi Zone Lookup Table

# COMMAND ----------

from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ###Load csv from landing:

# COMMAND ----------

df = spark.read.format("csv").options(header=True).load("/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Set schema:
# MAGIC

# COMMAND ----------

df = df.select(
                col("LocationID").cast(IntegerType()).alias("location_id"),
                col("Borough").alias("borough"),
                col("Zone").alias("zone"),
                col("service_zone"),
                current_timestamp().alias("effective_date"),
                lit(None).cast(TimestampType()).alias("end_date")
            )
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Load Delta Table for SCD2

# COMMAND ----------

# Load the SCD2 Delta table
dt = DeltaTable.forName(spark, "nyctaxi.02_silver.taxi_zone_lookup")

# Get current timestamp
end_timestamp = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD2 Step 1:  
# MAGIC
# MAGIC Set deltaTable as Target and dataframe as source.  
# MAGIC
# MAGIC Check if any ID in contians new or updated values. In cases where an update has occured, set the the `end_date` col value to `end_timestamp`. 
# MAGIC

# COMMAND ----------

dt.alias("t").\
    merge(
        source = df.alias("s"), 
        condition = "t.location_id = s.location_id AND t.end_date IS NULL AND (t.borough != s.borough OR t.zone != s.zone OR t.service_zone != s.service_zone)").\
    whenMatchedUpdate(
        set = {"end_date": lit(end_timestamp).cast(TimestampType())}).\
    execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD2 Step 2:
# MAGIC
# MAGIC Now insert a row for keys we just closed in Step 1 (no longer an active match).
# MAGIC 2. brand-new keys not present in the target.

# COMMAND ----------

# Get list of location_id's that have been closed in the previous step, ie where end_date = end_timestamp
updated_ids = [row_loc_id for row in dt.toDF().filter(f"end_date = '{end_timestamp}'").select("location_id").collect()]

# Only insert new records if any ids have been updated
if len(updated_ids) == 0:
    print("No new records to insert.")
else:
    # Insert new records using `whenNotMatchedInsert`
    # Merge on location ids NOT in the list of updated ids. 
    # We use NOT becuase there is no `whenMatchedInsert`
    # Insert new records when not matched from source dataframe, ie these ids ARE in the list of updated records
    dt.alias("t").\
        merge(
            source=df.alias("s"), 
            condition=f"s.location_id NOT IN ({(', ').join(map(str, updated_ids ))})"
            ).\
        whenNotMatchedInsert(
                            values={
                                "t.location_id": "s.location_id",
                                "t.borough": "s.borough",
                                "t.zone": "s.zone",
                                "t.effective_date": current_timestamp(),
                                "t.end_date": lit(None).cast(TimestampType())}
                            ).\
        execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD2 Step 3:  
# MAGIC
# MAGIC Now insert a row for brand-new keys not present in the target.

# COMMAND ----------

# Merge tables on location_id
# When records not match INSERT from source dataframe
dt.alias("t").\
    merge(
        source = df.alias("s"),
        condition = "s.location_id = t.location_id"
        ).\
    whenNotMatchedInsert(
        values = {
            "t.location_id": "s.location_id",
            "t.borough": "s.borough",        
            "t.zone": "s.zone",
            "t.effective_date": current_timestamp(),
            "t.end_date": lit(None).cast(TimestampType())}
        ).\
    execute()

# COMMAND ----------

# MAGIC %md
# MAGIC Display table:

# COMMAND ----------

dt.toDF().display()