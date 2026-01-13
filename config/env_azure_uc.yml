# Azure Databricks + Unity Catalog
env_name: azure_uc

platform:
  use_unity_catalog: true

naming:
  catalog: nyctaxi
  bronze_schema: bronze
  silver_schema: silver
  gold_schema: gold
  lookup_schema: silver

storage:
  # UC managed location / external storage that backs the catalog or volumes
  managed_location: "abfss://unity-catalog-storage@dbstorageuysu4xmttegry.dfs.core.windows.net/358315401255110"

  # Optional: if you use UC volumes for landing/staging, define them here
  # (You can point notebooks to these if you created the volumes.)
  landing_path: "/Volumes/nyctaxi/landing"
  checkpoints_path: "/Volumes/nyctaxi/checkpoints"
  exports_path: "/Volumes/nyctaxi/exports"

ingestion:
  dataset: yellow
  file_format: parquet
  partition_col: "year_month"
  overwrite_landing: false

tables:
  bronze_trips: yellow_trips_raw
  silver_trips_cleansed: yellow_trips_cleansed
  silver_trips_enriched: yellow_trips_enriched
  silver_zone_dim: taxi_zone_lookup_scd2
  gold_daily_summary: daily_trips_summary

scd2:
  enabled: true
  key_cols: ["location_id"]
  compare_cols: ["borough", "zone", "service_zone"]
  effective_col: "effective_date"
  end_col: "end_date"
  current_flag_col: "is_current"
  open_end_date: "9999-12-31"

run:
  default_end_month: "2025-10"
  default_prev_months: 12
  demo_mode: false