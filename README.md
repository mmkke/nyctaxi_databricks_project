# NYC Taxi Data Platform on Databricks

## Overview

This project implements a **production-style data platform** for the NYC Taxi dataset using **Databricks, PySpark, Spark SQL, and Delta Lake**. The pipeline follows a **Bronze / Silver / Gold medallion architecture**, supports both **historical backfills** and **monthly incremental ingestion**, and incorporates **Slowly Changing Dimension (SCD) Type 2** modeling to preserve historical changes in reference data.

The project is structured to closely mirror real-world data engineering workflows, emphasizing **automation, idempotency, governance, and analytics readiness**.

---

## Architecture

![Project Architecture](project_architecture.png)

The platform is organized into four logical layers:

### Landing
- Raw NYC Taxi parquet files staged in a **Unity Catalog Volume**
- Partitioned by `year-month` to support efficient backfills and incremental loads
- Lookup/reference datasets (e.g., taxi zones) staged alongside raw sources

### Bronze
- Raw ingestion of taxi trip data into Delta tables
- Minimal transformations to preserve source fidelity
- Adds ingestion metadata (e.g., `processed_timestamp`)
- Acts as the immutable system of record

### Silver
- Cleansed and conformed datasets
- Derived fields added (e.g., `trip_duration_mins`)
- Enrichment with pickup/dropoff borough and zone attributes
- **SCD Type 2** implemented for taxi zone lookup data using effective and end dates

### Gold
- Analytics-ready fact tables
- Pre-aggregated metrics optimized for BI and reporting
- Supports historical analysis and time-based comparisons

---

## Slowly Changing Dimensions (SCD Type 2)

The taxi zone lookup table is modeled as an **SCD Type 2 dimension** to track historical changes over time.

Key features:
- Versioned records with `effective_date` and `end_date`
- Current-record identification
- Idempotent updates using Delta Lake `MERGE` operations
- Enables accurate point-in-time analytics

---

## Data Pipelines & Job Design

Two primary workflows drive the platform:

### 1. Historical Backfill Pipeline
- Loads historical NYC Taxi data across all months
- Populates Bronze, Silver, and Gold layers
- Designed to be safely re-runnable
- Handles large-scale ingestion efficiently

### 2. Monthly Incremental Pipeline
- Processes newly available monthly data
- Applies incremental logic only
- Updates SCD Type 2 dimensions
- Keeps analytics tables current with minimal compute usage

Both pipelines are implemented as **Databricks Jobs** and are designed to be **idempotent and fault-tolerant**.


### Directory Highlights

- **one_off/**  
  One-time setup and historical backfill workflows (catalog creation, schema setup, historical loads).

- **transformations/**  
  Production pipelines for recurring monthly ingestion and transformation.

- **utils/**  
  Shared utility functions for parameter handling, validation, and table checks.

- **ad_hoc/**  
  Exploratory analysis and experimentation notebooks.

---

## Technologies Used

- **Databricks**
- **PySpark**
- **Spark SQL**
- **Delta Lake**
- **Unity Catalog (Volumes, Schemas)**
- **Databricks Jobs**
- **Medallion Architecture**
- **SCD Type 2 Dimensional Modeling**

---

## Key Design Principles

- **Scalability**: Handles large historical datasets and continuous growth
- **Reliability**: Idempotent pipelines and ACID guarantees via Delta Lake
- **Maintainability**: Clear separation of concerns across layers
- **Governance**: Unity Catalog–managed storage and schemas
- **Analytics Readiness**: Gold tables optimized for BI and downstream consumers

---

## Summary

This project demonstrates how to build a **production-grade analytics platform** on Databricks using modern data engineering patterns. It combines automated ingestion, incremental processing, historical change tracking, and analytics-ready data modeling to support scalable and reliable insights from NYC Taxi data.
