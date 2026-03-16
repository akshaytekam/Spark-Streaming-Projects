# End to End Data Engineering Project using Databricks | Lakeflow Spark Declarative Pipeline
## Introduction:

*This project demonstrates how to build a complete data engineering pipeline using Databricks and Lakeflow Spark Declarative Pipelines. 
The project simulates a real-world transportation domain scenario where trip data is processed and converted into analytics-ready datasets.*


**The pipeline architecture includes:**

- Data ingestion
- Data cleaning and transformation
- Data modeling
- Data aggregation
- Analytics-ready outputs

**With Declarative Pipelines, you describe What data should look like, not how to process it**

**Lakeflow Declarative Pipelines is a framework that allows engineers to create batch and streaming pipelines using SQL or Python, while Spark handles execution automatically.**

**Key benefits:**

- Less code
- Automatic orchestration
- Incremental processing
- Built-in data quality checks
- Unified batch + streaming pipelines

**Project Architecture Overview**

*The project uses a Lakehouse architecture with Medallion Layers.*

1. Bronze Layer : Raw data ingestion.

2. Silver Layer: Cleaned and transformed data.

3. Gold Layer: Business-ready aggregated data.

**Business Problem**

*The project simulates a ride-sharing transportation company.*

Data available:

1️⃣ City Dimension Table

Contains:

city_id

city_name

2️⃣ Trip Fact Table

Contains trip details such as:

trip_id

driver_id

customer_id

distance

fare amount

ratings

timestamps

Goal of the pipeline:

Produce city-level analytics such as

total rides

total revenue

average customer rating

ride distribution by time

This dataset will later be used for BI dashboards.

**Change Data Capture (CDC)**

*One important concept explained is Auto CDC (Change Data Capture).*

Instead of reprocessing the entire dataset every time:

CDC only processes:

- new records
- updated records
- deleted records

Benefits:

- faster processing
- reduced compute cost
- scalable pipelines

This is extremely important for large production datasets.

**Calendar Dimension Table**

The pipeline also generates a calendar dimension table programmatically.

This table contains:

- date
- day
- month
- quarter
- weekday/weekend
- holidays

**Gold Layer (Analytics Tables)**

The Gold layer combines:

- trip data
- city data
- calendar data

The result is a fully enriched analytics dataset.

**Pipeline Execution and Automation**

Lakeflow Declarative Pipelines automatically manages:
- Pipeline DAG
- Dependencies between tables.

Trips → Silver Trips → Gold Fact Table

**Key Advantages of Declarative Pipelines**

1️⃣ Less Code

Declarative pipelines require significantly fewer lines of code.

2️⃣ Automatic Orchestration

Execution order and dependencies are automatically handled.

3️⃣ Incremental Processing

Only new data is processed.

4️⃣ Unified Batch + Streaming

Both workloads can run on the same pipeline framework.

5️⃣ Built-in Data Quality

Constraints and validation rules are easy to implement.

## Final Pipeline Workflow

CSV Data (S3)
      ↓
Auto Loader
      ↓
Bronze Tables (Raw)
      ↓
Silver Tables (Cleaned + Validated)
      ↓
Calendar Dimension
      ↓
Gold Tables (Analytics Ready)
      ↓
City Views
      ↓
BI Dashboards
