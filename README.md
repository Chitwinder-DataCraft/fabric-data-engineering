# Microsoft Fabric Data Engineering

An end-to-end Microsoft Fabric data engineering project demonstrating a
Bronze → Silver → Gold architecture with automated ingestion, PySpark
transformations, and incremental data loading.

## Architecture

```text
Source Data
    │
    ▼
Data Ingestion Pipelines
    │
    ▼
Bronze Layer
    │
    │ PySpark Transformation
    ▼
Silver Layer
    │
    ▼
Gold Layer

Technologies
Microsoft Fabric
OneLake
Lakehouse
PySpark
Spark SQL
Data Factory Pipelines
Delta Lake
GitHub
Incremental Data Loading
Current Project Components
Bronze Layer

Contains raw ingested customer, order, and product data.

Silver Layer

Contains cleaned and transformed data ready for analytics.

Current table:

silver_customers
Pipelines
Ingest_Customer_data
Ingest_Orders_data
Ingest_Product_data
Notebook

The PySpark notebook implements incremental processing by comparing the
Bronze ingestion_timestamp with the latest processed timestamp in the
Silver layer.

Incremental Loading

The project uses a watermark-based incremental loading pattern:

Bronze ingestion_timestamp
          │
          ▼
Latest Silver last_updated
          │
          ▼
Filter newly ingested records
          │
          ▼
Transform
          │
          ▼
Load into Silver

For the initial load, the watermark is set to:

1900-01-01

Subsequent runs process only records newer than the last processed timestamp.

Project Goals

This project demonstrates practical data engineering concepts including:

Lakehouse architecture
Medallion architecture
Data ingestion
PySpark transformations
Spark SQL
Incremental data processing
Delta tables
Data quality and cleansing
Git version control
Microsoft Fabric development
Author

Chitwinder Singh

Data Architect | Power BI | Microsoft Fabric | SQL | Analytics

GitHub: Chitwinder-DataCraft

