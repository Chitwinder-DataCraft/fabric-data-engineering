# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "61419789-976c-430c-9e6f-da2b9cbc6d54",
# META       "default_lakehouse_name": "Silver_Layer",
# META       "default_lakehouse_workspace_id": "1260db2a-9814-4def-ab4b-251121e96891",
# META       "known_lakehouses": [
# META         {
# META           "id": "cf411b4f-e934-473f-8919-7abd3e23cd38"
# META         },
# META         {
# META           "id": "61419789-976c-430c-9e6f-da2b9cbc6d54"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Create Silver Orders Table — Creates the Delta table to store cleansed and transformed product data in the Silver layer.

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    total_amount DOUBLE,
    transaction_date DATE,
    order_status STRING,
    last_updated TIMESTAMP
)
USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the latest processed timestamp for incremental loading
last_processed_df = spark.sql("SELECT MAX(last_updated) as last_processed FROM silver_orders")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']
if last_processed_timestamp is None:
    last_processed_timestamp = "1900-01-01T00:00:00.000+00:00"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Last processed timestamp:", last_processed_timestamp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a temporary view containing new Bronze records for incremental processing
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW bronze_incremental AS
SELECT *
FROM BronzeLayer.dbo.Orders
WHERE ingestion_timestamp > '{last_processed_timestamp}'
""")

print("bronze_incremental created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### 🔄 Create Bronze Incremental View — Filters newly ingested product records from the Bronze layer based on the last processed timestamp.
bronze_incremental_df = spark.sql(f"""
    SELECT *
    FROM BronzeLayer.dbo.Orders
    WHERE ingestion_timestamp > '{last_processed_timestamp}'
""")

# Create temporary view from the DataFrame
bronze_incremental_df.createOrReplaceTempView("bronze_incremental")

print("bronze_incremental created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Transform Silver Orders --> Cleanses and transforms incremental order data, validates key fields, derives order status, and prepares records for the Silver layer.
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW silver_incremental_orders AS
SELECT
    transaction_id as order_id,
    customer_id,
    product_id,
    CASE
        WHEN quantity < 0 THEN 0
        ELSE quantity
    END AS quantity,
    CASE
        WHEN total_amount < 0 THEN 0
        ELSE total_amount
    END AS total_amount,
    CAST(transaction_date AS DATE) AS transaction_date,
    CASE
        WHEN quantity = 0 AND total_amount = 0 THEN 'Cancelled'
        WHEN quantity > 0 AND total_amount > 0 THEN 'Completed'
        ELSE 'In Progress'
    END AS order_status,
    CURRENT_TIMESTAMP() AS last_updated
FROM bronze_incremental
WHERE transaction_date IS NOT NULL 
  AND customer_id IS NOT NULL 
  AND product_id IS NOT NULL
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### 🔄 Merge Silver Orders — Merges transformed incremental data into the Silver table, updating existing products and inserting new records.
spark.sql("""
MERGE INTO silver_orders target
USING silver_incremental_orders source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the final Silver Orders table after MERGE

display(
    spark.sql("""
        SELECT *
        FROM Silver_Layer.dbo.silver_orders
        ORDER BY order_id
        LIMIT 20
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("select count(*) from silver_orders").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Silver Layer – Order Load
# 
# Transforms and incrementally loads order data from Bronze to Silver using PySpark.
# 
# ## Key Steps
# 
# - Identify new/updated orders using watermarking.
# - Validate and cleanse order data.
# - Derive order status and standardize values.
# - MERGE new and updated records into the Silver table.
# 
# ## Data Flow
# 
# `Bronze Order → Incremental Filter → Transformation → MERGE → Silver Order`
