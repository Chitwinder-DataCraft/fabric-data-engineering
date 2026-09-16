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

### 🥈 Create Silver Products Table — Creates the Delta table to store cleansed and transformed product data in the Silver layer.
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_products (
    product_id STRING,
    name STRING,
    category STRING,
    brand STRING,
    price DOUBLE,
    stock_quantity INT,
    rating DOUBLE,
    is_active BOOLEAN,
    price_category STRING,
    stock_status STRING,
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
last_processed_df = spark.sql("SELECT MAX(last_updated) as last_processed FROM silver_products")
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
FROM BronzeLayer.dbo.Product
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
    FROM BronzeLayer.dbo.Product
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

# Transform Silver Products — Cleanses product data, handles invalid values, derives price and stock categories, and adds the Silver-layer timestamp.
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW silver_incremental_products AS
SELECT
    product_id,
    name,
    category,
    brand,
    CASE
        WHEN price < 0 THEN 0
        ELSE price
    END AS price,
    CASE
        WHEN stock_quantity < 0 THEN 0
        ELSE stock_quantity
    END AS stock_quantity,
    CASE
        WHEN rating < 0 THEN 0
        WHEN rating > 5 THEN 5
        ELSE rating
    END AS rating,
    is_active,
    CASE
        WHEN price > 1000 THEN 'Premium'
        WHEN price > 100 THEN 'Standard'
        ELSE 'Budget'
    END AS price_category,
    CASE
        WHEN stock_quantity = 0 THEN 'Out of Stock'
        WHEN stock_quantity < 10 THEN 'Low Stock'
        WHEN stock_quantity < 50 THEN 'Moderate Stock'
        ELSE 'Sufficient Stock'
    END AS stock_status,
    CURRENT_TIMESTAMP() AS last_updated
FROM bronze_incremental
WHERE name IS NOT NULL AND category IS NOT NULL
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### 🔄 Merge Silver Products — Merges transformed incremental data into the Silver table, updating existing products and inserting new records.
spark.sql("""
MERGE INTO silver_products target
USING silver_incremental_products source
ON target.product_id = source.product_id
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

# Verify the final Silver customer table after MERGE

display(
    spark.sql("""
        SELECT *
        FROM Silver_Layer.dbo.silver_products
        ORDER BY product_id
        LIMIT 20
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("select count(*) from silver_products").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Silver Layer – Product Load
# 
# Transforms and incrementally loads product data from Bronze to Silver using PySpark.
# 
# ## Key Steps
# 
# - Identify new/updated products using watermarking.
# - Validate and cleanse product data.
# - Derive price and stock categories.
# - MERGE new and updated records into the Silver table.
# 
# ## Data Flow
# 
# `Bronze Product → Incremental Filter → Transformation → MERGE → Silver Product`
