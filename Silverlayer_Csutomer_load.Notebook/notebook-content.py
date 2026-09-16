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
# META           "id": "61419789-976c-430c-9e6f-da2b9cbc6d54"
# META         },
# META         {
# META           "id": "cf411b4f-e934-473f-8919-7abd3e23cd38"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Create Silver customer table for transformed customer data
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id STRING,
    name STRING,
    email STRING,
    country STRING,
    customer_type STRING,
    registration_date DATE,
    age INT,
    gender STRING,
    total_purchases INT,
    customer_segment STRING,
    days_since_registration INT,
    last_updated TIMESTAMP)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the latest processed timestamp for incremental loading
last_processed_df = spark.sql("SELECT MAX(last_updated) as last_processed FROM silver_customers")
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
FROM BronzeLayer.dbo.Customer
WHERE ingestion_timestamp > '{last_processed_timestamp}'
""")

print("bronze_incremental created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read new Bronze records using the watermark and create a temporary incremental view
bronze_incremental_df = spark.sql(f"""
    SELECT *
    FROM BronzeLayer.dbo.Customer
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

# Transform incremental Bronze data, validate records, and derive customer attributes
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW silver_incremental AS
SELECT
    customer_id,
    name,
    email,
    country,
    customer_type,
    registration_date,
    age,
    gender,
    total_purchases,
    CASE
        WHEN total_purchases > 10000 THEN 'High Value'
        WHEN total_purchases > 5000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment,
    DATEDIFF(CURRENT_DATE(), registration_date) AS days_since_registration,
    CURRENT_TIMESTAMP() AS last_updated
FROM bronze_incremental
WHERE 
    age BETWEEN 18 AND 100
    AND email IS NOT NULL
    AND total_purchases >= 0
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the transformed Silver records before loading into the permanent Silver table
display(
    spark.sql("""
        SELECT *
        FROM silver_incremental
        LIMIT 10
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge transformed records into Silver, updating existing customers and inserting new ones

spark.sql("""
MERGE INTO Silver_Layer.dbo.silver_customers AS target
USING silver_incremental AS source

ON target.customer_id = source.customer_id

WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *
""")

print("Silver MERGE completed successfully")

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
        FROM Silver_Layer.dbo.silver_customers
        ORDER BY customer_id
        LIMIT 20
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("select count(*) from silver_customers").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Silver Layer – Customer Load
# 
# Transforms and incrementally loads customer data from Bronze to Silver using PySpark.
# 
# ### Key Steps
# - Identify new/updated records using watermarking.
# - Validate and transform customer data.
# - Derive customer segment and registration days.
# - MERGE new and updated records into the Silver table.
# 
# ### Data Flow
# `Bronze Customer → Incremental Filter → Transformation → MERGE → Silver Customer`
