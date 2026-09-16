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
# Type here in the cell editor to add code!
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

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW bronze_incremental AS
SELECT *
FROM BronzeLayer.dbo.Customer c
WHERE c.ingestion_timestamp > '{last_processed_timestamp}'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test it 
display(
    spark.sql("""
        SELECT *
        FROM BronzeLayer.dbo.Customer
        LIMIT 10
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Sure — in simple terms, you have completed these steps:
# 
# 1. **Created the `silver_customers` table** in the `Silver_Layer` Lakehouse with the required customer columns.
# 2. **Checked the last processed timestamp** from the Silver table. Since this is the first load, you set it to **`1900-01-01`**.
# 3. **Created the `bronze_incremental` temporary view** to identify new customer records from the Bronze layer based on `ingestion_timestamp`.
# 4. **Successfully accessed `BronzeLayer.dbo.Customer`** and verified that customer records are available, including the `ingestion_timestamp` column.

# MARKDOWN ********************

