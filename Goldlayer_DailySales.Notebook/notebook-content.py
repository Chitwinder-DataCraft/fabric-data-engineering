# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "76b27e57-ae84-43cf-8a58-0695a20a4165",
# META       "default_lakehouse_name": "Gold_Layer",
# META       "default_lakehouse_workspace_id": "1260db2a-9814-4def-ab4b-251121e96891",
# META       "known_lakehouses": [
# META         {
# META           "id": "61419789-976c-430c-9e6f-da2b9cbc6d54"
# META         },
# META         {
# META           "id": "76b27e57-ae84-43cf-8a58-0695a20a4165"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold Layer – Daily Sales
# 
# Aggregates daily sales from the Silver Orders table and creates the Gold-level `gold_daily_sales` table.
# 
# ## Key Steps
# 
# - Read order data from `SilverLayer.silver_orders`.
# - Group sales by `transaction_date`.
# - Calculate total daily sales using `SUM(total_amount)`.
# - Create or replace the `gold_daily_sales` table.
# 
# ## Data Flow
# 
# `Silver Orders → Group by Date → SUM Sales → Gold Daily Sales`

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE gold_daily_sales AS
SELECT 
    transaction_date,
    SUM(total_amount) AS daily_total_sales
FROM 
    Silver_Layer.dbo.silver_orders
GROUP BY 
    transaction_date
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the data in the Gold table
spark.sql("SELECT * FROM gold_daily_sales LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE gold_category_sales AS
SELECT 
    p.category AS product_category,
    SUM(o.total_amount) AS category_total_sales
FROM 
    Silver_Layer.dbo.silver_orders o
JOIN 
    Silver_Layer.dbo.silver_products p ON o.product_id = p.product_id
GROUP BY 
    p.category
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT *
FROM gold_category_sales
ORDER BY category_total_sales DESC
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Layer – Business Aggregations
# 
# Transforms Silver-layer data into business-ready aggregated tables for reporting and analytics.
# 
# ## Key Steps
# 
# * Aggregate daily sales by `transaction_date`.
# * Aggregate total sales by product category.
# * Join Silver Orders and Silver Products where required.
# * Create optimized Gold tables for Power BI consumption.
# 
# ## Gold Tables
# 
# * `gold_daily_sales` – Daily total sales by transaction date.
# * `gold_category_sales` – Total sales by product category.
# 
# ## Data Flow
# 
# `Silver Orders + Silver Products → Business Aggregations → Gold Layer → Power BI`

