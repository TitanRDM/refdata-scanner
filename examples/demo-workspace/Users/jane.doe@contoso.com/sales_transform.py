# Databricks notebook source
# MAGIC %md
# MAGIC # Sales transform
# MAGIC Cleans raw orders and adds descriptive columns.

# COMMAND ----------

from pyspark.sql import functions as F

STATE_MAP = {
    "NSW": "New South Wales",
    "QLD": "Queensland",
    "VIC": "Victoria",
    "SA": "South Australia",
    "WA": "Western Australia",
    "TAS": "Tasmania",
    "NT": "Northern Territory",
    "ACT": "Australian Capital Territory",
}

select_cols = ["order_id", "order_date", "customer_id", "state", "channel_code", "product_code",
               "qty", "unit_price", "discount", "tax", "total", "currency"]

# COMMAND ----------

orders = spark.table("main.sales.raw_orders").select(*select_cols)
orders = orders.replace(STATE_MAP, subset=["state"])

orders = orders.withColumn(
    "channel",
    F.when(F.col("channel_code") == "01", "Retail")
     .when(F.col("channel_code") == "02", "Wholesale")
     .when(F.col("channel_code") == "03", "Online")
     .when(F.col("channel_code") == "04", "Marketplace")
     .when(F.col("channel_code") == "05", "Phone")
     .otherwise("Unknown"),
)

# COMMAND ----------

categories = spark.read.option("header", True).csv("/Volumes/main/reference/uploads/product_categories.csv")
orders = orders.join(categories, "product_code", "left")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_flagged AS
# MAGIC SELECT *,
# MAGIC   CASE WHEN product_code IN ('P100','P101','P102','P103','P104','P105','P106','P107','P108','P109','P110','P111')
# MAGIC        THEN 'Discontinued' ELSE 'Active' END AS product_status
# MAGIC FROM orders
