# Databricks notebook source
import pandas as pd

# Copied from the sales notebook. TODO: keep in sync
STATE_MAP = {
    "NSW": "New South Wales",
    "QLD": "Queensland",
    "VIC": "Victoria",
    "SA": "South Australia",
    "WA": "West Australia",
    "TAS": "Tasmania",
    "NT": "Northern Territory",
    "ACT": "Australian Capital Territory",
}

COST_CENTRES = ["CC100", "CC110", "CC120", "CC130", "CC200", "CC210", "CC220", "CC300", "CC310", "CC320", "CC400", "CC410"]

spark_conf = {"spark.sql.shuffle.partitions": "64", "spark.sql.adaptive.enabled": "true",
              "spark.databricks.delta.optimizeWrite.enabled": "true", "spark.sql.ansi.enabled": "false",
              "spark.sql.session.timeZone": "Australia/Brisbane"}

# COMMAND ----------

gl = spark.table("main.finance.gl_balances").filter(F.col("cost_centre").isin(COST_CENTRES))
mapping = pd.read_excel("/Workspace/Shared/finance/gl_account_mapping.xlsx")

# COMMAND ----------

account_groups = spark.createDataFrame(
    [
        ("4000", "Revenue"),
        ("4100", "Revenue"),
        ("5000", "Cost of Sales"),
        ("6000", "Operating Expenses"),
        ("6100", "Operating Expenses"),
        ("7000", "Other Income"),
    ],
    ["account", "account_group"],
)
