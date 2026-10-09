-- Databricks notebook source
CREATE OR REPLACE VIEW main.finance.v_sales_by_region AS
SELECT
  CASE state
    WHEN 'NSW' THEN 'East'
    WHEN 'VIC' THEN 'East'
    WHEN 'QLD' THEN 'North'
    WHEN 'NT'  THEN 'North'
    WHEN 'WA'  THEN 'West'
    WHEN 'SA'  THEN 'South'
    WHEN 'TAS' THEN 'South'
    ELSE 'Other'
  END AS sales_region,
  sum(total) AS total
FROM main.sales.orders
GROUP BY 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("""
-- MAGIC   INSERT INTO main.reference.currency VALUES
-- MAGIC     ('AUD', 'Australian Dollar'), ('NZD', 'New Zealand Dollar'), ('USD', 'US Dollar'),
-- MAGIC     ('EUR', 'Euro'), ('GBP', 'Pound Sterling')
-- MAGIC """)

-- COMMAND ----------

SELECT * FROM read_files('/Volumes/main/reference/uploads/cost_centres.csv', format => 'csv')
