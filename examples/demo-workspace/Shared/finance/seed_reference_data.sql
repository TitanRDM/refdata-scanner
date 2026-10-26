-- Databricks notebook source
-- Seed reference tables. Run once per environment; add new codes at the bottom.
CREATE TABLE IF NOT EXISTS main.finance.ref_payment_terms (term_code STRING, term_name STRING);

INSERT INTO main.finance.ref_payment_terms VALUES ('COD', 'Cash on delivery');
INSERT INTO main.finance.ref_payment_terms VALUES ('N7', 'Net 7 days');
INSERT INTO main.finance.ref_payment_terms VALUES ('N14', 'Net 14 days');
INSERT INTO main.finance.ref_payment_terms VALUES ('N30', 'Net 30 days');
INSERT INTO main.finance.ref_payment_terms VALUES ('EOM', 'End of month');

-- COMMAND ----------

MERGE INTO main.finance.ref_currency AS t
USING (VALUES ('AUD', 'Australian Dollar'), ('NZD', 'New Zealand Dollar'), ('USD', 'US Dollar'),
              ('EUR', 'Euro'), ('GBP', 'Pound Sterling')) AS s(currency_code, currency_name)
ON t.currency_code = s.currency_code
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

INSERT INTO main.finance.etl_audit_log VALUES ('seed_reference_data', 'completed');
