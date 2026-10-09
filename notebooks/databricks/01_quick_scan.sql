-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Reference data quick scan (5 minutes, SQL only)
-- MAGIC
-- MAGIC Finds reference data hiding in your lakehouse using Unity Catalog **system tables** and **information schema**.
-- MAGIC Nothing is installed, nothing is written, and no data leaves your workspace: every cell is a read-only `SELECT`.
-- MAGIC
-- MAGIC | Cell | Finds |
-- MAGIC |---|---|
-- MAGIC | 1 | Tables that were loaded from CSV or Excel files (lineage) |
-- MAGIC | 2 | SQL that read CSV or Excel files in the last *N* days (query history) |
-- MAGIC | 3 | Views with hardcoded `CASE` mappings or long `IN (...)` lists |
-- MAGIC | 4 | Small tables that look like lookup / mapping tables |
-- MAGIC | 5 | A one-row summary |
-- MAGIC
-- MAGIC **How to run:** set the widgets at the top (catalog, optional comma-separated schemas, days of history), then *Run all*.
-- MAGIC
-- MAGIC **Requirements:** a SQL warehouse, serverless compute or a cluster on Databricks Runtime 15.2+ (for `:parameter` widgets).
-- MAGIC Cells 1 and 2 need read access to `system.access` and `system.query`; ask a metastore admin if they error.
-- MAGIC The other cells only need access to the catalog you are scanning.
-- MAGIC
-- MAGIC For the full picture (files in volumes and workspace folders, hardcoded lists and mappings in every notebook,
-- MAGIC duplicated and conflicting copies), run **02_full_scan** next.
-- MAGIC
-- MAGIC Open source: https://github.com/TitanRDM/refdata-scanner

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'main';
CREATE WIDGET TEXT schemas DEFAULT '';
CREATE WIDGET TEXT lookback_days DEFAULT '90';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Tables loaded from CSV or Excel files
-- MAGIC Every row is a table whose contents came from a spreadsheet or CSV. Somebody maintains that file by hand.

-- COMMAND ----------

SELECT
  source_path                         AS file_path,
  target_table_full_name              AS loaded_into_table,
  count(DISTINCT created_by)          AS loaded_by_users,
  max(event_time)                     AS last_loaded,
  count(*)                            AS load_events
FROM system.access.table_lineage
WHERE source_type = 'PATH'
  AND lower(source_path) RLIKE '\\.(csv|tsv|xlsx|xls|xlsm|xlsb)(\\.gz)?$'
  AND target_table_catalog = :catalog
  AND (trim(:schemas) = '' OR array_contains(transform(split(:schemas, ','), s -> trim(s)), target_table_schema))
GROUP BY source_path, target_table_full_name
ORDER BY last_loaded DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. SQL that read CSV or Excel files
-- MAGIC Queries run on SQL warehouses and serverless compute that mention a `.csv` / `.xlsx` file, most frequent first.

-- COMMAND ----------

SELECT
  left(regexp_replace(statement_text, '\\s+', ' '), 300) AS statement,
  regexp_extract(statement_text, '([^\'"`\\s]+\\.(csv|tsv|xlsx|xls|xlsm|xlsb))', 1) AS first_file_mentioned,
  count(DISTINCT executed_by)                             AS users,
  count(*)                                                AS runs,
  max(start_time)                                         AS last_run
FROM system.query.history
WHERE start_time >= date_sub(current_date(), CAST(:lookback_days AS INT))
  AND lower(statement_text) RLIKE '\\.(csv|tsv|xlsx|xls|xlsm|xlsb)'
GROUP BY 1, 2
ORDER BY runs DESC
LIMIT 500

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Views with hardcoded mappings
-- MAGIC `CASE` expressions with many `WHEN` branches are usually code-to-label mappings (or groupings) written into SQL.
-- MAGIC Each one has to be edited by a developer when the business changes a code.

-- COMMAND ----------

SELECT * FROM (
  SELECT
    concat_ws('.', table_catalog, table_schema, table_name)                   AS view_name,
    size(split(upper(view_definition), '\\bWHEN\\b')) - 1                    AS when_branches_total,
    size(split(upper(view_definition), '\\bIN\\s*\\(\\s*\'')) - 1           AS literal_in_lists,
    size(split(upper(view_definition), '\\bVALUES\\s*\\(')) - 1              AS inline_values
  FROM system.information_schema.views
  WHERE table_catalog = :catalog
    AND (trim(:schemas) = '' OR array_contains(transform(split(:schemas, ','), s -> trim(s)), table_schema))
)
WHERE when_branches_total >= 5 OR literal_in_lists > 0 OR inline_values > 0
ORDER BY when_branches_total DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Lookup-table candidates
-- MAGIC Narrow tables with reference-data style names (`dim_`, `lkp_`, `_map`, `_codes`, `_type`, `status`, ...).
-- MAGIC These are often loaded once from a spreadsheet and then edited ad hoc.

-- COMMAND ----------

WITH col_counts AS (
  SELECT table_catalog, table_schema, table_name, count(*) AS column_count
  FROM system.information_schema.columns
  WHERE table_catalog = :catalog
  GROUP BY ALL
)
SELECT
  concat_ws('.', t.table_catalog, t.table_schema, t.table_name) AS table_name,
  c.column_count,
  t.data_source_format,
  t.table_owner,
  t.created,
  t.last_altered,
  t.last_altered_by,
  t.comment
FROM system.information_schema.tables t
JOIN col_counts c USING (table_catalog, table_schema, table_name)
WHERE t.table_catalog = :catalog
  AND t.table_schema <> 'information_schema'
  AND t.table_type NOT IN ('VIEW', 'MATERIALIZED_VIEW', 'METRIC_VIEW')
  AND (trim(:schemas) = '' OR array_contains(transform(split(:schemas, ','), s -> trim(s)), t.table_schema))
  AND c.column_count <= 12
  AND lower(t.table_name) RLIKE '(^|_)(lkp|lu|lookup|ref|reference|refdata|map|mapping|mappings|xref|crosswalk|code|codes|dim|type|types|status|statuses|category|categories|region|regions|country|countries|currency|currencies|seed|hierarchy|segment|segments|channel|channels|uom|calendar|holidays?)(_|$)'
ORDER BY c.column_count, table_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Summary

-- COMMAND ----------

SELECT
  (SELECT count(DISTINCT source_path)
     FROM system.access.table_lineage
    WHERE source_type = 'PATH'
      AND lower(source_path) RLIKE '\\.(csv|tsv|xlsx|xls|xlsm|xlsb)(\\.gz)?$'
      AND target_table_catalog = :catalog
      AND (trim(:schemas) = '' OR array_contains(transform(split(:schemas, ','), s -> trim(s)), target_table_schema))
  ) AS csv_excel_files_loaded_into_tables,
  (SELECT count(DISTINCT target_table_full_name)
     FROM system.access.table_lineage
    WHERE source_type = 'PATH'
      AND lower(source_path) RLIKE '\\.(csv|tsv|xlsx|xls|xlsm|xlsb)(\\.gz)?$'
      AND target_table_catalog = :catalog
      AND (trim(:schemas) = '' OR array_contains(transform(split(:schemas, ','), s -> trim(s)), target_table_schema))
  ) AS tables_fed_by_spreadsheets,
  (SELECT count(*)
     FROM system.information_schema.views
    WHERE table_catalog = :catalog
      AND (trim(:schemas) = '' OR array_contains(transform(split(:schemas, ','), s -> trim(s)), table_schema))
      AND size(split(upper(view_definition), '\\bWHEN\\b')) - 1 >= 5
  ) AS views_with_case_mappings,
  (SELECT count(*)
     FROM system.information_schema.tables t
    WHERE t.table_catalog = :catalog
      AND t.table_schema <> 'information_schema'
      AND t.table_type NOT IN ('VIEW', 'MATERIALIZED_VIEW', 'METRIC_VIEW')
      AND (trim(:schemas) = '' OR array_contains(transform(split(:schemas, ','), s -> trim(s)), t.table_schema))
      AND lower(t.table_name) RLIKE '(^|_)(lkp|lu|lookup|ref|reference|refdata|map|mapping|mappings|xref|crosswalk|code|codes|dim|type|types|status|statuses|category|categories|region|regions|country|countries|currency|currencies|seed|hierarchy|segment|segments|channel|channels|uom|calendar|holidays?)(_|$)'
  ) AS lookup_table_candidates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What next?
-- MAGIC
-- MAGIC * Run **02_full_scan** to find CSV/Excel files in volumes and workspace folders, and every hardcoded list,
-- MAGIC   dictionary, `CASE` and `when()` chain in your notebooks, including copies that disagree with each other.
-- MAGIC * Reference data that lives in spreadsheets and code has no owner, no approval workflow and no history.
-- MAGIC   [TitanRDM](https://titanrdm.com) gives business users a governed place to maintain it, published straight
-- MAGIC   into Databricks.
