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
-- MAGIC | 3 | Tables filled with literal rows: `INSERT ... VALUES`, `MERGE ... USING (VALUES ...)`, `CREATE TABLE AS VALUES` (query history) |
-- MAGIC | 4 | Views with hardcoded `CASE` mappings or long `IN (...)` lists |
-- MAGIC | 5 | Small tables that look like lookup / mapping tables |
-- MAGIC | 6 | A one-row summary |
-- MAGIC
-- MAGIC **How to run:** edit the three settings in the first SQL cell (catalog, optional schemas, days of history),
-- MAGIC then *Run all*. The settings are SQL session variables, so the notebook works the same on a SQL warehouse,
-- MAGIC serverless compute or a cluster, with no widgets to create.
-- MAGIC
-- MAGIC **Requirements:** a SQL warehouse (serverless or pro), serverless compute, or a cluster on Databricks Runtime 14.1+.
-- MAGIC Cells 1 to 3 need read access to `system.access` and `system.query`; ask a metastore admin if they error.
-- MAGIC The other cells only need access to the catalog you are scanning.
-- MAGIC
-- MAGIC For the full picture (files in volumes and workspace folders, hardcoded lists and mappings in every notebook,
-- MAGIC duplicated and conflicting copies), run **02_full_scan** next.
-- MAGIC
-- MAGIC Open source: https://github.com/TitanRDM/refdata-scanner

-- COMMAND ----------

-- Settings: edit these, then Run all.
-- Catalog to scan (default: the current catalog).
DECLARE OR REPLACE VARIABLE rds_catalog STRING DEFAULT current_catalog();
-- Schemas to scan, e.g. array('finance', 'sales'). Empty = every schema in the catalog.
DECLARE OR REPLACE VARIABLE rds_schemas ARRAY<STRING> DEFAULT array();
-- Days of query history to search.
DECLARE OR REPLACE VARIABLE rds_lookback_days INT DEFAULT 90;

SELECT rds_catalog AS catalog, rds_schemas AS schemas, rds_lookback_days AS lookback_days;

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
  AND target_table_catalog = rds_catalog
  AND (size(rds_schemas) = 0 OR array_contains(rds_schemas, target_table_schema))
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
WHERE start_time >= date_sub(current_date(), rds_lookback_days)
  AND lower(statement_text) RLIKE '\\.(csv|tsv|xlsx|xls|xlsm|xlsb)'
GROUP BY 1, 2
ORDER BY runs DESC
LIMIT 500

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Tables filled with literal rows
-- MAGIC `INSERT INTO t VALUES (...)`, `MERGE INTO t USING (VALUES ...)` and `CREATE TABLE t AS VALUES ...` statements
-- MAGIC run in the last *N* days, grouped by target table. A table whose only source is rows typed into SQL is
-- MAGIC reference data maintained by hand, often one `INSERT` at a time in the SQL editor. Log, audit, control and test
-- MAGIC tables are left out. Two-part names (`schema.table`) are kept because their catalog can't be told from the text.

-- COMMAND ----------

WITH loads AS (
  SELECT
    lower(replace(regexp_extract(statement_text, '(?is)^\\s*(?:insert\\s+(?:into|overwrite)(?:\\s+table)?|merge\\s+into|create\\s+(?:or\\s+replace\\s+)?table(?:\\s+if\\s+not\\s+exists)?)\\s+([\\w.`]+)', 1), '`', '')) AS target_table,
    statement_text,
    executed_by,
    start_time
  FROM system.query.history
  WHERE start_time >= date_sub(current_date(), rds_lookback_days)
    AND statement_text RLIKE '(?is)^\\s*(insert|merge|create)\\b.*\\bvalues\\s*\\('
)
SELECT
  target_table,
  count(DISTINCT statement_text)                                   AS distinct_statements,
  count(*)                                                         AS runs,
  count(DISTINCT executed_by)                                      AS users,
  max(start_time)                                                  AS last_run,
  left(regexp_replace(max_by(statement_text, start_time), '\\s+', ' '), 200) AS latest_statement
FROM loads
WHERE target_table <> ''
  AND (size(split(target_table, '\\.')) < 3 OR split(target_table, '\\.')[0] = lower(rds_catalog))
  AND (size(rds_schemas) = 0 OR size(split(target_table, '\\.')) < 2
       OR array_contains(transform(rds_schemas, s -> lower(s)), element_at(split(target_table, '\\.'), -2)))
  AND NOT target_table RLIKE '(^|[._])(log|logs|audit|watermark|checkpoint|control|tmp|temp|test)([._]|$)'
GROUP BY target_table
ORDER BY distinct_statements DESC, runs DESC
LIMIT 500

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Views with hardcoded mappings
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
  WHERE table_catalog = rds_catalog
    AND (size(rds_schemas) = 0 OR array_contains(rds_schemas, table_schema))
)
WHERE when_branches_total >= 5 OR literal_in_lists > 0 OR inline_values > 0
ORDER BY when_branches_total DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Lookup-table candidates
-- MAGIC Narrow tables with reference-data style names (`lkp_`, `lu_`, `lookup`, `ref_`, `_map`, `xref`, `crosswalk`, `seed`).
-- MAGIC These are often loaded once from a spreadsheet and then edited ad hoc.

-- COMMAND ----------

WITH col_counts AS (
  SELECT table_catalog, table_schema, table_name, count(*) AS column_count
  FROM system.information_schema.columns
  WHERE table_catalog = rds_catalog
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
WHERE t.table_catalog = rds_catalog
  AND t.table_schema <> 'information_schema'
  AND t.table_type NOT IN ('VIEW', 'MATERIALIZED_VIEW', 'METRIC_VIEW')
  AND (size(rds_schemas) = 0 OR array_contains(rds_schemas, t.table_schema))
  AND c.column_count <= 12
  AND lower(t.table_name) RLIKE '(^|_)(lkp|lu|lookup|ref|reference|refdata|map|mapping|mappings|xref|crosswalk|seed)(_|$)'
ORDER BY c.column_count, table_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6. Summary

-- COMMAND ----------

WITH loads AS (
  SELECT
    lower(replace(regexp_extract(statement_text, '(?is)^\\s*(?:insert\\s+(?:into|overwrite)(?:\\s+table)?|merge\\s+into|create\\s+(?:or\\s+replace\\s+)?table(?:\\s+if\\s+not\\s+exists)?)\\s+([\\w.`]+)', 1), '`', '')) AS target_table,
    statement_text,
    executed_by,
    start_time
  FROM system.query.history
  WHERE start_time >= date_sub(current_date(), rds_lookback_days)
    AND statement_text RLIKE '(?is)^\\s*(insert|merge|create)\\b.*\\bvalues\\s*\\('
)
SELECT
  (SELECT count(DISTINCT source_path)
     FROM system.access.table_lineage
    WHERE source_type = 'PATH'
      AND lower(source_path) RLIKE '\\.(csv|tsv|xlsx|xls|xlsm|xlsb)(\\.gz)?$'
      AND target_table_catalog = rds_catalog
      AND (size(rds_schemas) = 0 OR array_contains(rds_schemas, target_table_schema))
  ) AS csv_excel_files_loaded_into_tables,
  (SELECT count(DISTINCT target_table_full_name)
     FROM system.access.table_lineage
    WHERE source_type = 'PATH'
      AND lower(source_path) RLIKE '\\.(csv|tsv|xlsx|xls|xlsm|xlsb)(\\.gz)?$'
      AND target_table_catalog = rds_catalog
      AND (size(rds_schemas) = 0 OR array_contains(rds_schemas, target_table_schema))
  ) AS tables_fed_by_spreadsheets,
  (SELECT count(DISTINCT target_table) FROM loads
    WHERE target_table <> ''
      AND (size(split(target_table, '\\.')) < 3 OR split(target_table, '\\.')[0] = lower(rds_catalog))
      AND (size(rds_schemas) = 0 OR size(split(target_table, '\\.')) < 2
           OR array_contains(transform(rds_schemas, s -> lower(s)), element_at(split(target_table, '\\.'), -2)))
      AND NOT target_table RLIKE '(^|[._])(log|logs|audit|watermark|checkpoint|control|tmp|temp|test)([._]|$)'
  ) AS tables_filled_from_values,
  (SELECT count(*)
     FROM system.information_schema.views
    WHERE table_catalog = rds_catalog
      AND (size(rds_schemas) = 0 OR array_contains(rds_schemas, table_schema))
      AND size(split(upper(view_definition), '\\bWHEN\\b')) - 1 >= 5
  ) AS views_with_case_mappings,
  (SELECT count(*)
     FROM system.information_schema.tables t
    WHERE t.table_catalog = rds_catalog
      AND t.table_schema <> 'information_schema'
      AND t.table_type NOT IN ('VIEW', 'MATERIALIZED_VIEW', 'METRIC_VIEW')
      AND (size(rds_schemas) = 0 OR array_contains(rds_schemas, t.table_schema))
      AND lower(t.table_name) RLIKE '(^|_)(lkp|lu|lookup|ref|reference|refdata|map|mapping|mappings|xref|crosswalk|seed)(_|$)'
  ) AS lookup_table_candidates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What next?
-- MAGIC
-- MAGIC * Run **02_full_scan** to find CSV/Excel files in volumes and workspace folders, and every hardcoded list,
-- MAGIC   dictionary, `CASE`, `when()` chain and `INSERT ... VALUES` in your notebooks, including copies that disagree
-- MAGIC   with each other.
-- MAGIC * Reference data that lives in spreadsheets and code has no owner, no approval workflow and no history.
-- MAGIC   [TitanRDM](https://titanrdm.com) gives business users a governed place to maintain it, published straight
-- MAGIC   into Databricks.
