# Reference Data Scan

_Platform: Local | Paths: examples/demo-workspace | Scanned: 2026-10-09 05:48 UTC_

## At a glance

| Measure | Count |
|---|---:|
| Reference data sets (estimated) | 11 |
| CSV / Excel files | 4 |
| Lookup-table candidates | 0 |
| Hardcoded lists & mappings | 10 |
| Hardcoded values in code | 76 |
| Sets copied in 2+ places | 1 |
| Copies that disagree | 1 |

## Key findings

- **4 of 4 code assets** (notebooks, files, queries and views; 100%) contain hardcoded reference data, 76 values in total.
- **1 reference set** is copied into more than one place. The most widespread (`STATE_MAP`) appears in **4 places**.
- **1 copied set** no longer agrees with its other copies: the same code maps to a different value depending on which notebook or query you run. This is reference data drift. For example, in `STATE_MAP`: ACT: ACT vs Australian Capital Territory.
- **4 CSV/Excel files** found in the platform, including 1 spreadsheet that is typically edited by hand. 1 file is read directly by code, so a manual edit flows straight into pipelines. 3 files are not referenced by any scanned code.
- **1 dbt seed file**: reference data kept as CSV in a code repository, changed through pull requests rather than by the people who own it.
- The largest hardcoded set has **12 entries** (Python list, `COST_CENTRES`).

## Duplicated reference data and drift

The same list or mapping hardcoded in several places. Each copy has to be updated by hand when the reference data changes, and copies that disagree mean reports and pipelines quietly produce different answers.

| Group | Name | Copies | Items | Status | Locations | Where copies disagree |
|---|---|---|---|---|---|---|
| G001 | STATE_MAP | 4 | 8 | Copies disagree | /Repos/analytics/dbt_project/models/stg_customers.sql:14, /Shared/finance/finance_report.py:5, /Shared/finance/region_rollup.sql:12, /Users/jane.doe@contoso.com/sales_transform.py:10 | ACT: ACT vs Australian Capital Territory, WA: West Australia vs Western Australia |

## Top candidates for a managed reference data service

Ranked by how likely each item is to be reference data and how much risk it carries. Scores: 60+ High, 35-59 Medium.

| Score | Priority | Type | Where | What | Name | Why |
|---|---|---|---|---|---|---|
| 95 | High | File | /Repos/analytics/dbt_project/seeds/country_codes.csv | csv |  | dbt seed (reference data by definition); reference-data style name; small file |
| 90 | High | File | /Shared/finance/gl_account_mapping.xlsx | xlsx |  | reference-data style name; read by 1 code location(s); small file; spreadsheet (likely maintained by hand) |
| 84 | High | Code | /Repos/analytics/dbt_project/models/stg_customers.sql:14 | SQL CASE mapping | state_name | hardcoded mapping; reference-data style name; values look like codes; repeated in 4 places; copies disagree |
| 84 | High | Code | /Shared/finance/finance_report.py:5 | Python dict (code -> label) | STATE_MAP | hardcoded mapping; reference-data style name; values look like codes; repeated in 4 places; copies disagree |
| 84 | High | Code | /Users/jane.doe@contoso.com/sales_transform.py:10 | Python dict (code -> label) | STATE_MAP | hardcoded mapping; reference-data style name; values look like codes; repeated in 4 places; copies disagree |
| 80 | High | File | /FileStore/tables/cost_centres.csv | csv |  | reference-data style name; manual upload location; small file |
| 80 | High | File | /FileStore/tables/product_categories.csv | csv |  | reference-data style name; manual upload location; small file |
| 73 | High | Code | /Shared/finance/region_rollup.sql:12 | SQL CASE mapping | sales_region | hardcoded mapping; reference-data style name; values look like codes; repeated in 4 places |
| 58 | Medium | Code | /Shared/finance/finance_report.py:30 | DataFrame built from literals | account_groups | hardcoded mapping; reference-data style name; values look like codes |
| 57 | Medium | Code | /Repos/analytics/dbt_project/models/stg_customers.sql:11 | SQL CASE mapping | segment | hardcoded mapping; reference-data style name; values look like codes |
| 57 | Medium | Code | /Shared/finance/region_rollup.sql:23 | SQL INSERT VALUES inside spark.sql() | main.reference.currency | hardcoded mapping; reference-data style name; values look like codes |
| 57 | Medium | Code | /Users/jane.doe@contoso.com/sales_transform.py:31 | PySpark when() chain | channel | hardcoded mapping; reference-data style name; values look like codes |
| 46 | Medium | Code | /Shared/finance/finance_report.py:16 | Python list | COST_CENTRES | hardcoded list; reference-data style name; values look like codes |
| 46 | Medium | Code | /Users/jane.doe@contoso.com/sales_transform.py:49 | SQL IN (...) list | product_code | hardcoded list; reference-data style name; values look like codes |

## CSV and Excel files

| Score | Path | Size | Modified | Owner | Read by code | Loaded into |
|---|---|---|---|---|---|---|
| 95 | /Repos/analytics/dbt_project/seeds/country_codes.csv | 89 B | 2026-10-09 |  | - | - |
| 90 | /Shared/finance/gl_account_mapping.xlsx | 5.2 KB | 2026-10-09 |  | 1 | - |
| 80 | /FileStore/tables/cost_centres.csv | 79 B | 2026-10-09 |  | - | - |
| 80 | /FileStore/tables/product_categories.csv | 50 B | 2026-10-09 |  | - | - |

## Hardcoded reference data by pattern

| Pattern | Count | Values |
|---|---|---|
| SQL CASE mapping | 3 | 20 |
| Python dict (code -> label) | 2 | 16 |
| DataFrame built from literals | 1 | 6 |
| SQL INSERT VALUES inside spark.sql() | 1 | 5 |
| PySpark when() chain | 1 | 5 |
| Python list | 1 | 12 |
| SQL IN (...) list | 1 | 12 |

## Hardcoded lists and mappings in code

| Score | Location | Pattern | Name | Items | Group | Sample |
|---|---|---|---|---|---|---|
| 84 | /Repos/analytics/dbt_project/models/stg_customers.sql:14 | SQL CASE mapping | state_name | 8 | G001 | NSW -> New South Wales, QLD -> Queensland, VIC -> Victoria, SA -> South Australia, WA -> Western Australia, ... (+3) |
| 84 | /Shared/finance/finance_report.py:5 | Python dict (code -> label) | STATE_MAP | 8 | G001 | NSW -> New South Wales, QLD -> Queensland, VIC -> Victoria, SA -> South Australia, WA -> West Australia, ... (+3) |
| 84 | /Users/jane.doe@contoso.com/sales_transform.py:10 | Python dict (code -> label) | STATE_MAP | 8 | G001 | NSW -> New South Wales, QLD -> Queensland, VIC -> Victoria, SA -> South Australia, WA -> Western Australia, ... (+3) |
| 73 | /Shared/finance/region_rollup.sql:12 | SQL CASE mapping | sales_region | 7 | G001 | NSW -> East, VIC -> East, QLD -> North, NT -> North, WA -> West, ... (+2) |
| 58 | /Shared/finance/finance_report.py:30 | DataFrame built from literals | account_groups | 6 |  | 4000 -> Revenue, 4100 -> Revenue, 5000 -> Cost of Sales, 6000 -> Operating Expenses, 6100 -> Operating Expenses, ... (+1) |
| 57 | /Repos/analytics/dbt_project/models/stg_customers.sql:11 | SQL CASE mapping | segment | 5 |  | E -> Enterprise, M -> Mid-market, S -> Small business, C -> Consumer, G -> Government |
| 57 | /Shared/finance/region_rollup.sql:23 | SQL INSERT VALUES inside spark.sql() | main.reference.currency | 5 |  | AUD -> Australian Dollar, NZD -> New Zealand Dollar, USD -> US Dollar, EUR -> Euro, GBP -> Pound Sterling |
| 57 | /Users/jane.doe@contoso.com/sales_transform.py:31 | PySpark when() chain | channel | 5 |  | 01 -> Retail, 02 -> Wholesale, 03 -> Online, 04 -> Marketplace, 05 -> Phone |
| 46 | /Shared/finance/finance_report.py:16 | Python list | COST_CENTRES | 12 |  | CC100, CC110, CC120, CC130, CC200, ... (+7) |
| 46 | /Users/jane.doe@contoso.com/sales_transform.py:49 | SQL IN (...) list | product_code | 12 |  | P100, P101, P102, P103, P104, ... (+7) |

## Hotspots by location

| Location | Files | Tables | Code | Total |
|---|---|---|---|---|
| /Shared/finance | 1 | 0 | 5 | 6 |
| /Users/jane.doe@contoso.com | 0 | 0 | 3 | 3 |
| /Repos/analytics/dbt_project/models | 0 | 0 | 2 | 2 |
| /FileStore/tables | 2 | 0 | 0 | 2 |
| /Repos/analytics/dbt_project/seeds | 1 | 0 | 0 | 1 |

## What this means

- **Changes need a developer.** Business users cannot update a hardcoded mapping; every change is a code change and a deployment.
- **No single source of truth.** Copies drift apart, so the same code means different things in different reports.
- **No audit trail.** Spreadsheets and code literals have no approval workflow, effective dates or history of who changed what.
- **Hidden risk.** Hand-edited files that feed pipelines directly can break them, or silently change results.

## Next steps

- Pick the high-priority sets, especially those that are duplicated or disagree, and agree one owner for each.
- Move them into a managed reference data service with approval workflows, versioning and an API, and replace the hardcoded copies with a lookup.
- [TitanRDM](https://titanrdm.com) is a reference data management service built for exactly this: business users maintain the data, and your pipelines read it from Databricks, Snowflake or Fabric.

## How this was produced

- Scanned 0 tables, 4 code assets and 4 CSV/Excel files.
- Thresholds: lists with more than 10 items; mappings with 5+ entries; CASE statements with 5+ branches; inline tables with 3+ rows.
- 2 findings were set aside as probable column lists or configuration (listed in code_findings.csv with a noise_reason).
- Results only include what the account running the scan is allowed to see.
- The scanner is read-only, reads metadata and code only (file contents only if you opted in), and makes no network calls outside your platform.

---
Generated by [refdata-scanner](https://github.com/TitanRDM/refdata-scanner), an open-source tool from [TitanRDM](https://titanrdm.com).
