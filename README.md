# refdata-scanner

**Find the reference data hiding in your data platform.**

Country codes in a Python dictionary. A cost-centre list pasted into three notebooks. A `CASE` statement mapping
product codes to categories. A spreadsheet someone uploads to a volume every month. That is reference data, and on most
data platforms it is scattered, copied and quietly drifting apart.

refdata-scanner is a free, open-source, **read-only** scanner that finds it and writes an assessment report:

- **CSV and Excel files** in Unity Catalog volumes, workspace folders (including Git folders), DBFS `/FileStore`,
  dbt seed folders, and files known only from lineage
- **Lookup-table candidates**: small, narrow tables with reference-data style names, or tables loaded from spreadsheets
- **Hardcoded reference data in code**: Python dicts, lists, sets and tuples; `spark.createDataFrame([...])` and
  `pd.DataFrame(...)` literals; `.isin([...])` filters; `.replace({...})` mappings; `F.when().when()` chains;
  SQL `CASE` mappings, long `IN (...)` lists and `VALUES` tables, including SQL inside `spark.sql("...")`, in
  notebooks, code files, dbt models, saved SQL queries and view definitions
- **Duplicates and drift**: the same list or mapping copied into several places, and copies that *disagree*
  (for example `WA: West Australia vs Western Australia`)

See a [sample report](examples/sample-report/report.md) produced from the [demo workspace](examples/demo-workspace).

Supported today: **Databricks** and **local folders** (Git checkouts, dbt projects, exported notebooks).
Snowflake and Microsoft Fabric are on the [roadmap](#roadmap).

---

## Three ways to run it

### 1. Five-minute SQL check (Databricks, nothing to install)

Import [`notebooks/databricks/01_quick_scan.sql`](notebooks/databricks/01_quick_scan.sql) into your workspace (it is
also in the zip), edit the catalog in the first SQL cell (it defaults to your current catalog) and *Run all*. It uses Unity Catalog system tables and the information schema to show:

- tables that were loaded from CSV or Excel files (lineage)
- SQL that read CSV or Excel files in the last 90 days (query history)
- views with hardcoded `CASE` mappings or literal `IN` lists
- lookup-table candidates

It runs on a SQL warehouse (including serverless), serverless compute, or a cluster on Databricks Runtime 14.1+.
Its settings are SQL session variables rather than widgets, because notebooks attached to a SQL warehouse can't
create widgets from code. The lineage and query history cells need
read access to `system.access` and `system.query`, which a metastore admin may have to grant.

### 2. Full scan in a Databricks notebook (recommended)

1. Download `refdata-scanner-databricks.zip` from the
   [latest release](https://github.com/TitanRDM/refdata-scanner/releases/latest).
2. In Databricks, open the folder you want it in, choose **Import > File** and drop in the zip. You get a
   `refdata-scanner` folder with both notebooks and a `lib` folder holding the scanner and the
   [sqlglot](https://github.com/tobymao/sqlglot) SQL parser it uses (MIT licence, included unmodified).
3. Open `02_full_scan`, attach serverless or a Unity Catalog cluster (Databricks Runtime 15.4 LTS or later
   recommended), and run the first cell to create the widgets.
4. Set the **catalog**, optional **schemas**, and the **workspace folders** to scan, then *Run all*.

The report is shown in the notebook, and the inventories are displayed as sortable tables.

**Where the files go.** Each run writes a timestamped `refdata-scan-YYYYMMDD-HHMM` folder containing the report and
inventories. By default it goes in a `Results` folder next to the notebook, so if you imported the zip into your home
folder, the results stay in your home folder too. Open the folder in the workspace browser to download `report.html`
or any CSV. To put results somewhere else, set the *Output folder* widget to another workspace folder
(for example `/Users/you@company.com/scans`) or a volume. The scanner never scans its own results.

**Running on a schedule.** To track reference data over time, run `02_full_scan` as a Databricks job (for example
monthly) with the `output_path` parameter set to a volume such as `/Volumes/main/governance/refdata_scans`. A volume
keeps the history of scans in one governed place that others can be granted access to, and each run's
`summary.json` gives you counts to compare between runs. Set the job's other parameters (catalog, schemas, workspace
folders) the same way; they match the widget names.

With the zip, the notebook installs nothing and downloads nothing: everything it runs is in the folder you
imported, which you can read before running it.

You can also clone the repository as a **Git folder** instead of importing the zip. The notebook then loads the
scanner from `src/`. The repository does not include sqlglot, so if the cluster doesn't already have it, the notebook
installs it with `pip` from the package index your workspace uses (PyPI or your organisation's mirror). That is the
only thing the scanner ever installs; set the *Install sqlglot* widget to `no` to skip it, in which case SQL is
scanned with pattern matching, which is less precise.

No credentials to configure: inside a notebook the scanner uses your own identity, so it only sees what you can see.

### Trying it on a test workspace

A quiet workspace gives a quiet report. [`tools/databricks/create_demo_artifacts.py`](tools/databricks/create_demo_artifacts.py)
fills a workspace with realistic reference data for the scanner to find: a schema with a volume of CSV and Excel
files, lookup tables (some loaded from those files, so lineage picks them up), views with `CASE` mappings, notebooks
with hardcoded dicts, lists and `when()` chains (including copies that disagree), a saved query and a job.
Import it into your workspace, run it with *action* = `create`, then point `02_full_scan` at the schema and folder it
prints. Run it again with *action* = `cleanup` to delete everything it created.

Unlike the scanner, this notebook **writes** to the workspace, so it is not part of the zip bundle. Use it only
where test objects are welcome.

### 3. Command line

```bash
pip install "refdata-scanner[all] @ git+https://github.com/TitanRDM/refdata-scanner"

# Databricks: uses the standard Databricks CLI / SDK authentication (~/.databrickscfg, env vars, OAuth)
refdata-scanner databricks --profile prod --catalog main --schemas finance,sales \
    --workspace-path /Shared --workspace-path /Users \
    --warehouse-id 1234567890abcdef

# Local folders: a Git checkout of your notebooks, a dbt project, an exported workspace
refdata-scanner local ./analytics-repo ./dbt_project --dialect snowflake
```

`--warehouse-id` is optional; it enables lineage, query history and table sizes through a SQL warehouse.
Run `refdata-scanner databricks --help` for every option.

---

## What you get

Each run writes a folder:

| File | Contents |
|---|---|
| `report.html` / `report.md` | The assessment: headline numbers, key findings, duplicated and drifting sets, top candidates, hotspots by location and owner, and next steps |
| `code_findings.csv` | Every hardcoded list, mapping and inline table: location, line, pattern, name, item count, sample values, score and reasons |
| `files.csv` | Every CSV/Excel file: location, size, modified date, owner, which code reads it, which tables it was loaded into |
| `tables.csv` | Lookup-table candidates with column count, size and reasons |
| `duplicate_groups.csv` | Sets copied into several places, whether copies are identical, and where they disagree |
| `findings.json` | Everything above in one machine-readable file (input for the AI assessment skill) |
| `summary.json` | Counts only, with no names, paths or values. Safe to share. |

Every candidate gets a **score from 0 to 100** (High 60+, Medium 35-59, Low below 35) with the reasons spelled out:
mappings and inline tables score higher than plain lists; reference-data style names (`_map`, `lookup`, `codes`,
`status`, `region` ...), duplication, disagreeing copies, use in scheduled jobs, spreadsheets and manual upload
locations all add to the score.

Things that look like reference data but usually are not are **set aside, not hidden**: lists of column names
(checked against the catalog's real column names), Spark/configuration dictionaries, column renames and numeric
sequences. They stay in `code_findings.csv` with a `noise_reason`.

## Safe by design

- **Read-only.** The scanner lists and reads; it never creates, changes or deletes anything on your platform.
- **Metadata and code only.** File contents are not read unless you turn on `inspect files`, and even then only the
  header row and a row estimate are kept.
- **Nothing leaves your environment.** No telemetry and no downloads. The only network calls are to your own
  platform's APIs, using your own identity. The single exception is the optional `pip install` of the sqlglot SQL
  parser when running from a Git folder (see above); the zip bundle never needs it. A test
  (`tests/test_no_outbound_calls.py`) fails the build if any other route out is added.
- **Your permissions.** It runs as you, so results only include what your account can see.
- **Shareable output.** `--redact` (or the *Redact* widget) removes literal values, comments and user names from every
  output, and `summary.json` never contains them.
- **Small and readable.** Plain Python. Its only dependencies are `sqlglot` (bundled) for SQL parsing and the
  Databricks SDK that Databricks already provides. Read it before you run it.

## Thresholds

| Option | Default | Meaning |
|---|---|---|
| `--min-list-items` | 10 | Report lists with **more than** this many literal items |
| `--min-mapping-items` | 5 | Report dicts and other code-to-value mappings from this many entries |
| `--min-case-branches` | 5 | Report `CASE` expressions and `when()` chains from this many branches |
| `--min-inline-rows` | 3 | Report inline tables (`VALUES`, `createDataFrame`) from this many rows |
| `--inspect-files` | off | Read CSV/Excel header rows (up to 5 MB per file) |
| `--redact` | off | Hide values and names in all outputs |

## AI-assisted assessment (optional)

The scanner is deterministic: it finds candidates but cannot tell what they mean. The
[`skills/refdata-assessment`](skills/refdata-assessment/SKILL.md) skill takes `findings.json` and has an AI assistant
(for example Claude) classify each finding by business domain, merge copies into named reference data sets, and write
`assessment.md` with a migration plan. Load the skill in your assistant and point it at a scan folder. This step is
optional, and only sends data to the assistant you choose.

## How it works

```
adapters/        one per platform: find files, code and tables (Databricks, local; Snowflake and Fabric next)
scanners/        python_scanner (Python ast), sql_scanner (sqlglot, with a regex fallback)
notebook_parser  splits Databricks source-format and Jupyter (.ipynb) notebooks into Python and SQL cells
analysis         noise filtering, duplicate and drift detection, file usage, scoring
report           CSV/JSON inventories and the Markdown/HTML assessment
```

Python is parsed with the standard library `ast` module and SQL with [sqlglot](https://github.com/tobymao/sqlglot)
(an SQL parser that understands the Databricks, Snowflake and T-SQL dialects), so literals are counted from the
parsed code rather than guessed with regular expressions. Templated SQL (dbt Jinja, `${var}`) is neutralised before
parsing, and anything that still cannot be parsed falls back to pattern matching rather than being skipped.

## Roadmap

- **Snowflake**: stages (`LIST @stage`), `ACCOUNT_USAGE.COPY_HISTORY` and `ACCESS_HISTORY`, Snowflake Notebooks,
  stored procedures, UDFs (user-defined functions) and views.
- **Microsoft Fabric**: Lakehouse `Files` in OneLake, notebooks through the Fabric REST API, Dataflow Gen2 and
  Power BI semantic models (where "Enter data" tables and DAX `SWITCH` statements hold a lot of reference data).

The scanners, analysis and reports are platform-neutral; a new platform only needs an adapter implementing
[`PlatformAdapter`](src/refdata_scanner/adapters/base.py). Contributions are welcome.

## Development

```bash
pip install -e ".[dev]"
pytest
refdata-scanner local examples/demo-workspace --inspect-files --out /tmp/demo
python scripts/build_databricks_bundle.py   # builds dist/refdata-scanner-databricks.zip
```

The `databricks-bundle` workflow builds the bundle on every push to `main` (as a workflow artifact) and attaches
it to the GitHub release whenever a `v*` tag is pushed. There is deliberately no `.dbc` archive: a DBC can only
hold notebooks, so the scanner would have to be downloaded at run time.

## About

Built by [TitanRDM](https://titanrdm.com). Reference data that lives in spreadsheets and code has no owner, no approval
workflow and no history. TitanRDM gives business users a governed place to maintain it, published straight into
Databricks, Snowflake and Fabric.

Licensed under the [MIT License](LICENSE).
