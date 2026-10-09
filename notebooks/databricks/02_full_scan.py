# Databricks notebook source
# MAGIC %md
# MAGIC # Reference data full scan
# MAGIC
# MAGIC Finds reference data hiding in this workspace and produces an assessment report:
# MAGIC
# MAGIC * **CSV and Excel files** in Unity Catalog volumes, workspace folders (including Git folders) and DBFS `/FileStore`
# MAGIC * **Lookup-table candidates**: small, narrow tables with reference-data style names, or tables loaded from spreadsheets
# MAGIC * **Hardcoded reference data in code**: Python dicts, lists and sets, `spark.createDataFrame([...])` literals,
# MAGIC   `F.when().when()` chains, SQL `CASE` mappings, long `IN (...)` lists and `VALUES` tables, in notebooks,
# MAGIC   workspace files, saved SQL queries and views
# MAGIC * **Duplicates and drift**: the same list or mapping copied into several places, and copies that disagree
# MAGIC
# MAGIC **Safe to run:**
# MAGIC * **Read-only.** It lists and reads; it never creates, changes or deletes anything in the workspace.
# MAGIC * **Stays inside your workspace.** It only calls this workspace's own APIs and sends nothing anywhere else.
# MAGIC   Everything it needs ships in the `lib` folder next to this notebook. The one exception: if the `sqlglot` SQL
# MAGIC   parser is not in `lib` or on the cluster (for example when running from a Git folder), it is installed with
# MAGIC   `pip` from your workspace's package index. Set widget 10 to `no` to skip that and scan SQL by pattern matching.
# MAGIC * **Runs as you.** Results include only what your account can see.
# MAGIC * **Metadata and code only.** File contents are read only if you opt in, and then only the header row.
# MAGIC
# MAGIC Source code: https://github.com/TitanRDM/refdata-scanner
# MAGIC
# MAGIC **How to run:** attach to serverless or a Unity Catalog cluster (Databricks Runtime 15.4 LTS or later
# MAGIC recommended), run the next cell to create the widgets, fill them in, then *Run all*.
# MAGIC A typical workspace takes a few minutes.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "1. Catalog")
dbutils.widgets.text("schemas", "", "2. Schemas (comma-separated, blank = all)")
dbutils.widgets.text("workspace_paths", "/", "3. Workspace folders (comma-separated)")
dbutils.widgets.text("exclude_paths", "", "4. Folders to skip (comma-separated)")
dbutils.widgets.text("output_path", "", "5. Output folder (e.g. /Volumes/main/default/scans; blank = temp)")
dbutils.widgets.dropdown("include_dbfs", "yes", ["yes", "no"], "6. Scan DBFS /FileStore")
dbutils.widgets.dropdown("use_system_tables", "yes", ["yes", "no"], "7. Use lineage and query history")
dbutils.widgets.dropdown("inspect_files", "no", ["no", "yes"], "8. Read CSV/Excel header rows")
dbutils.widgets.dropdown("redact", "no", ["no", "yes"], "9. Redact values and names")
dbutils.widgets.text("min_list_items", "10", "Lists with more than N items")
dbutils.widgets.text("min_case_branches", "5", "CASE with at least N branches")
dbutils.widgets.dropdown("install_sqlglot", "yes", ["yes", "no"], "10. Install sqlglot if missing (pip)")

# COMMAND ----------

import datetime
import os
import sys

# Load the scanner from the files next to this notebook:
#   imported bundle (ZIP)          -> ./lib   (includes the sqlglot SQL parser)
#   Git folder clone of the repo   -> ../../src  (sqlglot not included)
_SCANNER_ROOT_LEVELS = None  # how many folders up the scanner's own folder is (excluded from the scan)
for _rel, _levels in (("lib", 0), (os.path.join("..", "..", "src"), 2)):
    _candidate = os.path.abspath(os.path.join(os.getcwd(), _rel))
    if os.path.isdir(os.path.join(_candidate, "refdata_scanner")):
        sys.path.insert(0, _candidate)
        _SCANNER_ROOT_LEVELS = _levels
        break
else:
    raise RuntimeError(
        "Scanner code not found next to this notebook. Import refdata-scanner-databricks.zip from "
        "https://github.com/TitanRDM/refdata-scanner/releases and open 02_full_scan from the imported folder."
    )

# The SQL parser. Used from lib/ or the cluster if present; otherwise installed with pip from the package
# index this workspace is configured to use (PyPI or your organisation's mirror). This is the only thing the
# scanner ever installs, and only when widget 10 allows it.
try:
    import sqlglot  # noqa: F401
except ImportError:
    if dbutils.widgets.get("install_sqlglot") == "yes":
        import importlib
        import subprocess

        print("sqlglot not found next to the notebook or on the cluster; installing it with pip ...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "sqlglot>=25"])
        importlib.invalidate_caches()
        import sqlglot  # noqa: F401
    else:
        print("Note: sqlglot is not available, so SQL is scanned with pattern matching (less precise).")

from refdata_scanner import ScanConfig, scan_databricks


def _list(name):
    return [v.strip() for v in dbutils.widgets.get(name).split(",") if v.strip()]


catalog = dbutils.widgets.get("catalog").strip() or None
output_path = dbutils.widgets.get("output_path").strip()
stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M")
out_dir = os.path.join(output_path, f"refdata-scan-{stamp}") if output_path else f"/tmp/refdata-scan-{stamp}"

config = ScanConfig(
    min_list_items=int(dbutils.widgets.get("min_list_items")),
    min_case_branches=int(dbutils.widgets.get("min_case_branches")),
    inspect_file_contents=dbutils.widgets.get("inspect_files") == "yes",
    redact=dbutils.widgets.get("redact") == "yes",
    sample_values=0 if dbutils.widgets.get("redact") == "yes" else 5,
)

# Don't scan the scanner: skip the folder its source code was imported into.
exclude = _list("exclude_paths")
if _SCANNER_ROOT_LEVELS is not None:
    try:
        _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        _root = _nb_path.rsplit("/", 1 + _SCANNER_ROOT_LEVELS)[0]
        if _root:
            exclude.append(_root)
    except Exception:
        pass

paths = scan_databricks(
    catalog=catalog,
    schemas=_list("schemas"),
    workspace_paths=_list("workspace_paths") or ["/"],
    exclude_paths=exclude,
    dbfs_paths=["/FileStore"] if dbutils.widgets.get("include_dbfs") == "yes" else [],
    use_system_tables=dbutils.widgets.get("use_system_tables") == "yes",
    spark=spark,
    config=config,
    out_dir=out_dir,
)
print(f"\nOutputs written to {out_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Report

# COMMAND ----------

with open(paths["report_html"], encoding="utf-8") as fh:
    displayHTML(fh.read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed inventories
# MAGIC Sort and filter these tables, or download them with the table toolbar.

# COMMAND ----------

import pandas as pd

code = pd.read_csv(paths["code_findings"], dtype=str).fillna("")
display(code)

# COMMAND ----------

files = pd.read_csv(paths["files"], dtype=str).fillna("")
display(files)

# COMMAND ----------

tables = pd.read_csv(paths["tables"], dtype=str).fillna("")
display(tables)

# COMMAND ----------

dupes = pd.read_csv(paths["duplicate_groups"], dtype=str).fillna("")
display(dupes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting the files
# MAGIC
# MAGIC * If you set an **output folder** in a volume, download `report.html` and the CSV files from Catalog Explorer.
# MAGIC * Otherwise the files are in a temporary folder on the cluster; run the next cell to copy them to a volume.
# MAGIC * `findings.json` is the full machine-readable result. `summary.json` contains counts only (no names, paths or
# MAGIC   values) and is safe to share.
# MAGIC * Optional: give `findings.json` to an AI assistant with the `refdata-assessment` skill in this repository for a
# MAGIC   written assessment and classification of each finding.

# COMMAND ----------

# Uncomment and set a volume path to keep a copy of the outputs.
# target = "/Volumes/main/default/scans"
# dbutils.fs.cp(f"file:{out_dir}", f"{target}/{os.path.basename(out_dir)}", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC Reference data in spreadsheets and code has no owner, no approval workflow and no history.
# MAGIC [TitanRDM](https://titanrdm.com) gives business users a governed place to maintain it, published straight into
# MAGIC Databricks, Snowflake and Fabric.
