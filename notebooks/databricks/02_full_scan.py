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
dbutils.widgets.text("output_path", "", "5. Output folder (blank = Results next to this notebook)")
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

# Where this notebook lives. On serverless the working directory is not guaranteed,
# so use the notebook's own workspace path rather than os.getcwd().
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    NOTEBOOK_DIR = "/Workspace" + os.path.dirname(_nb_path) if not _nb_path.startswith("/Workspace") \
        else os.path.dirname(_nb_path)
except Exception:
    NOTEBOOK_DIR = os.getcwd()

# Load the scanner from the files next to this notebook:
#   imported bundle (ZIP)          -> ./lib   (includes the sqlglot SQL parser)
#   Git folder clone of the repo   -> ../../src  (sqlglot not included)
SCANNER_FOLDER = None  # the scanner's own folder, excluded from the scan
for _rel, _root in (("lib", "."), (os.path.join("..", "..", "src"), os.path.join("..", ".."))):
    _candidate = os.path.normpath(os.path.join(NOTEBOOK_DIR, _rel))
    if os.path.isdir(os.path.join(_candidate, "refdata_scanner")):
        sys.path.insert(0, _candidate)
        SCANNER_FOLDER = os.path.normpath(os.path.join(NOTEBOOK_DIR, _root))
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

# Output folder. Default: a Results folder next to this notebook (in your home folder if that's where you
# imported it). For scheduled runs, point this at a volume, e.g. /Volumes/main/governance/refdata_scans.
# Each run writes its own timestamped sub-folder.
output_path = dbutils.widgets.get("output_path").strip().rstrip("/")
if not output_path:
    output_path = os.path.join(NOTEBOOK_DIR, "Results")
elif output_path.startswith(("/Users/", "/Shared/", "/Repos/")):
    output_path = "/Workspace" + output_path  # workspace paths are mounted under /Workspace
stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M")
out_dir = os.path.join(output_path, f"refdata-scan-{stamp}")

config = ScanConfig(
    min_list_items=int(dbutils.widgets.get("min_list_items")),
    min_case_branches=int(dbutils.widgets.get("min_case_branches")),
    inspect_file_contents=dbutils.widgets.get("inspect_files") == "yes",
    redact=dbutils.widgets.get("redact") == "yes",
    sample_values=0 if dbutils.widgets.get("redact") == "yes" else 5,
)

# Don't scan the scanner or its earlier results.
exclude = _list("exclude_paths") + [SCANNER_FOLDER, output_path]

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
print(f"\nReport and inventories written to {out_dir}")

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
# MAGIC ## Where the files are
# MAGIC
# MAGIC Each run writes a timestamped `refdata-scan-YYYYMMDD-HHMM` folder (the path is printed above):
# MAGIC
# MAGIC * **Default:** a `Results` folder next to this notebook. Open it in the workspace browser and download
# MAGIC   `report.html` or any CSV from the file's menu.
# MAGIC * **Volume:** if you set the output folder to a volume, find the files in Catalog Explorer. Use a volume for
# MAGIC   scheduled runs (a Databricks job running this notebook) so the history of scans is kept in one governed place,
# MAGIC   and compare `summary.json` across runs to track progress.
# MAGIC
# MAGIC Outputs:
# MAGIC * `report.html` / `report.md`: the assessment shown above.
# MAGIC * `code_findings.csv`, `files.csv`, `tables.csv`, `duplicate_groups.csv`: the full inventories.
# MAGIC * `findings.json`: everything in one machine-readable file. Optional: give it to an AI assistant with the
# MAGIC   `refdata-assessment` skill from the repository for a written assessment and classification of each finding.
# MAGIC * `summary.json`: counts only (no names, paths or values), safe to share.
# MAGIC
# MAGIC Earlier results are never scanned: the output folder and any `refdata-scan-*` folder are skipped.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC Reference data in spreadsheets and code has no owner, no approval workflow and no history.
# MAGIC [TitanRDM](https://titanrdm.com) gives business users a governed place to maintain it, published straight into
# MAGIC Databricks, Snowflake and Fabric.
