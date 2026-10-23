# Databricks notebook source
# MAGIC %md
# MAGIC # Install refdata-scanner __VERSION__
# MAGIC
# MAGIC Run this notebook once to install [refdata-scanner](https://github.com/TitanRDM/refdata-scanner) into your
# MAGIC workspace. It creates a `refdata-scanner` folder next to this notebook containing:
# MAGIC
# MAGIC * `01_quick_scan`: the five-minute SQL check
# MAGIC * `02_full_scan`: the full scan and report
# MAGIC * `lib/`: the scanner's Python code and the sqlglot SQL parser it uses (MIT licence)
# MAGIC
# MAGIC **What it does:** the cells below hold those files as one compressed, base64-encoded archive. The last cell
# MAGIC checks the archive's SHA-256 checksum, unpacks it in memory and writes each file into the folder through this
# MAGIC workspace's own API, as you. It reads nothing else, installs no packages and contacts nothing outside your
# MAGIC workspace. The archive is byte-for-byte `refdata-scanner-databricks.zip` from the same GitHub release, so you can
# MAGIC compare the checksum below with that file, or read the files in the zip before running this.
# MAGIC
# MAGIC Archive SHA-256: `__SHA256__`
# MAGIC
# MAGIC **How to run:** attach any compute (serverless is fine), optionally change the install folder in the widget,
# MAGIC then *Run all*. You can delete this notebook afterwards.

# COMMAND ----------

dbutils.widgets.text("install_folder", "", "Install folder (blank = refdata-scanner next to this notebook)")
dbutils.widgets.dropdown("overwrite", "yes", ["yes", "no"], "Overwrite an existing install")

# COMMAND ----------

PAYLOAD_SHA256 = "__SHA256__"
PAYLOAD = []

# COMMAND ----------

#__PAYLOAD_CELLS__

# COMMAND ----------

import base64
import hashlib
import io
import posixpath
import re
import zipfile

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

data = base64.b64decode("".join(PAYLOAD))
digest = hashlib.sha256(data).hexdigest()
if digest != PAYLOAD_SHA256:
    raise ValueError(f"Archive checksum mismatch ({digest}); the notebook may be damaged. Download it again.")

w = WorkspaceClient()
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_nb_path = re.sub(r"^/Workspace(?=/)", "", _nb_path)
target = dbutils.widgets.get("install_folder").strip().rstrip("/") or posixpath.join(posixpath.dirname(_nb_path),
                                                                                       "refdata-scanner")
target = re.sub(r"^/Workspace(?=/)", "", target)

try:
    w.workspace.get_status(target)
    exists = True
except Exception:
    exists = False
if exists and dbutils.widgets.get("overwrite") != "yes":
    raise RuntimeError(f"{target} already exists. Set 'Overwrite an existing install' to yes, or choose another folder.")

NOTEBOOKS = {"01_quick_scan.sql": Language.SQL, "02_full_scan.py": Language.PYTHON}
archive = zipfile.ZipFile(io.BytesIO(data))
written = 0
made_dirs = set()
for name in archive.namelist():
    if name.endswith("/"):
        continue
    rel = name.split("/", 1)[1] if "/" in name else name  # drop the archive's top-level folder
    folder = posixpath.dirname(posixpath.join(target, rel))
    if folder not in made_dirs:
        w.workspace.mkdirs(folder)
        made_dirs.add(folder)
    content = archive.read(name)
    if rel in NOTEBOOKS:
        w.workspace.upload(posixpath.join(target, posixpath.splitext(rel)[0]), content, format=ImportFormat.SOURCE,
                           language=NOTEBOOKS[rel], overwrite=True)
    else:
        w.workspace.upload(posixpath.join(target, rel), content, format=ImportFormat.AUTO, overwrite=True)
    written += 1
    if written % 50 == 0:
        print(f"  {written} files written ...")

print(f"Installed refdata-scanner __VERSION__: {written} files in {target}")
print(f"Next: open {target}/01_quick_scan (SQL warehouse) or {target}/02_full_scan.")
