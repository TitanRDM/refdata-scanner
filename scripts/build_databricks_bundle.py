"""Build importable Databricks bundles from the notebooks in notebooks/databricks.

Produces, in dist/ (or --out):

* refdata-scanner-databricks.zip  (recommended)
    refdata-scanner/01_quick_scan      SQL notebook
    refdata-scanner/02_full_scan       Python notebook
    refdata-scanner/lib/refdata_scanner/...   the scanner package as workspace files
  Import with Workspace > Import > File, choosing the .zip. Everything runs offline
  (apart from the %pip install of sqlglot/openpyxl from your package index).

* refdata-scanner.dbc  (notebooks only)
  A DBC archive cannot carry plain files, so 02_full_scan installs the scanner
  from GitHub on first run. Use the .zip if the cluster cannot reach GitHub.

Usage: python scripts/build_databricks_bundle.py [--out dist]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import uuid
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_DIR = os.path.join(ROOT, "notebooks", "databricks")
PACKAGE_DIR = os.path.join(ROOT, "src", "refdata_scanner")
BUNDLE_FOLDER = "refdata-scanner"

_HEADER_RE = re.compile(r"^(#|--)\s*Databricks notebook source\s*$")
_SEPARATOR_RE = re.compile(r"^(#|--)\s*COMMAND -{5,}\s*$")
_MAGIC_RE = re.compile(r"^(#|--)\s*MAGIC ?")
# A fixed timestamp keeps zip output identical between builds of the same source.
_ZIP_DATE = (2026, 1, 1, 0, 0, 0)


def _notebooks():
    for name in sorted(os.listdir(NOTEBOOK_DIR)):
        path = os.path.join(NOTEBOOK_DIR, name)
        if not name.endswith((".py", ".sql")):
            continue
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        first = content.split("\n", 1)[0].strip()
        if not _HEADER_RE.match(first):
            raise SystemExit(f"{name} is not in Databricks source format (missing header line)")
        yield name, content


def split_commands(content: str):
    """Split a source-format notebook into command texts, unwrapping MAGIC lines."""
    lines = content.split("\n")[1:]
    cells, current = [], []
    for line in lines + [None]:
        if line is None or _SEPARATOR_RE.match(line.strip()):
            while current and not current[0].strip():
                current.pop(0)
            while current and not current[-1].strip():
                current.pop()
            if current:
                if _MAGIC_RE.match(current[0]):
                    current = [_MAGIC_RE.sub("", l, count=1) for l in current]
                cells.append("\n".join(current))
            current = []
        else:
            current.append(line)
    return cells


def _command(text: str, position: int) -> dict:
    return {
        "version": "CommandV1", "origId": 0, "guid": str(uuid.uuid4()), "subtype": "command",
        "commandType": "auto", "position": float(position), "command": text, "commandVersion": 0,
        "state": "finished", "results": None, "resultDbfsStatus": "INLINED_IN_TREE",
        "resultDbfsErrorMessage": None, "errorSummary": None, "errorTraceType": None, "error": None,
        "workflows": [], "startTime": 0, "submitTime": 0, "finishTime": 0, "collapsed": False,
        "bindings": {}, "inputWidgets": {}, "displayType": "table", "width": "auto", "height": "auto",
        "xColumns": None, "yColumns": None, "pivotColumns": None, "pivotAggregation": None,
        "useConsistentColors": False, "customPlotOptions": {}, "commentThread": [], "commentsVisible": False,
        "parentHierarchy": [], "diffInserts": [], "diffDeletes": [], "globalVars": {}, "latestUser": "",
        "latestUserId": None, "commandTitle": "", "showCommandTitle": False, "hideCommandCode": False,
        "hideCommandResult": False, "isLockedInExamMode": False, "iPythonMetadata": None, "metadata": {},
        "streamStates": {}, "datasetPreviewNameToCmdIdMap": {}, "tableResultIndex": None,
        "listResultMetadata": [], "subcommandOptions": None, "nuid": str(uuid.uuid4()),
    }


def notebook_json(name: str, language: str, content: str) -> dict:
    return {
        "version": "NotebookV1", "origId": 0, "name": name, "language": language,
        "commands": [_command(text, (i + 1) * 1000) for i, text in enumerate(split_commands(content))],
        "dashboards": [], "guid": str(uuid.uuid4()), "globalVars": {}, "iPythonMetadata": None,
        "inputWidgets": {}, "notebookMetadata": {}, "reposExportFormat": "SOURCE",
    }


def _write(zf: zipfile.ZipFile, arcname: str, data: str) -> None:
    info = zipfile.ZipInfo(arcname, date_time=_ZIP_DATE)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    zf.writestr(info, data)


def build_zip(out_dir: str) -> str:
    path = os.path.join(out_dir, "refdata-scanner-databricks.zip")
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in _notebooks():
            _write(zf, f"{BUNDLE_FOLDER}/{name}", content)
        for root, dirs, files in os.walk(PACKAGE_DIR):
            dirs[:] = sorted(d for d in dirs if d != "__pycache__")
            for f in sorted(files):
                if f.endswith(".pyc"):
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, os.path.dirname(PACKAGE_DIR)).replace(os.sep, "/")
                with open(full, encoding="utf-8") as fh:
                    _write(zf, f"{BUNDLE_FOLDER}/lib/{rel}", fh.read())
    return path


def build_dbc(out_dir: str) -> str:
    path = os.path.join(out_dir, "refdata-scanner.dbc")
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in _notebooks():
            stem, ext = os.path.splitext(name)
            language = "python" if ext == ".py" else "sql"
            entry_ext = ".python" if language == "python" else ".sql"
            _write(zf, f"{BUNDLE_FOLDER}/{stem}{entry_ext}", json.dumps(notebook_json(stem, language, content)))
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", default=os.path.join(ROOT, "dist"))
    args = parser.parse_args()
    os.makedirs(args.out, exist_ok=True)
    for built in (build_zip(args.out), build_dbc(args.out)):
        print(f"built {os.path.relpath(built)} ({os.path.getsize(built):,} bytes)")


if __name__ == "__main__":
    main()
