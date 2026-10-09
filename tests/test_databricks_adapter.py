"""Exercise the Databricks adapter end to end against an in-memory fake workspace.

The fake returns the Databricks SDK's own dataclasses, so attribute names are
checked against the real SDK.
"""
import base64
import io
import json
import os

import pytest

sdk = pytest.importorskip("databricks.sdk")
from databricks.sdk.service.catalog import ColumnInfo, DataSourceFormat, SchemaInfo, TableInfo, TableType, VolumeInfo  # noqa: E402
from databricks.sdk.service.files import DirectoryEntry, FileInfo  # noqa: E402
from databricks.sdk.service.jobs import BaseJob, JobSettings, NotebookTask, Task  # noqa: E402
from databricks.sdk.service.sql import ListQueryObjectsResponseQuery  # noqa: E402
from databricks.sdk.service.workspace import ExportResponse, Language, ObjectInfo, ObjectType  # noqa: E402

from refdata_scanner.adapters.databricks import DatabricksAdapter  # noqa: E402
from refdata_scanner.engine import run_scan  # noqa: E402
from refdata_scanner.models import ScanConfig  # noqa: E402
from refdata_scanner.report import write_report  # noqa: E402

NB_SALES = """# Databricks notebook source
STATE_MAP = {"NSW": "New South Wales", "QLD": "Queensland", "VIC": "Victoria", "SA": "South Australia", "WA": "Western Australia"}
cats = spark.read.csv("/Volumes/main/ref/uploads/categories.csv")
"""
NB_FIN = """# Databricks notebook source
STATE_MAP = {"NSW": "New South Wales", "QLD": "Queensland", "VIC": "Victoria", "SA": "South Australia", "WA": "West Australia"}
"""


class _NS:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class FakeWorkspace:
    def __init__(self):
        self.tree = {
            "/": [ObjectInfo(path="/Users", object_type=ObjectType.DIRECTORY),
                  ObjectInfo(path="/Shared", object_type=ObjectType.DIRECTORY),
                  ObjectInfo(path="/Repos", object_type=ObjectType.DIRECTORY)],
            "/Users": [ObjectInfo(path="/Users/jane@x.com", object_type=ObjectType.DIRECTORY)],
            "/Users/jane@x.com": [
                ObjectInfo(path="/Users/jane@x.com/sales", object_type=ObjectType.NOTEBOOK, language=Language.PYTHON,
                           modified_at=1_700_000_000_000),
                ObjectInfo(path="/Users/jane@x.com/upload.xlsx", object_type=ObjectType.FILE, size=2048,
                           modified_at=1_700_000_000_000),
            ],
            "/Shared": [ObjectInfo(path="/Shared/finance", object_type=ObjectType.NOTEBOOK, language=Language.PYTHON)],
            "/Repos": [ObjectInfo(path="/Repos/me/refdata-scanner", object_type=ObjectType.REPO)],
            "/Repos/me/refdata-scanner": [ObjectInfo(path="/Repos/me/refdata-scanner/x.py", object_type=ObjectType.FILE)],
        }
        self.content = {"/Users/jane@x.com/sales": NB_SALES, "/Shared/finance": NB_FIN,
                        "/Repos/me/refdata-scanner/x.py": "BIG = {" + ",".join(f"'{i}':'{i}'" for i in range(20)) + "}"}

    def list(self, path):
        if path not in self.tree:
            raise RuntimeError(f"RESOURCE_DOES_NOT_EXIST: {path}")
        return iter(self.tree[path])

    def export(self, path, format=None):
        return ExportResponse(content=base64.b64encode(self.content[path].encode()).decode())

    def download(self, path, format=None):
        return io.BytesIO(b"PK...")


class FakeClient:
    def __init__(self):
        self.config = _NS(host="https://adb-1.azuredatabricks.net")
        self.workspace = FakeWorkspace()
        self.current_user = _NS(me=lambda: _NS(user_name="scanner@x.com"))
        self.schemas = _NS(list=lambda catalog: iter([SchemaInfo(name="ref"), SchemaInfo(name="information_schema")]))
        self.volumes = _NS(list=lambda c, s: iter([VolumeInfo(name="uploads", owner="data-team")]))
        self.files = _NS(list_directory_contents=self._ls, download=lambda p: _NS(contents=io.BytesIO(b"a,b\n1,2\n")))
        self.dbfs = _NS(list=self._dbfs_ls, download=lambda p: io.BytesIO(b"x\n"))
        self.tables = _NS(list=lambda c, s: iter([
            TableInfo(full_name="main.ref.lkp_status", catalog_name="main", schema_name="ref", name="lkp_status",
                      table_type=TableType.MANAGED, data_source_format=DataSourceFormat.DELTA, owner="jane@x.com",
                      columns=[ColumnInfo(name="status_code"), ColumnInfo(name="status_name")]),
            TableInfo(full_name="main.ref.big_facts", catalog_name="main", schema_name="ref", name="big_facts",
                      table_type=TableType.MANAGED, data_source_format=DataSourceFormat.DELTA,
                      columns=[ColumnInfo(name=f"c{i}") for i in range(60)]),
            TableInfo(full_name="main.ref.v_region", catalog_name="main", schema_name="ref", name="v_region",
                      table_type=TableType.VIEW, view_definition="select case r when 'N' then 'North' when 'S' then "
                      "'South' when 'E' then 'East' when 'W' then 'West' when 'C' then 'Central' end as region from t"),
        ]))
        self.queries = _NS(list=lambda: iter([ListQueryObjectsResponseQuery(
            id="q1", display_name="Channel report", owner_user_name="bob@x.com",
            query_text="select * from t where channel in (" + ",".join(f"'{i}'" for i in range(12)) + ")")]))
        self.jobs = _NS(list=lambda expand_tasks=None: iter([BaseJob(job_id=1, settings=JobSettings(tasks=[
            Task(task_key="t", notebook_task=NotebookTask(notebook_path="/Shared/finance"))]))]))

    def _ls(self, path):
        entries = {
            "/Volumes/main/ref/uploads": [
                DirectoryEntry(path="/Volumes/main/ref/uploads/categories.csv", name="categories.csv", is_directory=False,
                               file_size=500, last_modified=1_700_000_000_000),
                DirectoryEntry(path="/Volumes/main/ref/uploads/sub/", name="sub", is_directory=True),
                DirectoryEntry(path="/Volumes/main/ref/uploads/refdata-scan-20260101-0900/", name="refdata-scan-20260101-0900",
                               is_directory=True),
                DirectoryEntry(path="/Volumes/main/ref/uploads/scans/", name="scans", is_directory=True),
            ],
            "/Volumes/main/ref/uploads/refdata-scan-20260101-0900": [
                DirectoryEntry(path="/Volumes/main/ref/uploads/refdata-scan-20260101-0900/files.csv", name="files.csv",
                               is_directory=False)],
            "/Volumes/main/ref/uploads/scans": [
                DirectoryEntry(path="/Volumes/main/ref/uploads/scans/old.csv", name="old.csv", is_directory=False)],
            "/Volumes/main/ref/uploads/sub": [
                DirectoryEntry(path="/Volumes/main/ref/uploads/sub/raw.parquet", name="raw.parquet", is_directory=False)],
        }
        return iter(entries[path])

    def _dbfs_ls(self, path):
        if path == "/FileStore":
            return iter([FileInfo(path="/FileStore/tables", is_dir=True)])
        if path == "/FileStore/tables":
            return iter([FileInfo(path="/FileStore/tables/cost_centres.csv", is_dir=False, file_size=90)])
        raise RuntimeError("not found")


def fake_sql(query):
    if "table_lineage" in query:
        return [{"source_path": "s3://bucket/landing/gl_accounts.xlsx", "target_table_full_name": "main.ref.lkp_status"}]
    if "query.history" in query:
        return [{"statement_text": "COPY INTO main.ref.x FROM 'dbfs:/FileStore/tables/cost_centres.csv'",
                 "executed_by": "bob@x.com", "last_run": "2026-10-01", "runs": 4}]
    if query.startswith("DESCRIBE DETAIL"):
        return [{"sizeInBytes": 4096}]
    return []


def test_databricks_scan_end_to_end(tmp_path):
    adapter = DatabricksAdapter(client=FakeClient(), catalog="main",
                                exclude_paths=["/Workspace/Repos/me/refdata-scanner", "/Volumes/main/ref/uploads/scans"],
                                sql_runner=fake_sql)
    result = run_scan(adapter, ScanConfig(inspect_file_contents=True))

    paths = {f.path for f in result.files}
    assert "/Volumes/main/ref/uploads/categories.csv" in paths
    assert "/Users/jane@x.com/upload.xlsx" in paths
    assert "dbfs:/FileStore/tables/cost_centres.csv" in paths
    assert "s3://bucket/landing/gl_accounts.xlsx" in paths  # known only from lineage
    assert not any("refdata-scan-" in p or "/scans/" in p for p in paths)  # earlier results never scanned

    vol = next(f for f in result.files if f.area == "volume")
    assert vol.referenced_by == ["/Users/jane@x.com/sales:3"] and vol.owner == "data-team"
    assert vol.header == ["a", "b"]
    dbfs = next(f for f in result.files if f.area == "dbfs")
    assert dbfs.referenced_by  # read by COPY INTO in query history

    sources = {f.source_path for f in result.code_findings}
    assert "/Repos/me/refdata-scanner/x.py" not in sources  # excluded folder
    assert {"/Users/jane@x.com/sales", "/Shared/finance", "main.ref.v_region", "query: Channel report"} <= sources
    assert not any(f.source_kind == "query_history" for f in result.code_findings)

    fin = next(f for f in result.code_findings if f.source_path == "/Shared/finance")
    assert fin.in_job and fin.group_conflict
    sales = next(f for f in result.code_findings if f.source_path == "/Users/jane@x.com/sales")
    assert sales.owner == "jane@x.com"

    names = [t.full_name for t in result.tables]
    assert names == ["main.ref.lkp_status"]
    assert result.tables[0].size_bytes == 4096
    assert result.tables[0].loaded_from_files == ["s3://bucket/landing/gl_accounts.xlsx"]
    assert result.stats["conflicting_groups"] == 1
    assert result.run_by == "scanner@x.com"

    out = write_report(result, ScanConfig(), str(tmp_path))
    html = open(out["report_html"], encoding="utf-8").read()
    assert "West Australia vs Western Australia" in html
    summary = json.load(open(out["summary"]))
    assert "jane" not in json.dumps(summary)


def test_redacted_report_hides_values_and_names(tmp_path):
    adapter = DatabricksAdapter(client=FakeClient(), catalog="main", sql_runner=fake_sql)
    cfg = ScanConfig(redact=True, sample_values=0)
    result = run_scan(adapter, cfg)
    out = write_report(result, cfg, str(tmp_path))
    for key in ("report_html", "report_md", "code_findings", "files", "findings", "duplicate_groups"):
        text = open(out[key], encoding="utf-8").read()
        assert "jane@x.com" not in text, key
        assert "West Australia" not in text, key
    assert os.path.exists(out["summary"])


def test_rejects_unsafe_catalog_name():
    with pytest.raises(ValueError):
        DatabricksAdapter(client=FakeClient(), catalog="main'; drop table x --")
