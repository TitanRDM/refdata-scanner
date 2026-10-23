"""refdata-scanner: find hidden reference data in your data platform.

Quick use inside a Databricks notebook::

    from refdata_scanner import scan_databricks
    paths = scan_databricks(catalog="main", schemas=["finance", "sales"],
                            workspace_paths=["/Users", "/Shared"], spark=spark,
                            out_dir="/Volumes/main/default/scans/run1")

Or from a terminal: ``refdata-scanner databricks --catalog main --schemas finance,sales``
"""
from __future__ import annotations

from typing import Dict, List, Optional

from .engine import run_scan
from .models import ScanConfig, ScanResult
from .report import write_report

__version__ = "0.1.0"
__all__ = ["run_scan", "write_report", "scan_databricks", "scan_local", "ScanConfig", "ScanResult"]


def _printer(msg: str) -> None:
    print(msg, flush=True)


def scan_databricks(
    catalog: Optional[str] = None,
    schemas: Optional[List[str]] = None,
    workspace_paths: Optional[List[str]] = None,
    exclude_paths: Optional[List[str]] = None,
    dbfs_paths: Optional[List[str]] = None,
    use_system_tables: bool = True,
    include_queries: bool = True,
    include_jobs: bool = True,
    history_days: int = 90,
    out_dir: str = "refdata-scan",
    spark=None,
    client=None,
    warehouse_id: Optional[str] = None,
    config: Optional[ScanConfig] = None,
    verbose: bool = True,
) -> Dict[str, str]:
    """Scan a Databricks workspace and write the report. Returns the output file paths.

    catalog / schemas: Unity Catalog scope for volumes, tables and views (schemas blank = all).
    workspace_paths: workspace folders to scan for notebooks and files (default: the whole workspace, "/").
        Paths may be written with or without the /Workspace prefix. A folder that doesn't exist stops the scan.
    exclude_paths: workspace or volume folders to skip.
    dbfs_paths: DBFS folders to scan (default: /FileStore); [] to skip DBFS.
    use_system_tables: read lineage and query history (needs ``spark`` or ``warehouse_id``).

    Pass ``spark`` (inside a notebook) or ``warehouse_id`` (from a terminal) to enable
    system-table lineage, query history and table sizes; without either, the scan
    still covers files, notebooks, queries, views and table metadata.
    """
    from .adapters.databricks import DatabricksAdapter, spark_sql_runner, warehouse_sql_runner

    log = _printer if verbose else None
    if client is None:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
    runner = spark_sql_runner(spark) if spark is not None else (
        warehouse_sql_runner(client, warehouse_id) if warehouse_id else None)
    adapter = DatabricksAdapter(client=client, catalog=catalog, schemas=schemas, workspace_paths=workspace_paths,
                                exclude_paths=exclude_paths, dbfs_paths=dbfs_paths,
                                use_system_tables=use_system_tables, include_queries=include_queries,
                                include_jobs=include_jobs, history_days=history_days, sql_runner=runner, log=log)
    adapter.check_workspace_paths()
    if log:
        log(f"Scope: catalog={catalog or '(none)'}, schemas={', '.join(adapter.schemas) or 'all'}, "
            f"workspace folders={', '.join(adapter.workspace_paths)}, excluded={', '.join(adapter.exclude_paths) or 'none'}")
    cfg = config or ScanConfig()
    result = run_scan(adapter, cfg, log)
    return write_report(result, cfg, out_dir)


def scan_local(paths: List[str], out_dir: str = "refdata-scan", dialect: str = "databricks",
               config: Optional[ScanConfig] = None, verbose: bool = True) -> Dict[str, str]:
    """Scan local folders (Git checkouts, dbt projects, exported notebooks)."""
    from .adapters.local import LocalAdapter

    log = _printer if verbose else None
    cfg = config or ScanConfig()
    result = run_scan(LocalAdapter(paths, dialect=dialect, log=log), cfg, log)
    return write_report(result, cfg, out_dir)
