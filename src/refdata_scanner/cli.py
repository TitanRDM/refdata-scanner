"""Command line interface: ``refdata-scanner databricks ...`` or ``refdata-scanner local ...``."""
from __future__ import annotations

import argparse
import datetime as _dt
import sys
from typing import List, Optional

from . import __version__
from .engine import run_scan
from .models import ScanConfig
from .report import write_report


def _csv_list(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def _add_common(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("output and thresholds")
    g.add_argument("--out", default=None, help="Output folder (default: ./refdata-scan-<timestamp>)")
    g.add_argument("--min-list-items", type=int, default=10, help="Report lists with MORE than this many items (default 10)")
    g.add_argument("--min-mapping-items", type=int, default=5, help="Report mappings with at least this many entries (default 5)")
    g.add_argument("--min-case-branches", type=int, default=5, help="Report CASE statements with at least this many WHENs (default 5)")
    g.add_argument("--min-inline-rows", type=int, default=3, help="Report inline tables with at least this many rows (default 3)")
    g.add_argument("--inspect-files", action="store_true",
                   help="Read CSV/Excel header rows (downloads up to 5 MB per file). Off by default.")
    g.add_argument("--redact", action="store_true",
                   help="Hide values, comments and user names in the outputs, so the report can be shared")
    g.add_argument("--sample-values", type=int, default=5, help="How many example values to show per finding (0 = none)")
    g.add_argument("--quiet", action="store_true", help="Less progress output")


def _config(args) -> ScanConfig:
    return ScanConfig(min_list_items=args.min_list_items, min_mapping_items=args.min_mapping_items,
                      min_case_branches=args.min_case_branches, min_inline_rows=args.min_inline_rows,
                      inspect_file_contents=args.inspect_files, redact=args.redact,
                      sample_values=0 if args.redact else args.sample_values)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="refdata-scanner",
        description="Find hidden reference data: CSV/Excel files, small lookup tables, and hardcoded lists, "
                    "mappings and CASE statements in notebooks, queries and views. Read-only.")
    parser.add_argument("--version", action="version", version=f"refdata-scanner {__version__}")
    sub = parser.add_subparsers(dest="platform", required=True)

    db = sub.add_parser("databricks", help="Scan a Databricks workspace and Unity Catalog")
    g = db.add_argument_group("connection (standard Databricks SDK authentication)")
    g.add_argument("--profile", help="Profile name in ~/.databrickscfg")
    g.add_argument("--host", help="Workspace URL, e.g. https://adb-123.4.azuredatabricks.net")
    g.add_argument("--warehouse-id", help="SQL warehouse used for system tables (lineage, query history) and table sizes")
    g = db.add_argument_group("scope")
    g.add_argument("--catalog", help="Unity Catalog catalog to scan (volumes, tables, views)")
    g.add_argument("--schemas", type=_csv_list, default=[], help="Comma-separated schemas (default: all in the catalog)")
    g.add_argument("--workspace-path", action="append", dest="workspace_paths",
                   help="Workspace folder to scan for notebooks/files; repeatable (default: /)")
    g.add_argument("--exclude-path", action="append", dest="exclude_paths", default=[],
                   help="Workspace folder to skip; repeatable")
    g.add_argument("--dbfs-path", action="append", dest="dbfs_paths", help="DBFS folder to scan (default: /FileStore)")
    g.add_argument("--no-dbfs", action="store_true", help="Skip DBFS")
    g.add_argument("--no-queries", action="store_true", help="Skip saved SQL queries")
    g.add_argument("--no-jobs", action="store_true", help="Do not check which notebooks run in jobs")
    g.add_argument("--no-system-tables", action="store_true", help="Skip lineage and query history")
    g.add_argument("--history-days", type=int, default=90, help="Days of query history to search (default 90)")
    _add_common(db)

    loc = sub.add_parser("local", help="Scan local folders (Git checkouts, dbt projects, exported notebooks)")
    loc.add_argument("paths", nargs="+", help="Folders or files to scan")
    loc.add_argument("--dialect", default="databricks", help="SQL dialect: databricks, snowflake, tsql, ... (default databricks)")
    _add_common(loc)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = _config(args)
    log = (lambda m: None) if args.quiet else (lambda m: print(m, file=sys.stderr, flush=True))
    out = args.out or f"refdata-scan-{_dt.datetime.now().strftime('%Y%m%d-%H%M')}"

    if args.platform == "databricks":
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            print("The Databricks SDK is required: pip install 'refdata-scanner[databricks]'", file=sys.stderr)
            return 2
        from .adapters.databricks import DatabricksAdapter, warehouse_sql_runner

        kwargs = {k: v for k, v in (("profile", args.profile), ("host", args.host)) if v}
        client = WorkspaceClient(**kwargs)
        runner = warehouse_sql_runner(client, args.warehouse_id) if args.warehouse_id else None
        if runner is None and not args.no_system_tables:
            log("Note: no --warehouse-id given, so lineage, query history and table sizes are skipped.")
        if not args.catalog:
            log("Note: no --catalog given, so volumes, tables and views are skipped.")
        adapter = DatabricksAdapter(
            client=client, catalog=args.catalog, schemas=args.schemas, workspace_paths=args.workspace_paths,
            exclude_paths=args.exclude_paths, dbfs_paths=[] if args.no_dbfs else args.dbfs_paths,
            include_queries=not args.no_queries, include_jobs=not args.no_jobs,
            use_system_tables=not args.no_system_tables, sql_runner=runner, history_days=args.history_days, log=log)
    else:
        from .adapters.local import LocalAdapter

        adapter = LocalAdapter(args.paths, dialect=args.dialect, log=log)

    result = run_scan(adapter, cfg, log)
    paths = write_report(result, cfg, out)
    s = result.stats
    print(f"\nScan complete: {s['files_found']} CSV/Excel files, {s['lookup_table_candidates']} lookup-table candidates, "
          f"{s['code_findings']} hardcoded lists/mappings ({s['duplicate_groups']} duplicated, "
          f"{s['conflicting_groups']} with conflicting copies).")
    print(f"Report: {paths['report_html']}")
    print(f"Inventories: {out}/")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
