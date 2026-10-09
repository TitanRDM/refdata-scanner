"""Databricks adapter.

Finds, in one workspace:

* CSV/Excel files in Unity Catalog volumes, workspace folders (including Git
  folders) and legacy DBFS ``/FileStore``
* notebooks and code files in workspace folders, saved SQL queries, and view
  definitions in the chosen catalog/schemas
* tables in the chosen catalog/schemas (for lookup-table candidates)
* optionally, system tables: lineage (which tables were loaded from CSV/Excel)
  and query history (SQL that read CSV/Excel files)

Everything is read-only. Authentication uses the standard Databricks SDK
chain, so inside a Databricks notebook no configuration is needed.
"""
from __future__ import annotations

import base64
import datetime as _dt
import re
import time
from collections import deque
from typing import Any, Callable, Dict, Iterator, List, Optional

from ..models import CODE_EXTENSIONS, CodeAsset, FileAsset, TableAsset
from .base import Logger, PlatformAdapter

SqlRunner = Callable[[str], List[Dict[str, Any]]]

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_\-]+$")
_FILE_EXT_RE = r"\\.(csv|tsv|xlsx|xls|xlsm|xlsb)(\\.gz)?"


def _ms_to_iso(ms: Optional[int]) -> Optional[str]:
    if not ms:
        return None
    try:
        return _dt.datetime.fromtimestamp(int(ms) / 1000, _dt.timezone.utc).replace(microsecond=0).isoformat()
    except (ValueError, OSError, OverflowError):
        return None


def _enum(v: Any) -> Optional[str]:
    return getattr(v, "value", v) if v is not None else None


def _ws_path(path: str) -> str:
    """Workspace API paths have no /Workspace prefix: /Workspace/Users/x -> /Users/x."""
    path = path.strip()
    if path == "/Workspace":
        return "/"
    return re.sub(r"^/Workspace(?=/)", "", path) or "/"


def _owner_from_path(path: str) -> Optional[str]:
    m = re.match(r"^/(?:Workspace/)?(?:Users|Repos)/([^/]+)/", path)
    return m.group(1) if m else None


def spark_sql_runner(spark) -> SqlRunner:
    """Run SQL through an existing SparkSession (inside a Databricks notebook)."""

    def run(query: str) -> List[Dict[str, Any]]:
        return [row.asDict() for row in spark.sql(query).collect()]

    return run


def warehouse_sql_runner(client, warehouse_id: str, timeout_s: int = 300) -> SqlRunner:
    """Run SQL on a SQL warehouse through the Statement Execution API (for local runs)."""

    def run(query: str) -> List[Dict[str, Any]]:
        resp = client.statement_execution.execute_statement(statement=query, warehouse_id=warehouse_id,
                                                            wait_timeout="30s")
        deadline = time.time() + timeout_s
        while _enum(resp.status.state) in ("PENDING", "RUNNING"):
            if time.time() > deadline:
                raise TimeoutError("SQL statement timed out")
            time.sleep(2)
            resp = client.statement_execution.get_statement(resp.statement_id)
        if _enum(resp.status.state) != "SUCCEEDED":
            err = getattr(resp.status, "error", None)
            raise RuntimeError(getattr(err, "message", None) or f"statement {_enum(resp.status.state)}")
        cols = [c.name for c in resp.manifest.schema.columns]
        rows = (resp.result.data_array or []) if resp.result else []
        return [dict(zip(cols, r)) for r in rows]

    return run


class DatabricksAdapter(PlatformAdapter):
    platform = "databricks"
    dialect = "databricks"

    def __init__(
        self,
        client=None,
        catalog: Optional[str] = None,
        schemas: Optional[List[str]] = None,
        workspace_paths: Optional[List[str]] = None,
        exclude_paths: Optional[List[str]] = None,
        dbfs_paths: Optional[List[str]] = None,
        include_queries: bool = True,
        include_jobs: bool = True,
        use_system_tables: bool = True,
        measure_tables: bool = True,
        sql_runner: Optional[SqlRunner] = None,
        history_days: int = 90,
        max_workspace_objects: int = 50_000,
        log: Optional[Logger] = None,
    ):
        super().__init__(log)
        if client is None:
            from databricks.sdk import WorkspaceClient  # imported lazily: optional dependency

            client = WorkspaceClient()
        self.w = client
        self.catalog = catalog.strip() if catalog else None
        if self.catalog and not _SAFE_NAME.match(self.catalog):
            raise ValueError(f"Unexpected characters in catalog name: {self.catalog!r}")
        self.schemas = [s.strip() for s in (schemas or []) if s and s.strip()]
        for s in self.schemas:
            if not _SAFE_NAME.match(s):
                raise ValueError(f"Unexpected characters in schema name: {s!r}")
        self.workspace_paths = [_ws_path(p) for p in (workspace_paths if workspace_paths is not None else ["/"])]
        self.exclude_paths = [_ws_path(p).rstrip("/") for p in (exclude_paths or []) if p]
        self.dbfs_paths = dbfs_paths if dbfs_paths is not None else ["/FileStore"]
        self.include_queries = include_queries
        self.include_jobs = include_jobs
        self.use_system_tables = use_system_tables and sql_runner is not None
        self.measure_tables = measure_tables and sql_runner is not None
        self.sql = sql_runner
        self.history_days = int(history_days)
        self.max_workspace_objects = max_workspace_objects
        self._ws_objects: Optional[List[Any]] = None
        self._tables: Optional[List[Any]] = None
        self._schemas_resolved: Optional[List[str]] = None
        self._job_paths: Optional[set] = None
        self._user: Optional[str] = None

    # ------------------------------------------------------------------ scope
    def describe_scope(self) -> Dict[str, Any]:
        host = getattr(getattr(self.w, "config", None), "host", None)
        return {
            "workspace": host,
            "catalog": self.catalog,
            "schemas": self.schemas or ("all" if self.catalog else None),
            "workspace_paths": self.workspace_paths,
            "dbfs_paths": self.dbfs_paths,
            "system_tables": "yes" if self.use_system_tables else "no",
        }

    def run_by(self) -> Optional[str]:
        if self._user is None:
            try:
                self._user = self.w.current_user.me().user_name
            except Exception:
                self._user = ""
        return self._user or None

    def _resolve_schemas(self) -> List[str]:
        if self._schemas_resolved is not None:
            return self._schemas_resolved
        if not self.catalog:
            self._schemas_resolved = []
        elif self.schemas:
            self._schemas_resolved = self.schemas
        else:
            try:
                self._schemas_resolved = [s.name for s in self.w.schemas.list(self.catalog)
                                          if s.name and s.name != "information_schema"]
            except Exception as exc:
                self.warn(f"Could not list schemas in {self.catalog}: {exc}")
                self._schemas_resolved = []
        return self._schemas_resolved

    # -------------------------------------------------------------- workspace
    def _excluded(self, path: str) -> bool:
        return any(path == p or path.startswith(p + "/") for p in self.exclude_paths)

    def _workspace_objects(self) -> List[Any]:
        if self._ws_objects is not None:
            return self._ws_objects
        objs: List[Any] = []
        queue = deque(self.workspace_paths)
        seen = set()
        while queue:
            path = queue.popleft()
            if path in seen or self._excluded(path):
                continue
            seen.add(path)
            try:
                children = list(self.w.workspace.list(path))
            except Exception as exc:
                self.warn(f"Could not list workspace folder {path}: {exc}")
                continue
            for obj in children:
                kind = _enum(obj.object_type)
                if kind in ("DIRECTORY", "REPO"):
                    queue.append(obj.path)
                elif kind in ("NOTEBOOK", "FILE"):
                    objs.append(obj)
                if len(objs) >= self.max_workspace_objects:
                    self.warn(f"Stopped after {self.max_workspace_objects} workspace objects; narrow the workspace paths "
                              "or raise max_workspace_objects")
                    queue.clear()
                    break
            if len(seen) % 200 == 0:
                self.log(f"  listed {len(seen)} workspace folders, {len(objs)} objects ...")
        self._ws_objects = objs
        return objs

    def _job_notebooks(self) -> set:
        if self._job_paths is not None:
            return self._job_paths
        paths: set = set()
        if self.include_jobs:
            try:
                for job in self.w.jobs.list(expand_tasks=True):
                    settings = job.settings
                    if not settings or not settings.tasks:
                        continue
                    for task in settings.tasks:
                        for p in (
                            getattr(task.notebook_task, "notebook_path", None) if task.notebook_task else None,
                            getattr(task.spark_python_task, "python_file", None) if task.spark_python_task else None,
                            getattr(getattr(task.sql_task, "file", None), "path", None) if task.sql_task else None,
                        ):
                            if p and p.startswith("/"):
                                paths.add(re.sub(r"^/Workspace", "", p))
            except Exception as exc:
                self.warn(f"Could not list jobs (job usage will not be shown): {exc}")
        self._job_paths = paths
        return paths

    def _export(self, path: str) -> Optional[str]:
        from databricks.sdk.service.workspace import ExportFormat

        try:
            resp = self.w.workspace.export(path, format=ExportFormat.SOURCE)
            return base64.b64decode(resp.content or "").decode("utf-8", errors="replace")
        except Exception as exc:
            self.warn(f"Could not export {path}: {exc}")
            return None

    # ------------------------------------------------------------------ files
    def iter_files(self, extensions: tuple) -> Iterator[FileAsset]:
        yield from self._volume_files(extensions)
        for obj in self._workspace_objects():
            if _enum(obj.object_type) != "FILE":
                continue
            name = obj.path.rsplit("/", 1)[-1]
            ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
            if ext in extensions:
                yield FileAsset(path=obj.path, name=name, extension=ext, area="workspace",
                                container=obj.path.rsplit("/", 1)[0], size_bytes=obj.size,
                                modified_at=_ms_to_iso(obj.modified_at), owner=_owner_from_path(obj.path))
        yield from self._dbfs_files(extensions)

    def _volume_files(self, extensions: tuple) -> Iterator[FileAsset]:
        if not self.catalog:
            return
        for schema in self._resolve_schemas():
            try:
                volumes = list(self.w.volumes.list(self.catalog, schema))
            except Exception as exc:
                self.warn(f"Could not list volumes in {self.catalog}.{schema}: {exc}")
                continue
            for vol in volumes:
                root = f"/Volumes/{self.catalog}/{schema}/{vol.name}"
                container = f"{self.catalog}.{schema}.{vol.name}"
                self.log(f"  walking volume {container}")
                stack = [root]
                while stack:
                    d = stack.pop()
                    try:
                        entries = list(self.w.files.list_directory_contents(d))
                    except Exception as exc:
                        # Older SDKs lack the Files API; inside Databricks the volume is also mounted
                        # at the same /Volumes path, so read the listing from there instead.
                        entries = _local_listing(d)
                        if entries is None:
                            self.warn(f"Could not list {d}: {exc}")
                            continue
                    for e in entries:
                        if e.is_directory:
                            stack.append(e.path.rstrip("/"))
                            continue
                        name = e.name or e.path.rsplit("/", 1)[-1]
                        ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
                        if ext in extensions:
                            yield FileAsset(path=e.path, name=name, extension=ext, area="volume", container=container,
                                            size_bytes=e.file_size, modified_at=_ms_to_iso(e.last_modified),
                                            owner=vol.owner)

    def _dbfs_files(self, extensions: tuple) -> Iterator[FileAsset]:
        for root in self.dbfs_paths:
            stack = [root]
            while stack:
                d = stack.pop()
                try:
                    entries = list(self.w.dbfs.list(d))
                except Exception as exc:
                    msg = str(exc)
                    if d == root and ("not exist" in msg.lower() or "disabled" in msg.lower() or "404" in msg):
                        self.log(f"  DBFS path {root} not available, skipping")
                    else:
                        self.warn(f"Could not list DBFS {d}: {exc}")
                    continue
                for e in entries:
                    if e.is_dir:
                        stack.append(e.path)
                        continue
                    name = e.path.rsplit("/", 1)[-1]
                    ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
                    if ext in extensions:
                        yield FileAsset(path="dbfs:" + e.path if not e.path.startswith("dbfs:") else e.path, name=name,
                                        extension=ext, area="dbfs", container=e.path.rsplit("/", 1)[0],
                                        size_bytes=e.file_size, modified_at=_ms_to_iso(e.modification_time))

    def read_file_head(self, f: FileAsset, max_bytes: int) -> Optional[bytes]:
        try:
            if f.area == "volume":
                stream = self.w.files.download(f.path).contents
            elif f.area == "workspace":
                stream = self.w.workspace.download(f.path)
            elif f.area == "dbfs":
                stream = self.w.dbfs.download(f.path.replace("dbfs:", "", 1))
            else:
                return None
            data = stream.read(max_bytes)
            try:
                stream.close()
            except Exception:
                pass
            return data
        except Exception as exc:
            self.warn(f"Could not read {f.path}: {exc}")
            return None

    # ------------------------------------------------------------------- code
    def iter_code(self) -> Iterator[CodeAsset]:
        job_paths = self._job_notebooks()
        for obj in self._workspace_objects():
            kind = _enum(obj.object_type)
            path = obj.path
            if kind == "NOTEBOOK":
                lang = (_enum(obj.language) or "unknown").lower()
                content = self._export(path)
                fmt = "auto"
            elif kind == "FILE" and path.lower().endswith(CODE_EXTENSIONS):
                if obj.size and obj.size > 5 * 1024 * 1024:
                    continue
                lang = "python" if path.endswith(".py") else "sql" if path.endswith(".sql") else "unknown"
                content = self._export(path)
                fmt = "auto"
            else:
                continue
            if content is None:
                continue
            yield CodeAsset(path=path, kind="notebook" if kind == "NOTEBOOK" else "file", language=lang,
                            content=content, owner=_owner_from_path(path), modified_at=_ms_to_iso(obj.modified_at),
                            location=path.rsplit("/", 1)[0], in_job=path in job_paths, format=fmt)

        if self.include_queries:
            try:
                for q in self.w.queries.list():
                    # Newer SDKs: query_text / display_name. Older SDKs (legacy queries API): query / name.
                    text = getattr(q, "query_text", None) or getattr(q, "query", None)
                    if not text:
                        continue
                    name = getattr(q, "display_name", None) or getattr(q, "name", None) or q.id
                    owner = (getattr(q, "owner_user_name", None) or getattr(q, "last_modifier_user_name", None)
                             or getattr(getattr(q, "user", None), "email", None))
                    updated = getattr(q, "update_time", None) or getattr(q, "updated_at", None)
                    yield CodeAsset(path=f"query: {name}", kind="query", language="sql", content=text, owner=owner,
                                    modified_at=str(updated) if updated else None, location="SQL queries")
            except Exception as exc:
                self.warn(f"Could not list saved SQL queries: {exc}")

        for t in self._list_tables():
            if t.view_definition:
                yield CodeAsset(path=t.full_name, kind="view", language="sql", content=t.view_definition,
                                owner=t.owner, modified_at=_ms_to_iso(t.updated_at),
                                location=f"{t.catalog_name}.{t.schema_name}")

        if self.use_system_tables:
            yield from self._query_history()

    def _query_history(self) -> Iterator[CodeAsset]:
        q = f"""
            SELECT statement_text, executed_by, max(start_time) AS last_run, count(*) AS runs
            FROM system.query.history
            WHERE start_time >= current_timestamp() - INTERVAL {self.history_days} DAYS
              AND lower(statement_text) RLIKE '{_FILE_EXT_RE}'
            GROUP BY statement_text, executed_by
            ORDER BY runs DESC
            LIMIT 2000"""
        try:
            rows = self.sql(q)
        except Exception as exc:
            self.warn(f"Query history (system.query.history) not available: {_short_err(exc)}")
            return
        for n, row in enumerate(rows, 1):
            yield CodeAsset(path=f"query history #{n} ({row.get('runs')} runs)", kind="query_history", language="sql",
                            content=row.get("statement_text") or "", owner=row.get("executed_by"),
                            modified_at=str(row.get("last_run") or "") or None, location="Query history")

    # ----------------------------------------------------------------- tables
    def _list_tables(self) -> List[Any]:
        if self._tables is not None:
            return self._tables
        tables: List[Any] = []
        if self.catalog:
            for schema in self._resolve_schemas():
                try:
                    tables.extend(self.w.tables.list(self.catalog, schema))
                except Exception as exc:
                    self.warn(f"Could not list tables in {self.catalog}.{schema}: {exc}")
        self._tables = tables
        return tables

    def iter_tables(self) -> Iterator[TableAsset]:
        from ..analysis import has_name_hint

        measured = 0
        for t in self._list_tables():
            ttype = _enum(t.table_type)
            if t.view_definition or (ttype and "VIEW" in ttype):
                continue
            cols = [c.name for c in (t.columns or []) if c.name]
            asset = TableAsset(
                full_name=t.full_name or f"{t.catalog_name}.{t.schema_name}.{t.name}",
                catalog=t.catalog_name, schema=t.schema_name, name=t.name, table_type=ttype,
                data_format=_enum(t.data_source_format), column_count=len(cols) if cols else None, columns=cols,
                owner=t.owner, created_by=t.created_by, comment=t.comment, updated_at=_ms_to_iso(t.updated_at),
            )
            # Measuring size costs a query, so only do it for plausible candidates.
            if (self.measure_tables and measured < 300 and asset.data_format == "DELTA"
                    and (has_name_hint(asset.name) or (asset.column_count or 99) <= 6)):
                measured += 1
                try:
                    detail = self.sql(f"DESCRIBE DETAIL `{asset.catalog}`.`{asset.schema}`.`{asset.name}`")
                    if detail:
                        asset.size_bytes = detail[0].get("sizeInBytes")
                except Exception:
                    pass
            yield asset

    # ---------------------------------------------------------------- lineage
    def file_loads(self) -> Dict[str, List[str]]:
        if not self.use_system_tables or not self.catalog:
            return {}
        q = f"""
            SELECT source_path, target_table_full_name
            FROM system.access.table_lineage
            WHERE source_type = 'PATH'
              AND target_table_catalog = '{self.catalog}'
              AND lower(source_path) RLIKE '{_FILE_EXT_RE}$'
            GROUP BY source_path, target_table_full_name"""
        try:
            rows = self.sql(q)
        except Exception as exc:
            self.warn(f"Lineage (system.access.table_lineage) not available: {_short_err(exc)}")
            return {}
        loads: Dict[str, List[str]] = {}
        schemas = set(self._resolve_schemas())
        for r in rows:
            target = r.get("target_table_full_name")
            if not target or (schemas and target.split(".")[1] not in schemas):
                continue
            loads.setdefault(r["source_path"], []).append(target)
        self.log(f"  lineage: {len(loads)} CSV/Excel file(s) loaded into tables")
        return loads


def _local_listing(directory: str):
    """List a /Volumes directory through the local mount. Returns None if it is not mounted."""
    import os
    from types import SimpleNamespace

    if not os.path.isdir(directory):
        return None
    out = []
    try:
        with os.scandir(directory) as it:
            for e in it:
                st = e.stat()
                out.append(SimpleNamespace(path=e.path, name=e.name, is_directory=e.is_dir(), file_size=st.st_size,
                                           last_modified=int(st.st_mtime * 1000)))
    except OSError:
        return None
    return out


def _short_err(exc: Exception) -> str:
    msg = str(exc).strip().split("\n")[0]
    return msg[:200]
