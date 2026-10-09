"""Scan a local folder: a Git checkout of notebooks, a dbt project, an exported workspace.

Useful on its own (most teams keep notebooks and dbt projects in Git) and for
trying the scanner without connecting to a platform.
"""
from __future__ import annotations

import datetime as _dt
import os
from typing import Any, Dict, Iterator, List, Optional

from ..models import CODE_EXTENSIONS, CodeAsset, FileAsset
from .base import Logger, PlatformAdapter

SKIP_DIRS = {".git", ".hg", ".svn", "node_modules", ".venv", "venv", "env", "__pycache__", ".tox", ".mypy_cache",
             ".pytest_cache", "site-packages", "dbt_packages", "target", "logs", ".idea", ".vscode", "dist", "build"}
MAX_CODE_BYTES = 5 * 1024 * 1024


def _mtime(path: str) -> Optional[str]:
    try:
        ts = os.path.getmtime(path)
    except OSError:
        return None
    return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).replace(microsecond=0).isoformat()


class LocalAdapter(PlatformAdapter):
    platform = "local"
    dialect = "databricks"

    def __init__(self, paths: List[str], dialect: str = "databricks", log: Optional[Logger] = None):
        super().__init__(log)
        self.given_paths = list(paths)
        self.paths = [os.path.abspath(p) for p in paths]
        self.dialect = dialect
        self._dbt_roots = self._find_dbt_roots()
        self._abs: Dict[str, str] = {}

    def describe_scope(self) -> Dict[str, Any]:
        return {"paths": self.given_paths}

    def run_by(self) -> Optional[str]:
        return None

    def _walk(self) -> Iterator[str]:
        for root_path in self.paths:
            if os.path.isfile(root_path):
                yield root_path
                continue
            for root, dirs, files in os.walk(root_path):
                dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS and not d.startswith("."))
                for name in sorted(files):
                    yield os.path.join(root, name)

    def display(self, path: str) -> str:
        """Paths in the report are shown relative to the scanned folder, Databricks-style."""
        for root in self.paths:
            base = root if os.path.isdir(root) else os.path.dirname(root)
            rel = os.path.relpath(path, base)
            if not rel.startswith(".."):
                prefix = "/" + os.path.basename(base) if len(self.paths) > 1 else ""
                return prefix + "/" + rel.replace(os.sep, "/")
        return path

    def _find_dbt_roots(self) -> List[str]:
        roots = []
        for root_path in self.paths:
            if os.path.isfile(root_path):
                continue
            for root, dirs, files in os.walk(root_path):
                dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith(".")]
                if "dbt_project.yml" in files:
                    roots.append(root)
        return roots

    def _is_dbt_seed(self, path: str) -> bool:
        for root in self._dbt_roots:
            rel = os.path.relpath(path, root)
            if not rel.startswith("..") and (rel.split(os.sep)[0] in ("seeds", "data")):
                return True
        return False

    def iter_files(self, extensions: tuple) -> Iterator[FileAsset]:
        for path in self._walk():
            name = os.path.basename(path)
            ext = os.path.splitext(name)[1].lower()
            if ext not in extensions:
                continue
            try:
                size = os.path.getsize(path)
            except OSError:
                size = None
            shown = self.display(path)
            self._abs[shown] = path
            yield FileAsset(path=shown, name=name, extension=ext, area="local", container=shown.rsplit("/", 1)[0] or "/",
                            size_bytes=size, modified_at=_mtime(path), is_dbt_seed=self._is_dbt_seed(path))

    def iter_code(self) -> Iterator[CodeAsset]:
        for path in self._walk():
            ext = os.path.splitext(path)[1].lower()
            if ext not in CODE_EXTENSIONS:
                continue
            try:
                if os.path.getsize(path) > MAX_CODE_BYTES:
                    self.warn(f"Skipped large file {path}")
                    continue
                with open(path, encoding="utf-8", errors="replace") as fh:
                    content = fh.read()
            except OSError as exc:
                self.warn(f"Could not read {path}: {exc}")
                continue
            kind = "notebook" if ext == ".ipynb" or content.lstrip().startswith(("# Databricks notebook source", "-- Databricks notebook source")) else "file"
            if ext == ".sql" and any(not os.path.relpath(path, r).startswith("..") for r in self._dbt_roots):
                kind = "dbt_model"
            shown = self.display(path)
            yield CodeAsset(path=shown, kind=kind, language={".py": "python", ".sql": "sql"}.get(ext, "unknown"),
                            content=content, modified_at=_mtime(path), location=shown.rsplit("/", 1)[0] or "/")

    def read_file_head(self, f: FileAsset, max_bytes: int) -> Optional[bytes]:
        try:
            with open(self._abs.get(f.path, f.path), "rb") as fh:
                return fh.read(max_bytes)
        except OSError:
            return None
