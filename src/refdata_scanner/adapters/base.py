"""The interface every platform adapter implements.

An adapter knows how to *find* things on one platform (Databricks, a local
folder, and later Snowflake or Fabric). It never analyses anything itself;
the platform-neutral core in :mod:`refdata_scanner.engine` does that.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterator, List, Optional

from ..models import CodeAsset, FileAsset, TableAsset

Logger = Callable[[str], None]


class PlatformAdapter(ABC):
    platform: str = "unknown"
    dialect: str = "databricks"  # sqlglot dialect for this platform's SQL

    def __init__(self, log: Optional[Logger] = None):
        self.log: Logger = log or (lambda msg: None)
        self.warnings: List[str] = []

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)
        self.log(f"WARNING: {msg}")

    @abstractmethod
    def describe_scope(self) -> Dict[str, Any]:
        """What was scanned, for the report header."""

    def run_by(self) -> Optional[str]:
        return None

    @abstractmethod
    def iter_files(self, extensions: tuple) -> Iterator[FileAsset]:
        """CSV/Excel files in storage."""

    @abstractmethod
    def iter_code(self) -> Iterator[CodeAsset]:
        """Notebooks, code files, saved queries and view definitions."""

    def iter_tables(self) -> Iterator[TableAsset]:
        """Tables in the catalog (used to find small lookup tables)."""
        return iter(())

    def file_loads(self) -> Dict[str, List[str]]:
        """Map of file path -> tables loaded from it (from lineage/load history), if available."""
        return {}

    def read_file_head(self, f: FileAsset, max_bytes: int) -> Optional[bytes]:
        """Return up to max_bytes of a file's content, or None if not supported."""
        return None
