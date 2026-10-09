"""Code scanning entry point: route each notebook cell to the right scanner."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List

from ..models import CodeAsset, CodeFinding, ScanConfig
from ..notebook_parser import split_cells
from .python_scanner import scan_python
from .sql_scanner import scan_sql

# Quoted strings that end in a spreadsheet/CSV extension (or a glob of them).
_FILE_REF_RE = re.compile(
    r"""['"`]([^'"`\n]{1,500}?\.(?:csv|tsv|xlsx|xls|xlsm|xlsb)(?:\.gz)?)['"`]""",
    re.IGNORECASE,
)


@dataclass
class FileReference:
    path: str
    line: int


@dataclass
class CodeScanOutput:
    findings: List[CodeFinding] = field(default_factory=list)
    file_refs: List[FileReference] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


def find_file_references(text: str, line_offset: int = 1) -> List[FileReference]:
    refs = []
    for m in _FILE_REF_RE.finditer(text):
        refs.append(FileReference(m.group(1), text.count("\n", 0, m.start()) + line_offset))
    return refs


def scan_code_asset(asset: CodeAsset, cfg: ScanConfig, dialect: str = "databricks") -> CodeScanOutput:
    out = CodeScanOutput()
    out.file_refs = find_file_references(asset.content)
    cells = split_cells(asset.path, asset.content, asset.language, asset.format)
    for cell in cells:
        if cell.language == "python":
            findings, embedded, err = scan_python(cell.source, cfg, cell.start_line)
            out.findings.extend(findings)
            if err:
                out.warnings.append(f"{asset.path}: {err}")
            for emb in embedded:
                sql_findings, _ = scan_sql(emb.sql, cfg, dialect, emb.line)
                for f in sql_findings:
                    f.pattern = f.pattern.replace("sql_", "embedded_sql_", 1)
                out.findings.extend(sql_findings)
        elif cell.language == "sql":
            findings, warn = scan_sql(cell.source, cfg, dialect, cell.start_line)
            out.findings.extend(findings)
            if warn:
                out.warnings.append(f"{asset.path}: {warn}")
        # Scala and R cells: file references only (picked up above).

    for f in out.findings:
        f.source_path = asset.path
        f.source_kind = asset.kind
        f.owner = asset.owner
        f.modified_at = asset.modified_at
        f.location = asset.location
        f.in_job = asset.in_job
        if f.language == "sql" and asset.kind == "view":
            f.pattern = f.pattern.replace("sql_", "view_", 1)
    return out
