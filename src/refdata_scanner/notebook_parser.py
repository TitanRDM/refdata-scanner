"""Split notebooks into language-tagged cells.

Supported formats:

* Databricks SOURCE export (``# Databricks notebook source``) for Python, SQL,
  Scala and R notebooks, including ``%sql`` / ``%python`` magic cells.
* Jupyter ``.ipynb`` (Databricks, Fabric and Snowflake notebooks can all be
  exported as ipynb), including ``%%sql`` / ``%sql`` cell magics.
* Plain ``.py`` and ``.sql`` files.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import List, Optional

_HEADER_RE = re.compile(r"^(#|--|//)\s*Databricks notebook source\s*$")
_SEPARATOR_RE = re.compile(r"^(#|--|//)\s*COMMAND -{5,}\s*$")
_MAGIC_PREFIX_RE = re.compile(r"^(#|--|//)\s*MAGIC ?")

_LANG_ALIASES = {
    "python": "python", "py": "python", "pyspark": "python", "sparkr": "r",
    "sql": "sql", "sparksql": "sql", "scala": "scala", "spark": "scala", "r": "r",
}
_SKIP_MAGICS = {"md", "markdown", "run", "pip", "sh", "fs", "conda", "lsmagic", "configure", "help"}


@dataclass
class Cell:
    language: str  # python | sql | scala | r | skip
    source: str
    start_line: int  # 1-based line of the first source line in the original file


def _language_from_magic(first_line: str, default: str) -> tuple:
    """Return (language, strip_first_line) for a cell's first line."""
    stripped = first_line.strip()
    m = re.match(r"^%{1,2}(\w+)", stripped)
    if not m:
        return default, False
    magic = m.group(1).lower()
    if magic in _LANG_ALIASES:
        return _LANG_ALIASES[magic], True
    if magic in _SKIP_MAGICS:
        return "skip", True
    return default, False


def detect_format(path: str, content: str) -> str:
    first = content.lstrip("﻿").split("\n", 1)[0]
    if _HEADER_RE.match(first.strip()):
        return "databricks_source"
    if path.lower().endswith(".ipynb") or content.lstrip().startswith("{") and '"cells"' in content[:5000]:
        return "ipynb"
    return "plain"


def language_from_path(path: str, default: str = "unknown") -> str:
    p = path.lower()
    if p.endswith(".py"):
        return "python"
    if p.endswith(".sql"):
        return "sql"
    if p.endswith(".scala"):
        return "scala"
    if p.endswith(".r"):
        return "r"
    return default


def split_cells(path: str, content: str, language: str = "unknown", fmt: str = "auto") -> List[Cell]:
    if fmt == "auto":
        fmt = detect_format(path, content)
    if language in ("", None, "unknown"):
        language = language_from_path(path, "unknown")
    if fmt == "databricks_source":
        return _split_databricks(content, language)
    if fmt == "ipynb":
        return _split_ipynb(content, language)
    return [Cell(language=language, source=content, start_line=1)]


def _split_databricks(content: str, language: str) -> List[Cell]:
    lines = content.lstrip("﻿").split("\n")
    first = lines[0].strip() if lines else ""
    if language in ("unknown", ""):
        language = {"#": "python", "--": "sql", "//": "scala"}.get(first.split(" ")[0], "python")

    cells: List[Cell] = []
    current: List[str] = []
    start = 2  # line after the header

    def flush(end_start: int) -> None:
        nonlocal current
        if not current:
            return
        cells.append(_make_db_cell(current, start, language))
        current = []

    for idx, line in enumerate(lines[1:], start=2):
        if _SEPARATOR_RE.match(line.strip()):
            flush(idx)
            start = idx + 1
            continue
        current.append(line)
    flush(len(lines))
    return [c for c in cells if c.language != "skip" and c.source.strip()]


def _make_db_cell(lines: List[str], start_line: int, default_lang: str) -> Cell:
    # Skip leading blank lines but keep line numbers accurate.
    offset = 0
    while offset < len(lines) and not lines[offset].strip():
        offset += 1
    body = lines[offset:]
    line_no = start_line + offset
    if body and _MAGIC_PREFIX_RE.match(body[0]):
        unwrapped = [_MAGIC_PREFIX_RE.sub("", l, count=1) for l in body]
        lang, strip_first = _language_from_magic(unwrapped[0], default_lang)
        if strip_first:
            return Cell(lang, "\n".join(unwrapped[1:]), line_no + 1)
        return Cell(lang, "\n".join(unwrapped), line_no)
    if body and body[0].startswith("%"):
        lang, strip_first = _language_from_magic(body[0], default_lang)
        if strip_first:
            return Cell(lang, "\n".join(body[1:]), line_no + 1)
    return Cell(default_lang, "\n".join(body), line_no)


def _split_ipynb(content: str, language: str) -> List[Cell]:
    try:
        nb = json.loads(content)
    except (ValueError, TypeError):
        return [Cell(language, content, 1)]
    default = language
    if default in ("unknown", ""):
        meta = nb.get("metadata", {}) or {}
        lang_name: Optional[str] = (
            (meta.get("language_info") or {}).get("name")
            or (meta.get("kernelspec") or {}).get("language")
            or (meta.get("application/vnd.databricks.v1+notebook") or {}).get("language")
        )
        default = _LANG_ALIASES.get((lang_name or "python").lower(), "python")

    cells: List[Cell] = []
    line_no = 1
    for cell in nb.get("cells", []):
        src = cell.get("source", "")
        if isinstance(src, list):
            src = "".join(src)
        n_lines = src.count("\n") + 1
        if cell.get("cell_type") != "code":
            line_no += n_lines
            continue
        cell_lang = default
        meta = cell.get("metadata", {}) or {}
        meta_lang = meta.get("language") or (meta.get("microsoft") or {}).get("language")
        if meta_lang:
            cell_lang = _LANG_ALIASES.get(str(meta_lang).lower(), cell_lang)
        first, _, rest = src.partition("\n")
        lang, strip_first = _language_from_magic(first, cell_lang)
        if strip_first:
            cells.append(Cell(lang, rest, line_no + 1))
        else:
            cells.append(Cell(lang, src, line_no))
        line_no += n_lines
    return [c for c in cells if c.language != "skip" and c.source.strip()]
