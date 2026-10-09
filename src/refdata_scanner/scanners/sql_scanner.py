"""Find hardcoded reference data in SQL using ``sqlglot``.

Detects:

* ``CASE`` expressions with many ``WHEN`` branches (code -> label mappings)
* long ``IN (...)`` lists of literals
* inline tables: ``VALUES (...), (...)`` and ``INSERT INTO ... VALUES``

Falls back to regular expressions when a statement cannot be parsed (for
example because of templating), so nothing is silently skipped.
"""
from __future__ import annotations

import logging
import re
from typing import List, Optional, Tuple

from ..models import CodeFinding, ScanConfig

try:  # sqlglot is a hard dependency, but keep the scanner importable without it.
    import sqlglot
    from sqlglot import exp
    from sqlglot.errors import ErrorLevel

    _HAS_SQLGLOT = True
    logging.getLogger("sqlglot").setLevel(logging.ERROR)  # unsupported-syntax notices are expected
except ImportError:  # pragma: no cover
    _HAS_SQLGLOT = False

# Dialect names used by sqlglot for each platform.
PLATFORM_DIALECTS = {"databricks": "databricks", "snowflake": "snowflake", "fabric": "tsql", "local": "databricks"}

_JINJA_EXPR = re.compile(r"\{\{.*?\}\}", re.S)
_JINJA_STMT = re.compile(r"\{%.*?%\}|\{#.*?#\}", re.S)
_DOLLAR_VAR = re.compile(r"\$\{\s*([A-Za-z_][\w.]*)\s*\}")


_JINJA_LINE = re.compile(r"^[ \t]*\{\{.*?\}\}[ \t]*$", re.M)


def _prepare(sql: str) -> str:
    """Neutralise templating so the parser can cope, keeping line numbers intact."""
    sql = _JINJA_STMT.sub(lambda m: "\n" * m.group(0).count("\n"), sql)
    sql = _JINJA_LINE.sub("", sql)  # e.g. {{ config(...) }} on its own line
    sql = _JINJA_EXPR.sub(lambda m: "templ_var" + "\n" * m.group(0).count("\n"), sql)
    sql = _DOLLAR_VAR.sub(lambda m: m.group(1).replace(".", "_"), sql)
    return sql


def _lit(node) -> Tuple[bool, Optional[str]]:
    if isinstance(node, exp.Literal):
        return True, str(node.this).strip()
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal):
        return True, "-" + str(node.this.this)
    if isinstance(node, exp.Null):
        return True, "NULL"
    if isinstance(node, exp.Boolean):
        return True, str(node.this)
    return False, None


def _line_of(node, line_offset: int) -> int:
    for sub in node.walk():
        meta = getattr(sub, "meta", None) or {}
        if "line" in meta:
            return meta["line"] + line_offset - 1
    return line_offset


def _col(node) -> Optional[str]:
    if node is None:
        return None
    if isinstance(node, exp.Column):
        return node.name
    c = node.find(exp.Column)
    return c.name if c else None


def _snippet(node) -> str:
    try:
        text = node.sql()
    except Exception:  # pragma: no cover
        text = str(node)
    text = re.sub(r"\s+", " ", text)
    return text[:240] + ("..." if len(text) > 240 else "")


def _finding(node, pattern, shape, count, values, pairs, name, line_offset) -> CodeFinding:
    return CodeFinding(
        source_path="", source_kind="", language="sql", line=_line_of(node, line_offset),
        pattern=pattern, shape=shape, item_count=count, name=name, values=values,
        pairs=pairs or [], snippet=_snippet(node),
    )


def _scan_tree(tree, cfg: ScanConfig, line_offset: int) -> List[CodeFinding]:
    out: List[CodeFinding] = []
    seen_in: set = set()

    for case in tree.find_all(exp.Case):
        ifs = case.args.get("ifs") or []
        if len(ifs) < cfg.min_case_branches:
            continue
        simple_col = _col(case.args.get("this"))
        keys, pairs, column = [], [], simple_col
        for branch in ifs:
            cond, result = branch.this, branch.args.get("true")
            ok_v, v = _lit(result)
            v = v if ok_v else _snippet(result)
            branch_keys: List[str] = []
            if simple_col is not None:
                ok, k = _lit(cond)
                if ok:
                    branch_keys = [k]
            elif isinstance(cond, exp.EQ):
                ok, k = _lit(cond.expression)
                if not ok:
                    ok, k = _lit(cond.this)
                if ok:
                    branch_keys = [k]
                    column = column or _col(cond)
            elif isinstance(cond, exp.In) and not cond.args.get("query"):
                lits = [_lit(e) for e in cond.expressions]
                branch_keys = [k for ok, k in lits if ok]
                column = column or _col(cond.this)
                seen_in.add(id(cond))
            elif isinstance(cond, exp.Like):
                ok, k = _lit(cond.expression)
                if ok:
                    branch_keys = [k]
                    column = column or _col(cond.this)
            for k in branch_keys:
                keys.append(k)
                pairs.append([k, v])
        parent = case.parent
        name = parent.alias if isinstance(parent, exp.Alias) and parent.alias else column
        out.append(_finding(case, "sql_case", "mapping", len(ifs), keys, pairs, name, line_offset))

    for node in tree.find_all(exp.In):
        if id(node) in seen_in or node.args.get("query") or node.args.get("unnest"):
            continue
        lits = [_lit(e) for e in node.expressions]
        values = [v for ok, v in lits if ok]
        if len(values) > cfg.min_list_items and len(values) / max(len(node.expressions), 1) >= 0.9:
            out.append(_finding(node, "sql_in_list", "code_list", len(values), values, None, _col(node.this), line_offset))

    for values_node in tree.find_all(exp.Values):
        rows = [r for r in values_node.expressions if isinstance(r, exp.Tuple)]
        if len(rows) < cfg.min_inline_rows:
            continue
        parsed_rows = []
        for r in rows:
            lits = [_lit(e) for e in r.expressions]
            if not lits or sum(ok for ok, _ in lits) / len(lits) < 0.8:
                parsed_rows = []
                break
            parsed_rows.append([v for ok, v in lits if ok])
        if not parsed_rows:
            continue
        insert = values_node.find_ancestor(exp.Insert)
        if insert is not None:
            pattern = "sql_insert_values"
            target = insert.this
            name = (target.find(exp.Table) or target).sql() if target is not None else None
        else:
            pattern = "sql_values"
            name = values_node.alias or None
        widths = {len(r) for r in parsed_rows}
        pairs = [[r[0], r[1]] for r in parsed_rows] if widths == {2} else []
        shape = "mapping" if pairs else ("code_list" if widths == {1} else "inline_table")
        out.append(_finding(values_node, pattern, shape, len(parsed_rows), [r[0] for r in parsed_rows], pairs, name, line_offset))
    return out


# --------------------------------------------------------------------------- #
# Regex fallback
# --------------------------------------------------------------------------- #
_LIT = r"(?:'(?:[^']|'')*'|-?\d+(?:\.\d+)?)"
_IN_RE = re.compile(r"(\w+)\s+(?:NOT\s+)?IN\s*\(\s*(" + _LIT + r"(?:\s*,\s*" + _LIT + r")+)\s*\)", re.I)
_CASE_RE = re.compile(r"\bCASE\b(.*?)\bEND\b(?:\s+(?:AS\s+)?([A-Za-z_]\w*))?", re.I | re.S)
_WHEN_RE = re.compile(r"\bWHEN\b\s+(?:\w+(?:\.\w+)?\s*=\s*)?(" + _LIT + r")?.*?\bTHEN\b\s+(" + _LIT + r")?", re.I | re.S)
_LIT_ITEM_RE = re.compile(_LIT)


def _unquote(v: str) -> str:
    v = v.strip()
    if v.startswith("'") and v.endswith("'"):
        v = v[1:-1].replace("''", "'")
    return v


def _scan_regex(sql: str, cfg: ScanConfig, line_offset: int) -> List[CodeFinding]:
    out: List[CodeFinding] = []

    def line_at(pos: int) -> int:
        return sql.count("\n", 0, pos) + line_offset

    for m in _IN_RE.finditer(sql):
        values = [_unquote(v) for v in _LIT_ITEM_RE.findall(m.group(2))]
        if len(values) > cfg.min_list_items:
            out.append(CodeFinding("", "", "sql", line_at(m.start()), "sql_in_list", "code_list", len(values),
                                   name=m.group(1), values=values, snippet=re.sub(r"\s+", " ", m.group(0))[:240]))
    for m in _CASE_RE.finditer(sql):
        body = m.group(1)
        whens = list(_WHEN_RE.finditer(body))
        if len(whens) >= cfg.min_case_branches:
            pairs = [[_unquote(w.group(1)), _unquote(w.group(2) or "")] for w in whens if w.group(1)]
            alias = m.group(2) if m.group(2) and m.group(2).upper() not in ("FROM", "WHERE", "GROUP", "ORDER", "AND", "OR", "WHEN", "ELSE", "END", "THEN") else None
            out.append(CodeFinding("", "", "sql", line_at(m.start()), "sql_case", "mapping", len(whens),
                                   name=alias, values=[p[0] for p in pairs], pairs=pairs,
                                   snippet=re.sub(r"\s+", " ", m.group(0))[:240]))
    return out


def _split_statements(sql: str) -> List[Tuple[str, int]]:
    """Split on semicolons outside quotes; return (statement, start_line)."""
    out, buf, line, start_line, quote = [], [], 1, 1, None
    for ch in sql:
        if quote:
            if ch == quote:
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
        elif ch == ";":
            out.append(("".join(buf), start_line))
            buf, start_line = [], line
            continue
        buf.append(ch)
        if ch == "\n":
            line += 1
    if "".join(buf).strip():
        out.append(("".join(buf), start_line))
    return out


def scan_sql(sql: str, cfg: ScanConfig, dialect: str = "databricks", line_offset: int = 1) -> Tuple[List[CodeFinding], Optional[str]]:
    """Scan SQL text. Returns (findings, warning)."""
    prepared = _prepare(sql)
    if not _HAS_SQLGLOT:  # pragma: no cover
        return _scan_regex(prepared, cfg, line_offset), "sqlglot not installed; used regex fallback"
    findings: List[CodeFinding] = []
    fallback_used = False
    for stmt, start in _split_statements(prepared):
        if not stmt.strip():
            continue
        offset = line_offset + start - 1
        try:
            trees = sqlglot.parse(stmt, dialect=dialect, error_level=ErrorLevel.RAISE)
            for tree in trees:
                if tree is not None:
                    findings.extend(_scan_tree(tree, cfg, offset))
        except Exception:
            fallback_used = True
            findings.extend(_scan_regex(stmt, cfg, offset))
    return findings, ("Some SQL could not be parsed; regex fallback used" if fallback_used else None)
