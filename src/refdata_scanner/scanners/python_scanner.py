"""Find hardcoded reference data in Python code using the ``ast`` module.

Detects:

* dict literals (code -> label mappings, or code -> record "tables")
* list / set / tuple literals of constants (code lists)
* lists of tuples/dicts (inline tables), e.g. ``spark.createDataFrame([...])``
* ``.isin([...])`` filters and ``.replace({...})`` / ``.map({...})`` mappings
* chained ``F.when(...).when(...)`` expressions (the PySpark CASE statement)
* SQL embedded in ``spark.sql("...")`` calls (returned for the SQL scanner)
"""
from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from ..models import CodeFinding, ScanConfig

_CONST_TYPES = (str, int, float, bool)
_SQL_CALL_NAMES = {"sql"}
_DF_CALL_NAMES = {"createDataFrame", "DataFrame", "from_records", "from_dict", "createOrReplaceTempView"}
_MAP_CALL_NAMES = {"replace", "map", "rename", "fillna", "na_replace"}


@dataclass
class EmbeddedSql:
    sql: str
    line: int


def _const(node: ast.AST) -> Tuple[bool, Any]:
    if isinstance(node, ast.Constant) and isinstance(node.value, _CONST_TYPES):
        return True, node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub) and isinstance(node.operand, ast.Constant):
        if isinstance(node.operand.value, (int, float)):
            return True, -node.operand.value
    return False, None


def _norm(v: Any) -> str:
    return str(v).strip()


def _call_name(call: ast.Call) -> Optional[str]:
    f = call.func
    if isinstance(f, ast.Attribute):
        return f.attr
    if isinstance(f, ast.Name):
        return f.id
    return None


def _target_name(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        ok, val = _const(node.slice)
        base = _target_name(node.value)
        if ok:
            return f"{base}[{val!r}]" if base else str(val)
        return base
    if isinstance(node, (ast.Tuple, ast.List)) and node.elts:
        return _target_name(node.elts[0])
    return None


class _Visitor(ast.NodeVisitor):
    def __init__(self, source: str, line_offset: int, cfg: ScanConfig):
        self.source = source
        self.line_offset = line_offset
        self.cfg = cfg
        self.findings: List[CodeFinding] = []
        self.embedded_sql: List[EmbeddedSql] = []
        self.parents: Dict[int, ast.AST] = {}
        self.done: set = set()
        self.func_stack: List[str] = []

    # -- helpers ----------------------------------------------------------- #
    def _line(self, node: ast.AST) -> int:
        return getattr(node, "lineno", 1) + self.line_offset - 1

    def _snippet(self, node: ast.AST) -> str:
        try:
            seg = ast.get_source_segment(self.source, node) or ""
        except Exception:  # pragma: no cover - defensive
            seg = ""
        seg = re.sub(r"\s+", " ", seg).strip()
        return seg[:240] + ("..." if len(seg) > 240 else "")

    def _context(self, node: ast.AST) -> Tuple[Optional[str], Optional[str]]:
        """Return (name, enclosing_call_name) for a literal node."""
        child, parent = node, self.parents.get(id(node))
        call_name = None
        hops = 0
        while parent is not None and hops < 6:
            hops += 1
            if isinstance(parent, ast.Call) and call_name is None:
                call_name = _call_name(parent)
            if isinstance(parent, ast.Assign):
                return _target_name(parent.targets[0]), call_name
            if isinstance(parent, (ast.AnnAssign, ast.AugAssign)):
                return _target_name(parent.target), call_name
            if isinstance(parent, ast.keyword):
                return parent.arg, call_name
            if isinstance(parent, ast.Return) and self.func_stack:
                return f"{self.func_stack[-1]}()", call_name
            if isinstance(parent, ast.Dict) and child in parent.values:
                idx = parent.values.index(child)
                key = parent.keys[idx]
                ok, val = _const(key) if key is not None else (False, None)
                if ok:
                    return str(val), call_name
            if isinstance(parent, (ast.stmt,)):
                break
            child, parent = parent, self.parents.get(id(parent))
        return None, call_name

    def _mark(self, node: ast.AST) -> None:
        for sub in ast.walk(node):
            self.done.add(id(sub))

    def _add(self, node, pattern, shape, count, values, pairs=None, name=None) -> None:
        self.findings.append(
            CodeFinding(
                source_path="", source_kind="", language="python",
                line=self._line(node), pattern=pattern, shape=shape, item_count=count,
                name=name, values=values, pairs=pairs or [], snippet=self._snippet(node),
            )
        )
        self._mark(node)

    # -- traversal --------------------------------------------------------- #
    def generic_visit(self, node: ast.AST) -> None:
        for child in ast.iter_child_nodes(node):
            self.parents[id(child)] = node
        super().generic_visit(node)

    def visit_FunctionDef(self, node):  # noqa: N802
        self.func_stack.append(node.name)
        self.generic_visit(node)
        self.func_stack.pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        if id(node) in self.done:
            return
        name = _call_name(node)
        # Embedded SQL: spark.sql("..."), session.sql("""...""")
        if name in _SQL_CALL_NAMES and node.args:
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                self.embedded_sql.append(EmbeddedSql(arg.value, self._line(arg)))
            elif isinstance(arg, ast.JoinedStr):
                parts = [v.value if isinstance(v, ast.Constant) else "x" for v in arg.values]
                self.embedded_sql.append(EmbeddedSql("".join(str(p) for p in parts), self._line(arg)))
        # PySpark CASE: F.when(...).when(...).otherwise(...)
        if name in ("when", "otherwise"):
            chain = self._when_chain(node)
            if chain is not None:
                return
        self.generic_visit(node)

    def _when_chain(self, node: ast.Call) -> Optional[bool]:
        """Report a .when() chain from its outermost call. Returns True if handled."""
        whens: List[ast.Call] = []
        cur: Any = node
        while isinstance(cur, ast.Call) and _call_name(cur) in ("when", "otherwise"):
            if _call_name(cur) == "when":
                whens.append(cur)
            f = cur.func
            cur = f.value if isinstance(f, ast.Attribute) else None
        if len(whens) < self.cfg.min_when_chain:
            return None
        pairs, keys, column = [], [], None
        for w in reversed(whens):
            if len(w.args) < 2:
                continue
            cond, val = w.args[0], w.args[1]
            ok_v, v = _const(val)
            key = None
            if isinstance(cond, ast.Compare) and len(cond.comparators) == 1:
                ok_k, k = _const(cond.comparators[0])
                if ok_k:
                    key = k
                    column = column or self._col_name(cond.left)
            elif isinstance(cond, ast.Call) and _call_name(cond) == "isin":
                consts = [c for c in (_const(a) for a in cond.args) if c[0]]
                if cond.args and isinstance(cond.args[0], (ast.List, ast.Tuple)):
                    consts = [c for c in (_const(a) for a in cond.args[0].elts) if c[0]]
                key = "|".join(_norm(c[1]) for c in consts) or None
            if key is not None:
                keys.append(_norm(key))
                pairs.append([_norm(key), _norm(v) if ok_v else self._snippet(val)])
        name = self._spark_output_name(node) or self._context(node)[0]
        self._add(node, "python_when_chain", "mapping", len(whens), keys, pairs, name or column)
        return True

    def _spark_output_name(self, node: ast.AST) -> Optional[str]:
        """Name of the column an expression produces: withColumn("x", expr) or expr.alias("x")."""
        parent = self.parents.get(id(node))
        if isinstance(parent, ast.Call) and _call_name(parent) == "withColumn" and len(parent.args) >= 2 and parent.args[1] is node:
            ok, v = _const(parent.args[0])
            return str(v) if ok else None
        if isinstance(parent, ast.Attribute) and parent.attr in ("alias", "name"):
            call = self.parents.get(id(parent))
            if isinstance(call, ast.Call) and call.args:
                ok, v = _const(call.args[0])
                return str(v) if ok else None
        return None

    @staticmethod
    def _col_name(node: ast.AST) -> Optional[str]:
        # F.col("x"), col("x"), df["x"], df.x
        if isinstance(node, ast.Call) and node.args:
            ok, v = _const(node.args[0])
            if ok:
                return str(v)
        if isinstance(node, ast.Subscript):
            ok, v = _const(node.slice)
            if ok:
                return str(v)
        if isinstance(node, ast.Attribute):
            return node.attr
        if isinstance(node, ast.Name):
            return node.id
        return None

    def visit_Dict(self, node: ast.Dict) -> None:  # noqa: N802
        if id(node) in self.done:
            return
        n = len(node.keys)
        if n >= self.cfg.min_mapping_items and all(k is not None for k in node.keys):
            key_consts = [_const(k) for k in node.keys]
            const_keys = [v for ok, v in key_consts if ok]
            if len(const_keys) / n >= 0.8:
                val_consts = [_const(v) for v in node.values]
                name, call = self._context(node)
                pattern = "python_replace_map" if call in _MAP_CALL_NAMES else "python_dict"
                if sum(ok for ok, _ in val_consts) / n >= 0.8:
                    pairs = [[_norm(k), _norm(v)] for (okk, k), (okv, v) in zip(key_consts, val_consts) if okk and okv]
                    self._add(node, pattern, "mapping", n, [_norm(k) for k in const_keys], pairs, name)
                    return
                # dict of lists: {"Group A": ["x", "y"], ...} -> hierarchy/grouping
                if all(isinstance(v, (ast.List, ast.Tuple, ast.Set)) for v in node.values):
                    pairs = []
                    for (okk, k), v in zip(key_consts, node.values):
                        for ok, member in (_const(e) for e in v.elts):
                            if okk and ok:
                                pairs.append([_norm(member), _norm(k)])
                    self._add(node, "python_dict_of_lists", "mapping", n, [_norm(k) for k in const_keys], pairs, name)
                    return
                # dict of records: {"US": {"name": ..., "ccy": ...}}
                if all(isinstance(v, (ast.Dict, ast.Tuple, ast.List)) for v in node.values):
                    self._add(node, "python_dict_of_records", "inline_table", n, [_norm(k) for k in const_keys], None, name)
                    return
        self.generic_visit(node)

    def _visit_sequence(self, node) -> None:
        if id(node) in self.done:
            return
        elts = node.elts
        n = len(elts)
        if n == 0:
            return
        name, call = self._context(node)
        consts = [_const(e) for e in elts]
        n_const = sum(ok for ok, _ in consts)
        kind = {ast.List: "list", ast.Set: "set", ast.Tuple: "tuple"}[type(node)]
        # Code list: mostly constants
        if n > self.cfg.min_list_items and n_const / n >= 0.9:
            pattern = "python_isin_list" if call == "isin" else f"python_{kind}"
            self._add(node, pattern, "code_list", n, [_norm(v) for ok, v in consts if ok], None, name)
            return
        # Inline table: list of tuples/lists/dicts of constants
        if n >= self.cfg.min_inline_rows and all(isinstance(e, (ast.Tuple, ast.List, ast.Dict)) for e in elts):
            rows = []
            for e in elts:
                if isinstance(e, ast.Dict):
                    row = [_const(v) for v in e.values]
                else:
                    row = [_const(v) for v in e.elts]
                if not row or sum(ok for ok, _ in row) / len(row) < 0.8:
                    rows = []
                    break
                rows.append([_norm(v) for ok, v in row if ok])
            widths = {len(r) for r in rows}
            if rows and max(widths) >= 2 and (n >= self.cfg.min_mapping_items or call in _DF_CALL_NAMES):
                pattern = "python_dataframe_literal" if call in _DF_CALL_NAMES else f"python_{kind}_of_rows"
                pairs = [[r[0], r[1]] for r in rows] if widths == {2} else []
                self._add(node, pattern, "inline_table" if not pairs else "mapping", n, [r[0] for r in rows], pairs, name)
                return
        self.generic_visit(node)

    visit_List = _visit_sequence
    visit_Set = _visit_sequence
    visit_Tuple = _visit_sequence


_MAGIC_LINE_RE = re.compile(r"^\s*[%!]")


def scan_python(source: str, cfg: ScanConfig, line_offset: int = 1) -> Tuple[List[CodeFinding], List[EmbeddedSql], Optional[str]]:
    """Scan Python source. Returns (findings, embedded_sql, error)."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        # Notebook cells often contain %magic or !shell lines: blank them and retry.
        cleaned = "\n".join("" if _MAGIC_LINE_RE.match(l) else l for l in source.split("\n"))
        try:
            tree = ast.parse(cleaned)
            source = cleaned
        except SyntaxError as exc:
            return [], [], f"Python parse error at line {(exc.lineno or 1) + line_offset - 1}: {exc.msg}"
    v = _Visitor(source, line_offset, cfg)
    v.visit(tree)
    return v.findings, v.embedded_sql, None
