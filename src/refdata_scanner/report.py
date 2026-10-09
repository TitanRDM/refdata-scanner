"""Write scan results: inventories (CSV/JSON) and an assessment report (Markdown + HTML)."""
from __future__ import annotations

import csv
import html
import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from .analysis import band
from .models import ScanConfig, ScanResult

TITANRDM_URL = "https://titanrdm.com"
REPO_URL = "https://github.com/TitanRDM/refdata-scanner"

PATTERN_LABELS = {
    "python_dict": "Python dict (code -> label)",
    "python_replace_map": "Python .replace()/.map() mapping",
    "python_dict_of_lists": "Python dict of lists (grouping)",
    "python_dict_of_records": "Python dict of records",
    "python_list": "Python list",
    "python_set": "Python set",
    "python_tuple": "Python tuple",
    "python_isin_list": "Python .isin([...]) filter",
    "python_list_of_rows": "Python list of rows",
    "python_tuple_of_rows": "Python tuple of rows",
    "python_dataframe_literal": "DataFrame built from literals",
    "python_when_chain": "PySpark when() chain",
    "sql_case": "SQL CASE mapping",
    "sql_in_list": "SQL IN (...) list",
    "sql_values": "SQL VALUES inline table",
    "sql_insert_values": "SQL INSERT ... VALUES",
    "embedded_sql_case": "SQL CASE inside spark.sql()",
    "embedded_sql_in_list": "SQL IN list inside spark.sql()",
    "embedded_sql_values": "SQL VALUES inside spark.sql()",
    "embedded_sql_insert_values": "SQL INSERT VALUES inside spark.sql()",
    "view_case": "CASE mapping in a view",
    "view_in_list": "IN list in a view",
    "view_values": "VALUES in a view",
}


def _pattern_label(p: str) -> str:
    return PATTERN_LABELS.get(p, p.replace("_", " "))


class _Redactor:
    def __init__(self, enabled: bool):
        self.enabled = enabled
        self.map: Dict[str, str] = {}

    def person(self, name: Optional[str]) -> str:
        if not name:
            return ""
        if not self.enabled:
            return name
        if name not in self.map:
            self.map[name] = f"user-{len(self.map) + 1:02d}"
        return self.map[name]

    def path(self, p: str) -> str:
        if not self.enabled or not p:
            return p
        # Keep the shape of the path (useful) but hide user names in home folders.
        parts = p.split("/")
        for i, part in enumerate(parts):
            if i > 0 and parts[i - 1].lower() in ("users", "repos") and part:
                parts[i] = self.person(part)
        return "/".join(parts)


def _fmt_size(n: Optional[int]) -> str:
    if n is None:
        return ""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return str(n)


def _n(count: int, singular: str, plural: Optional[str] = None) -> str:
    """'1 file', '3 files'."""
    return f"{count:,} {singular if count == 1 else (plural or singular + 's')}"


def _short_date(iso: Optional[str]) -> str:
    return (iso or "")[:10]


# --------------------------------------------------------------------------- #
# Inventories
# --------------------------------------------------------------------------- #

def _write_csv(path: str, headers: Sequence[str], rows: List[Sequence]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(headers)
        w.writerows(rows)


def write_inventories(result: ScanResult, cfg: ScanConfig, out_dir: str, r: _Redactor) -> Dict[str, str]:
    paths = {}
    p = os.path.join(out_dir, "files.csv")
    _write_csv(p, ["score", "priority", "path", "name", "type", "area", "container", "size_bytes", "modified",
                   "owner", "dbt_seed", "referenced_by", "loaded_into", "header", "row_estimate", "sheets", "reasons"], [
        [f.score, band(f.score), r.path(f.path), f.name, f.extension, f.area, r.path(f.container or ""), f.size_bytes or "",
         _short_date(f.modified_at), r.person(f.owner), "yes" if f.is_dbt_seed else "",
         "; ".join(r.path(x) for x in f.referenced_by), "; ".join(f.loaded_into),
         "" if r.enabled else "|".join(f.header), f.row_estimate or "", f.sheet_count or "", "; ".join(f.reasons)]
        for f in result.files])
    paths["files"] = p

    p = os.path.join(out_dir, "tables.csv")
    _write_csv(p, ["score", "priority", "table", "type", "format", "columns", "size_bytes", "owner", "updated",
                   "loaded_from_files", "comment", "reasons"], [
        [t.score, band(t.score), t.full_name, t.table_type or "", t.data_format or "", t.column_count or "",
         t.size_bytes or "", r.person(t.owner), _short_date(t.updated_at), "; ".join(t.loaded_from_files),
         "" if r.enabled else (t.comment or ""), "; ".join(t.reasons)]
        for t in result.tables])
    paths["tables"] = p

    p = os.path.join(out_dir, "code_findings.csv")
    _write_csv(p, ["id", "score", "priority", "source", "line", "kind", "language", "pattern", "shape", "name",
                   "items", "duplicate_group", "copies_disagree", "in_job", "owner", "modified", "sample",
                   "noise_reason", "reasons", "fingerprint"], [
        [f.id, f.score, band(f.score), r.path(f.source_path), f.line, f.source_kind, f.language, f.pattern, f.shape,
         f.name or "", f.item_count, f.group_id or "", "yes" if f.group_conflict else "", "yes" if f.in_job else "",
         r.person(f.owner), _short_date(f.modified_at), "" if r.enabled else f.sample(cfg.sample_values),
         f.likely_noise or "", "; ".join(f.reasons), f.fingerprint]
        for f in result.code_findings])
    paths["code_findings"] = p

    p = os.path.join(out_dir, "duplicate_groups.csv")
    _write_csv(p, ["group", "label", "copies", "max_items", "identical", "copies_disagree", "differences", "locations",
                   "sample"], [
        [g.group_id, g.label, len(g.locations), g.item_count, "yes" if g.exact else "no",
         "yes" if g.conflicting else "no", "" if r.enabled else "; ".join(g.differences),
         "; ".join(r.path(x) for x in g.locations), "" if r.enabled else g.sample]
        for g in result.duplicate_groups])
    paths["duplicate_groups"] = p

    # Full machine-readable output (input for the AI assessment skill).
    data = result.to_dict()
    if r.enabled:
        for f in data["code_findings"]:
            f["values"], f["pairs"], f["snippet"] = [], [], ""
            f["owner"] = r.person(f["owner"])
            f["source_path"] = r.path(f["source_path"])
            f["location"] = r.path(f["location"] or "")
        for g in data["duplicate_groups"]:
            g["differences"], g["sample"] = [], ""
            g["locations"] = [r.path(x) for x in g["locations"]]
        for f in data["files"]:
            f["owner"], f["header"] = r.person(f["owner"]), []
            f["path"], f["container"] = r.path(f["path"]), r.path(f["container"] or "")
            f["referenced_by"] = [r.path(x) for x in f["referenced_by"]]
        for t in data["tables"]:
            t["owner"], t["created_by"], t["comment"] = r.person(t["owner"]), r.person(t["created_by"]), None
        data["run_by"] = r.person(data["run_by"])
    p = os.path.join(out_dir, "findings.json")
    with open(p, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=1, default=str)
    paths["findings"] = p

    # Counts only: safe to share (no names, paths or values).
    p = os.path.join(out_dir, "summary.json")
    with open(p, "w", encoding="utf-8") as fh:
        json.dump({"platform": result.platform, "scanned_at": result.finished_at, "stats": result.stats,
                   "scanner": "refdata-scanner", "scanner_url": REPO_URL}, fh, indent=2)
    paths["summary"] = p
    return paths


# --------------------------------------------------------------------------- #
# Assessment report
# --------------------------------------------------------------------------- #

@dataclass
class _Section:
    title: str
    paragraphs: List[str] = field(default_factory=list)
    bullets: List[str] = field(default_factory=list)
    headers: List[str] = field(default_factory=list)
    rows: List[List[str]] = field(default_factory=list)
    note: str = ""


def _headline(result: ScanResult) -> List[tuple]:
    s = result.stats
    return [
        ("Reference data sets (estimated)", s["estimated_reference_sets"]),
        ("CSV / Excel files", s["files_found"]),
        ("Lookup-table candidates", s["lookup_table_candidates"]),
        ("Hardcoded lists & mappings", s["code_findings"]),
        ("Hardcoded values in code", s["hardcoded_values_total"]),
        ("Sets copied in 2+ places", s["duplicate_groups"]),
        ("Copies that disagree", s["conflicting_groups"]),
    ]


def _key_findings(result: ScanResult, r: _Redactor) -> List[str]:
    s, out = result.stats, []
    if s["code_assets_scanned"]:
        pct = 100 * s["code_assets_with_findings"] / s["code_assets_scanned"]
        out.append(f"**{s['code_assets_with_findings']:,} of {_n(s['code_assets_scanned'], 'code asset')}** "
                   f"(notebooks, files, queries and views; {pct:.0f}%) contain hardcoded reference data, "
                   f"{_n(s['hardcoded_values_total'], 'value')} in total.")
    if s["duplicate_groups"]:
        biggest = result.duplicate_groups[0]
        label = f" (`{biggest.label}`)" if biggest.label else ""
        verb = "is" if s["duplicate_groups"] == 1 else "are"
        out.append(f"**{_n(s['duplicate_groups'], 'reference set')}** {verb} copied into more than one place. "
                   f"The most widespread{label} appears in **{len(biggest.locations)} places**.")
    if s["conflicting_groups"]:
        drifted = next(g for g in result.duplicate_groups if g.conflicting)
        example = ""
        if drifted.differences and not r.enabled:
            example = f" For example, in `{drifted.label or drifted.group_id}`: {drifted.differences[0]}."
        verb = "no longer agrees" if s["conflicting_groups"] == 1 else "no longer agree"
        out.append(f"**{_n(s['conflicting_groups'], 'copied set')}** {verb} with its other copies: the same code "
                   f"maps to a different value depending on which notebook or query you run. This is reference "
                   f"data drift.{example}")
    if s["files_found"]:
        xl = sum(v for k, v in s["files_by_extension"].items() if k.startswith(".xls"))
        line = f"**{_n(s['files_found'], 'CSV/Excel file')}** found in the platform"
        if xl:
            line += f", including {_n(xl, 'spreadsheet')} that {'is' if xl == 1 else 'are'} typically edited by hand"
        line += "."
        used = s["files_referenced_by_code"]
        if used:
            line += (f" {_n(used, 'file')} {'is' if used == 1 else 'are'} read directly by code, so a manual edit "
                     f"flows straight into pipelines.")
        orphan = s["files_found"] - used - s["files_loaded_into_tables"]
        if orphan > 0 and s["code_assets_scanned"]:
            line += f" {_n(orphan, 'file')} {'is' if orphan == 1 else 'are'} not referenced by any scanned code."
        out.append(line)
    if s["files_loaded_into_tables"]:
        out.append(f"Lineage shows **{_n(s['files_loaded_into_tables'], 'file')}** loaded into tables.")
    if s["dbt_seeds"]:
        out.append(f"**{_n(s['dbt_seeds'], 'dbt seed file')}**: reference data kept as CSV in a code repository, "
                   f"changed through pull requests rather than by the people who own it.")
    if s["lookup_table_candidates"]:
        out.append(f"**{_n(s['lookup_table_candidates'], 'small table')}** look like lookup or mapping tables, "
                   f"typically with no clear owner, approval process or version history.")
    real = [f for f in result.code_findings if not f.likely_noise]
    if real:
        largest = max(real, key=lambda f: f.item_count)
        out.append(f"The largest hardcoded set has **{largest.item_count:,} entries** ({_pattern_label(largest.pattern)}"
                   f"{', `' + largest.name + '`' if largest.name else ''}).")
    if not out:
        out.append("No reference data candidates were found in the scanned scope. Try widening the scope "
                   "(more schemas or workspace folders) or lowering the thresholds.")
    return out


def _top_candidates(result: ScanResult, r: _Redactor, n: int = 25) -> List[List[str]]:
    rows = []
    for f in result.files:
        rows.append((f.score, ["File", r.path(f.path), f.extension.lstrip("."), "", "; ".join(f.reasons)]))
    for t in result.tables:
        rows.append((t.score, ["Table", t.full_name, f"{t.column_count or '?'} cols", "", "; ".join(t.reasons)]))
    for c in result.code_findings:
        if c.likely_noise:
            continue
        rows.append((c.score, ["Code", f"{r.path(c.source_path)}:{c.line}", _pattern_label(c.pattern),
                               c.name or "", "; ".join(c.reasons)]))
    rows.sort(key=lambda x: -x[0])
    return [[str(s), band(s)] + row for s, row in rows[:n]]


def _hotspots(result: ScanResult, r: _Redactor) -> tuple:
    by_loc: Dict[str, Counter] = defaultdict(Counter)
    by_owner: Counter = Counter()

    def folder(path: str) -> str:
        parts = [p for p in path.split("/") if p]
        return "/" + "/".join(parts[:3]) if len(parts) > 3 else "/" + "/".join(parts[:-1])

    for c in result.code_findings:
        if c.likely_noise:
            continue
        loc = c.location or folder(c.source_path)
        by_loc[r.path(loc)]["code"] += 1
        if c.owner:
            by_owner[r.person(c.owner)] += 1
    for f in result.files:
        by_loc[r.path(f.container or folder(f.path))]["files"] += 1
        if f.owner:
            by_owner[r.person(f.owner)] += 1
    for t in result.tables:
        by_loc[f"{t.catalog}.{t.schema}"]["tables"] += 1
        if t.owner:
            by_owner[r.person(t.owner)] += 1
    loc_rows = sorted(by_loc.items(), key=lambda kv: -sum(kv[1].values()))[:15]
    return ([[k, str(v["files"]), str(v["tables"]), str(v["code"]), str(sum(v.values()))] for k, v in loc_rows],
            [[k, str(v)] for k, v in by_owner.most_common(10)])


def build_sections(result: ScanResult, cfg: ScanConfig, r: _Redactor) -> List[_Section]:
    secs: List[_Section] = []
    secs.append(_Section("Key findings", bullets=_key_findings(result, r)))

    if result.duplicate_groups:
        rows = []
        for g in result.duplicate_groups[:20]:
            status = "Copies disagree" if g.conflicting else ("Identical copies" if g.exact else "Same codes, different copies")
            locs = [r.path(x) for x in g.locations]
            shown = "<br>".join(locs[:4]) + (f"<br>... +{len(locs) - 4} more" if len(locs) > 4 else "")
            diffs = "" if r.enabled else "<br>".join(g.differences[:4]) + (
                f"<br>... +{len(g.differences) - 4} more" if len(g.differences) > 4 else "")
            rows.append([g.group_id, g.label or "(unnamed)", str(len(g.locations)), str(g.item_count), status, shown,
                         diffs or "-"])
        secs.append(_Section(
            "Duplicated reference data and drift",
            paragraphs=["The same list or mapping hardcoded in several places. Each copy has to be updated by hand when "
                        "the reference data changes, and copies that disagree mean reports and pipelines quietly "
                        "produce different answers."],
            headers=["Group", "Name", "Copies", "Items", "Status", "Locations", "Where copies disagree"], rows=rows,
            note="Full list: duplicate_groups.csv" if len(result.duplicate_groups) > 20 else ""))

    secs.append(_Section("Top candidates for a managed reference data service",
                         paragraphs=["Ranked by how likely each item is to be reference data and how much risk it "
                                     "carries. Scores: 60+ High, 35-59 Medium."],
                         headers=["Score", "Priority", "Type", "Where", "What", "Name", "Why"],
                         rows=_top_candidates(result, r)))

    if result.files:
        rows = [[str(f.score), r.path(f.path), _fmt_size(f.size_bytes), _short_date(f.modified_at),
                 r.person(f.owner), str(len(f.referenced_by)) if f.referenced_by else "-",
                 ", ".join(f.loaded_into[:2]) or "-"] for f in result.files[:40]]
        secs.append(_Section("CSV and Excel files", headers=["Score", "Path", "Size", "Modified", "Owner", "Read by code",
                                                             "Loaded into"], rows=rows,
                             note="Full list: files.csv" if len(result.files) > 40 else ""))

    if result.tables:
        rows = [[str(t.score), t.full_name, str(t.column_count or ""), _fmt_size(t.size_bytes), r.person(t.owner),
                 "; ".join(t.reasons)] for t in result.tables[:40]]
        secs.append(_Section("Lookup-table candidates",
                             paragraphs=["Small, narrow tables with reference-data style names, or tables loaded from "
                                         "spreadsheets."],
                             headers=["Score", "Table", "Columns", "Size", "Owner", "Why"], rows=rows,
                             note="Full list: tables.csv" if len(result.tables) > 40 else ""))

    real = [f for f in result.code_findings if not f.likely_noise]
    if real:
        by_pattern = Counter(f.pattern for f in real)
        secs.append(_Section("Hardcoded reference data by pattern",
                             headers=["Pattern", "Count", "Values"],
                             rows=[[_pattern_label(p), str(n), f"{sum(f.item_count for f in real if f.pattern == p):,}"]
                                   for p, n in by_pattern.most_common()]))
        rows = [[str(f.score), f"{r.path(f.source_path)}:{f.line}", _pattern_label(f.pattern), f.name or "",
                 str(f.item_count), f.group_id or "", "" if r.enabled else f.sample(cfg.sample_values)]
                for f in real[:40]]
        secs.append(_Section("Hardcoded lists and mappings in code",
                             headers=["Score", "Location", "Pattern", "Name", "Items", "Group", "Sample"], rows=rows,
                             note="Full list: code_findings.csv" if len(real) > 40 else ""))

    loc_rows, owner_rows = _hotspots(result, r)
    if loc_rows:
        secs.append(_Section("Hotspots by location", headers=["Location", "Files", "Tables", "Code", "Total"], rows=loc_rows))
    if owner_rows:
        secs.append(_Section("Who maintains it today",
                             paragraphs=["Reference data maintained by individuals, outside any approval workflow."],
                             headers=["Owner / last editor", "Items"], rows=owner_rows))

    secs.append(_Section(
        "What this means",
        bullets=[
            "**Changes need a developer.** Business users cannot update a hardcoded mapping; every change is a code change and a deployment.",
            "**No single source of truth.** Copies drift apart, so the same code means different things in different reports.",
            "**No audit trail.** Spreadsheets and code literals have no approval workflow, effective dates or history of who changed what.",
            "**Hidden risk.** Hand-edited files that feed pipelines directly can break them, or silently change results.",
        ]))
    secs.append(_Section(
        "Next steps",
        bullets=[
            "Pick the high-priority sets, especially those that are duplicated or disagree, and agree one owner for each.",
            "Move them into a managed reference data service with approval workflows, versioning and an API, and replace the hardcoded copies with a lookup.",
            f"[TitanRDM]({TITANRDM_URL}) is a reference data management service built for exactly this: business users "
            f"maintain the data, and your pipelines read it from Databricks, Snowflake or Fabric.",
        ]))

    s = result.stats
    method = [
        f"Scanned {s['tables_scanned']:,} tables, {s['code_assets_scanned']:,} code assets and "
        f"{s['files_found']:,} CSV/Excel files.",
        f"Thresholds: lists with more than {cfg.min_list_items} items; mappings with {cfg.min_mapping_items}+ entries; "
        f"CASE statements with {cfg.min_case_branches}+ branches; inline tables with {cfg.min_inline_rows}+ rows.",
        f"{s['code_findings_filtered_as_noise']} findings were set aside as probable column lists or configuration "
        f"(listed in code_findings.csv with a noise_reason).",
        "Results only include what the account running the scan is allowed to see.",
        "The scanner is read-only, reads metadata and code only (file contents only if you opted in), and makes no "
        "network calls outside your platform.",
    ]
    secs.append(_Section("How this was produced", bullets=method))
    if result.warnings:
        uniq = list(dict.fromkeys(result.warnings))
        secs.append(_Section("Warnings", bullets=[html.escape(w) for w in uniq[:30]] +
                             ([f"... and {len(uniq) - 30} more (see findings.json)"] if len(uniq) > 30 else [])))
    return secs


def _scope_line(result: ScanResult, r: _Redactor) -> str:
    parts = [f"Platform: {result.platform.title()}"]
    for k, v in result.scope.items():
        if v in (None, "", [], {}):
            continue
        if isinstance(v, (list, tuple)):
            v = ", ".join(str(x) for x in v)
        parts.append(f"{k.replace('_', ' ').capitalize()}: {r.path(str(v)) if 'path' in k else v}")
    parts.append(f"Scanned: {result.finished_at[:16].replace('T', ' ')} UTC")
    if result.run_by:
        parts.append(f"Run by: {r.person(result.run_by)}")
    return " | ".join(parts)


# -- Markdown ---------------------------------------------------------------- #

def _md_cell(v: str) -> str:
    return str(v).replace("|", "\\|").replace("\n", " ")


def render_markdown(result: ScanResult, cfg: ScanConfig, r: _Redactor) -> str:
    lines = ["# Reference Data Scan", "", f"_{_scope_line(result, r)}_", "", "## At a glance", "",
             "| Measure | Count |", "|---|---:|"]
    lines += [f"| {k} | {v:,} |" for k, v in _headline(result)]
    lines.append("")
    for sec in build_sections(result, cfg, r):
        lines += [f"## {sec.title}", ""]
        for p in sec.paragraphs:
            lines += [p, ""]
        for b in sec.bullets:
            lines.append(f"- {b}")
        if sec.bullets:
            lines.append("")
        if sec.rows:
            lines.append("| " + " | ".join(sec.headers) + " |")
            lines.append("|" + "|".join("---" for _ in sec.headers) + "|")
            for row in sec.rows:
                lines.append("| " + " | ".join(_md_cell(c).replace("<br>", ", ") for c in row) + " |")
            lines.append("")
        if sec.note:
            lines += [f"_{sec.note}_", ""]
    lines += ["---", f"Generated by [refdata-scanner]({REPO_URL}), an open-source tool from [TitanRDM]({TITANRDM_URL}).", ""]
    return "\n".join(lines)


# -- HTML -------------------------------------------------------------------- #

_CSS = """
:root{--bg:#f7f8fa;--card:#fff;--ink:#1c2230;--muted:#5b6475;--line:#e3e6ec;--accent:#1f5eff;--high:#c2410c;--med:#a16207;--low:#64748b;--warn-bg:#fff4e5}
@media (prefers-color-scheme:dark){:root{--bg:#0f1218;--card:#171b23;--ink:#e6e9ef;--muted:#9aa3b2;--line:#2a303b;--accent:#6b9bff;--high:#fb923c;--med:#facc15;--low:#94a3b8;--warn-bg:#2a2013}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
main{max-width:1180px;margin:0 auto;padding:32px 20px 60px}
h1{font-size:28px;margin:0 0 4px}h2{font-size:19px;margin:36px 0 10px;padding-bottom:6px;border-bottom:1px solid var(--line)}
.scope{color:var(--muted);font-size:13px;margin-bottom:22px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.card .n{font-size:28px;font-weight:700;font-variant-numeric:tabular-nums}.card .l{color:var(--muted);font-size:12.5px}
.card.alert .n{color:var(--high)}
ul{padding-left:20px}li{margin:5px 0}
.tbl{overflow-x:auto;background:var(--card);border:1px solid var(--line);border-radius:10px}
table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--line);vertical-align:top}
th{position:sticky;top:0;background:var(--card);color:var(--muted);font-weight:600;white-space:nowrap}
tr:last-child td{border-bottom:none}td.path{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px;word-break:break-all}
.pill{display:inline-block;padding:1px 8px;border-radius:999px;font-size:11.5px;font-weight:600;border:1px solid currentColor}
.High{color:var(--high)}.Medium{color:var(--med)}.Low{color:var(--low)}
.note{color:var(--muted);font-size:12.5px;margin-top:6px}
.cta{margin-top:36px;background:var(--card);border:1px solid var(--line);border-left:4px solid var(--accent);border-radius:10px;padding:16px 20px}
a{color:var(--accent)}footer{color:var(--muted);font-size:12.5px;margin-top:40px}
code{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.92em}
"""


def _inline_md(text: str) -> str:
    """Tiny Markdown subset: **bold**, `code`, [link](url). Input is escaped first."""
    import re

    t = html.escape(text, quote=False).replace("&lt;br&gt;", "<br>")
    t = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", t)
    t = re.sub(r"`(.+?)`", r"<code>\1</code>", t)
    t = re.sub(r"\[(.+?)\]\((https?://[^)\s]+)\)", r'<a href="\2">\1</a>', t)
    return t


def render_html(result: ScanResult, cfg: ScanConfig, r: _Redactor) -> str:
    out = ["<!doctype html><html lang='en'><head><meta charset='utf-8'>",
           "<meta name='viewport' content='width=device-width,initial-scale=1'>",
           "<title>Reference Data Scan</title><style>", _CSS, "</style></head><body><main>",
           "<h1>Reference Data Scan</h1>", f"<div class='scope'>{html.escape(_scope_line(result, r))}</div>",
           "<div class='cards'>"]
    for label, value in _headline(result):
        alert = " alert" if label.startswith("Copies that disagree") and value else ""
        out.append(f"<div class='card{alert}'><div class='n'>{value:,}</div><div class='l'>{html.escape(label)}</div></div>")
    out.append("</div>")
    for sec in build_sections(result, cfg, r):
        is_cta = sec.title == "Next steps"
        if is_cta:
            out.append("<div class='cta'>")
        out.append(f"<h2>{html.escape(sec.title)}</h2>" if not is_cta else f"<h3>{html.escape(sec.title)}</h3>")
        for p in sec.paragraphs:
            out.append(f"<p>{_inline_md(p)}</p>")
        if sec.bullets:
            out.append("<ul>" + "".join(f"<li>{_inline_md(b)}</li>" for b in sec.bullets) + "</ul>")
        if sec.rows:
            out.append("<div class='tbl'><table><thead><tr>" +
                       "".join(f"<th>{html.escape(h)}</th>" for h in sec.headers) + "</tr></thead><tbody>")
            for row in sec.rows:
                cells = []
                for h, c in zip(sec.headers, row):
                    if h == "Priority":
                        cells.append(f"<td><span class='pill {c}'>{c}</span></td>")
                    elif h in ("Path", "Where", "Location", "Locations", "Table"):
                        cells.append(f"<td class='path'>{_inline_md(c)}</td>")
                    else:
                        cells.append(f"<td>{_inline_md(c)}</td>")
                out.append("<tr>" + "".join(cells) + "</tr>")
            out.append("</tbody></table></div>")
        if sec.note:
            out.append(f"<div class='note'>{html.escape(sec.note)}</div>")
        if is_cta:
            out.append("</div>")
    out.append(f"<footer>Generated by <a href='{REPO_URL}'>refdata-scanner</a>, an open-source tool from "
               f"<a href='{TITANRDM_URL}'>TitanRDM</a>. Detailed inventories are in the CSV files next to this report.</footer>")
    out.append("</main></body></html>")
    return "\n".join(out)


def write_report(result: ScanResult, cfg: ScanConfig, out_dir: str) -> Dict[str, str]:
    os.makedirs(out_dir, exist_ok=True)
    r = _Redactor(cfg.redact)
    paths = write_inventories(result, cfg, out_dir, r)
    p = os.path.join(out_dir, "report.md")
    with open(p, "w", encoding="utf-8") as fh:
        fh.write(render_markdown(result, cfg, r))
    paths["report_md"] = p
    p = os.path.join(out_dir, "report.html")
    with open(p, "w", encoding="utf-8") as fh:
        fh.write(render_html(result, cfg, r))
    paths["report_html"] = p
    return paths
