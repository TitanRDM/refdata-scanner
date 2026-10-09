"""Run a scan: collect assets from an adapter, analyse them, return a ScanResult."""
from __future__ import annotations

import datetime as _dt
from collections import Counter, defaultdict
from typing import Dict, List, Optional

from .adapters.base import Logger, PlatformAdapter
from .analysis import (
    find_duplicates,
    fingerprint,
    flag_noise,
    link_file_references,
    norm_path,
    score_code_finding,
    score_file,
    score_table,
)
from .inspector import inspect_file
from .models import FileAsset, ScanConfig, ScanResult
from .scanners import scan_code_asset

# Only tables at or above this score are kept in the results.
TABLE_CANDIDATE_MIN_SCORE = 35


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat()


def run_scan(adapter: PlatformAdapter, cfg: Optional[ScanConfig] = None, log: Optional[Logger] = None) -> ScanResult:
    cfg = cfg or ScanConfig()
    log = log or adapter.log
    result = ScanResult(platform=adapter.platform, scope=adapter.describe_scope(), started_at=_now(),
                        run_by=adapter.run_by())

    # 1. Tables (also gives us known column names for noise filtering) -------
    log("Listing tables ...")
    tables_seen = 0
    known_columns = set()
    candidate_tables = []
    for t in adapter.iter_tables():
        tables_seen += 1
        known_columns.update(t.columns)
        candidate_tables.append(t)

    # 2. Files ------------------------------------------------------------------
    log("Looking for CSV and Excel files ...")
    files = list(adapter.iter_files(tuple(cfg.file_extensions)))
    log(f"  found {len(files)} file(s)")
    if cfg.inspect_file_contents:
        log("Reading file headers ...")
        for f in files:
            inspect_file(f, adapter.read_file_head(f, cfg.max_inspect_bytes))

    # 3. Code ---------------------------------------------------------------------
    log("Scanning notebooks, code files, queries and views ...")
    code_assets = 0
    assets_with_findings = set()
    refs: Dict[str, List[str]] = defaultdict(list)
    findings = []
    for asset in adapter.iter_code():
        code_assets += 1
        out = scan_code_asset(asset, cfg, adapter.dialect)
        result.warnings.extend(out.warnings)
        for ref in out.file_refs:
            refs[ref.path].append(f"{asset.path}:{ref.line}")
        if asset.kind == "query_history":
            # Ad hoc history is used only to see which files get read; its literals
            # would double count code that is already scanned at its source.
            out.findings = []
        if out.findings:
            assets_with_findings.add(asset.path)
        findings.extend(out.findings)
        if code_assets % 200 == 0:
            log(f"  scanned {code_assets} code assets ...")
    log(f"  scanned {code_assets} code asset(s), {len(findings)} raw finding(s)")

    # 4. Analysis -------------------------------------------------------------------
    log("Analysing ...")
    for n, f in enumerate(findings, 1):
        f.id = f"C{n:05d}"
        f.fingerprint = fingerprint(f.values) if f.values else ""
    flag_noise(findings, known_columns)
    groups = find_duplicates(findings, cfg.similarity_threshold)
    group_sizes = {g.group_id: len(g.locations) for g in groups}
    by_id = {f.id: f for f in findings}
    for g in groups:
        sample_finding = by_id[g.finding_ids[0]]
        g.sample = "" if cfg.redact else sample_finding.sample(cfg.sample_values)
    for f in findings:
        score_code_finding(f, group_sizes)

    link_file_references(files, refs)
    loads = adapter.file_loads()
    if loads:
        _apply_loads(files, candidate_tables, loads)
    for f in files:
        score_file(f)
    for t in candidate_tables:
        score_table(t, cfg)

    result.files = sorted(files, key=lambda f: -f.score)
    result.tables = sorted([t for t in candidate_tables if t.score >= TABLE_CANDIDATE_MIN_SCORE], key=lambda t: -t.score)
    result.code_findings = sorted(findings, key=lambda f: -f.score)
    result.duplicate_groups = groups
    result.warnings.extend(adapter.warnings)

    real = [f for f in findings if not f.likely_noise]
    unique_sets = len({f.group_id or f.id for f in real if f.score >= 35})
    result.stats = {
        "tables_scanned": tables_seen,
        "code_assets_scanned": code_assets,
        "code_assets_with_findings": len({f.source_path for f in real}),
        "files_found": len(files),
        "files_by_extension": dict(Counter(f.extension for f in files)),
        "files_referenced_by_code": sum(1 for f in files if f.referenced_by),
        "files_loaded_into_tables": sum(1 for f in files if f.loaded_into),
        "dbt_seeds": sum(1 for f in files if f.is_dbt_seed),
        "lookup_table_candidates": len(result.tables),
        "code_findings": len(real),
        "code_findings_filtered_as_noise": len(findings) - len(real),
        "code_findings_by_pattern": dict(Counter(f.pattern for f in real)),
        "hardcoded_values_total": sum(f.item_count for f in real),
        "duplicate_groups": len(groups),
        "conflicting_groups": sum(1 for g in groups if g.conflicting),
        "duplicated_findings": sum(len(g.finding_ids) for g in groups),
        "estimated_reference_sets": unique_sets + len([f for f in files if f.score >= 50]) + len(result.tables),
        "high_priority": sum(1 for x in list(real) + files + result.tables if x.score >= 60),
    }
    result.finished_at = _now()
    log("Scan complete.")
    return result


def _apply_loads(files, tables, loads: Dict[str, List[str]]) -> None:
    file_index = {norm_path(f.path): f for f in files}
    table_index = {t.full_name.lower(): t for t in tables}
    for path, targets in loads.items():
        np_ = norm_path(path)
        f = file_index.get(np_)
        if f is None:
            # Known only from lineage: typically a file in an external location or one since deleted.
            name = path.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
            ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
            if ext.endswith(".gz"):
                ext = "." + name[:-3].rsplit(".", 1)[-1].lower()
            f = FileAsset(path=path, name=name, extension=ext, area="lineage", container=path.rsplit("/", 1)[0])
            files.append(f)
            file_index[np_] = f
        for target in targets:
            if f is not None and target not in f.loaded_into:
                f.loaded_into.append(target)
            t = table_index.get(target.lower())
            if t is not None and path not in t.loaded_from_files:
                t.loaded_from_files.append(path)
