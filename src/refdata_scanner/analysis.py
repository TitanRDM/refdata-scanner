"""Turn raw findings into an assessment: noise filtering, duplicate and drift
detection, file usage cross-referencing and scoring."""
from __future__ import annotations

import fnmatch
import hashlib
import re
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional, Set

from .models import (
    REFDATA_NAME_HINTS,
    CodeFinding,
    DuplicateGroup,
    FileAsset,
    ScanConfig,
    TableAsset,
)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CODE_RE = re.compile(r"^[A-Z0-9][A-Z0-9_\-./]{0,7}$")
_COLUMNISH_NAMES = re.compile(r"(^|_)(cols?|columns?|fields?|select(ed)?|keep|drop|exclude|include|schema|order_by|group_by|partition|features?|attrs?|headers?)(_|$|s$)", re.I)
_CONFIGISH_NAMES = re.compile(r"(^|_)(conf|config|configs|options?|opts|settings|params?|kwargs|args|properties|props|secrets?|creds?|credentials|env|tags|spark_conf|metadata|payload|request|response|json)(_|$)", re.I)


def norm_value(v: str) -> str:
    return re.sub(r"\s+", " ", str(v)).strip().strip("'\"").lower()


_HINTS = {h.strip("_").lower() for h in REFDATA_NAME_HINTS}
_TOKEN_SPLIT = re.compile(r"[^A-Za-z0-9]+|(?<=[a-z])(?=[A-Z])")


def name_tokens(name: str) -> List[str]:
    return [t.lower() for t in _TOKEN_SPLIT.split(name) if t]


def has_name_hint(*names: Optional[str]) -> bool:
    """True if a name contains a word typical of reference data (map, lookup, codes, region ...)."""
    for n in names:
        if not n:
            continue
        for tok in name_tokens(n):
            singular = tok[:-3] + "y" if tok.endswith("ies") else tok[:-1] if tok.endswith("s") and len(tok) > 3 else tok
            if tok in _HINTS or singular in _HINTS:
                return True
            if tok.startswith(("lkp", "lu", "ref", "dim", "map", "xref")) and len(tok) <= 6:
                return True
    return False


# --------------------------------------------------------------------------- #
# Noise filtering
# --------------------------------------------------------------------------- #

def flag_noise(findings: Iterable[CodeFinding], known_columns: Set[str]) -> None:
    """Mark findings that are probably column lists or configuration, not reference data."""
    known = {c.lower() for c in known_columns}
    for f in findings:
        name = (f.name or "").split("[")[0]
        vals = f.values
        ident_share = sum(bool(_IDENT_RE.match(v)) for v in vals) / len(vals) if vals else 0
        col_share = sum(v.lower() in known for v in vals) / len(vals) if vals and known else 0
        if f.shape == "code_list" and col_share >= 0.6:
            f.likely_noise = f"{int(col_share * 100)}% of values are column names in the catalog"
        elif f.shape == "code_list" and ident_share >= 0.9 and _COLUMNISH_NAMES.search(name):
            f.likely_noise = "looks like a list of column names"
        elif f.pattern == "python_replace_map" and name.lower() in ("columns", "mapper") and ident_share >= 0.9:
            f.likely_noise = "looks like a column rename"
        elif f.shape == "mapping" and f.pattern.startswith("python_dict") and _CONFIGISH_NAMES.search(name):
            f.likely_noise = "looks like configuration settings"
        elif f.shape == "code_list" and _is_numeric_sequence(vals):
            f.likely_noise = "a numeric sequence (better generated than stored)"


def _is_numeric_sequence(vals: List[str]) -> bool:
    try:
        nums = [float(v) for v in vals]
    except ValueError:
        return False
    if len(nums) < 3:
        return False
    diffs = {round(b - a, 9) for a, b in zip(nums, nums[1:])}
    return len(diffs) == 1


# --------------------------------------------------------------------------- #
# Fingerprinting, duplicates and drift
# --------------------------------------------------------------------------- #

def fingerprint(values: Iterable[str]) -> str:
    key = "\x1f".join(sorted({norm_value(v) for v in values}))
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def find_duplicates(findings: List[CodeFinding], threshold: float) -> List[DuplicateGroup]:
    """Group findings whose value sets are identical or nearly so (Jaccard >= threshold)."""
    candidates = [f for f in findings if not f.likely_noise and len(set(f.values)) >= 3]
    sets = [frozenset(norm_value(v) for v in f.values) for f in candidates]

    parent = list(range(len(candidates)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    # Inverted index so we only compare findings that share at least one value.
    index: Dict[str, List[int]] = defaultdict(list)
    for i, s in enumerate(sets):
        for v in s:
            index[v].append(i)
    compared: Set[tuple] = set()
    for i, s in enumerate(sets):
        overlap = Counter()
        for v in s:
            postings = index[v]
            if len(postings) > 500:  # very common value ("y", "1"): not discriminating
                continue
            for j in postings:
                if j > i:
                    overlap[j] += 1
        for j, inter in overlap.items():
            if (i, j) in compared:
                continue
            compared.add((i, j))
            union_size = len(s) + len(sets[j]) - inter
            if union_size and inter / union_size >= threshold:
                union(i, j)

    members: Dict[int, List[int]] = defaultdict(list)
    for i in range(len(candidates)):
        members[find(i)].append(i)

    clusters = []
    for idxs in members.values():
        group_findings = [candidates[i] for i in idxs]
        # A group only counts if it spans more than one place in the code.
        if len({(f.source_path, f.line) for f in group_findings}) < 2:
            continue
        clusters.append(group_findings)
    clusters.sort(key=lambda g: (-len(g), -max(f.item_count for f in g)))

    groups: List[DuplicateGroup] = []
    for n, group_findings in enumerate(clusters, 1):
        gid = f"G{n:03d}"
        fps = {fingerprint(f.values + [f"{k}\x1e{v}" for k, v in f.pairs]) for f in group_findings}
        drift_ids, diffs = _drift(group_findings)
        conflicting = bool(drift_ids)
        names = Counter(f.name for f in group_findings if f.name)
        groups.append(DuplicateGroup(
            group_id=gid,
            finding_ids=[f.id for f in group_findings],
            locations=sorted({f"{f.source_path}:{f.line}" for f in group_findings}),
            item_count=max(f.item_count for f in group_findings),
            exact=len(fps) == 1 and not conflicting,
            conflicting=conflicting,
            label=names.most_common(1)[0][0] if names else "",
            differences=[f"{k}: " + " vs ".join(sorted(v)) for k, v in sorted(diffs.items())],
        ))
        for f in group_findings:
            f.group_id = gid
            f.group_conflict = f.id in drift_ids
    return groups


def _drift(group: List[CodeFinding]) -> tuple:
    """Find copies of the same mapping that disagree on a minority of codes (drift).

    Returns (ids of findings involved, {code: set of differing values}).
    Two mappings that disagree on most codes are different mappings of the same code
    set (state -> name vs state -> region): duplication, but not drift.
    """
    maps = [(f, {norm_value(k): str(v).strip() for k, v in f.pairs}) for f in group if f.pairs]
    display = {norm_value(k): str(k).strip() for f in group for k, _ in f.pairs}
    involved: Set[str] = set()
    diffs: Dict[str, Set[str]] = defaultdict(set)
    for i in range(len(maps)):
        for j in range(i + 1, len(maps)):
            (fa, a), (fb, b) = maps[i], maps[j]
            shared = set(a) & set(b)
            if len(shared) < 3:
                continue
            differ = [k for k in shared if norm_value(a[k]) != norm_value(b[k])]
            if 0 < len(differ) <= len(shared) / 2:
                involved.update((fa.id, fb.id))
                for k in differ:
                    diffs[display.get(k, k)].update((a[k], b[k]))
    return involved, diffs


# --------------------------------------------------------------------------- #
# File usage
# --------------------------------------------------------------------------- #

def norm_path(p: str) -> str:
    p = p.strip().replace("\\", "/")
    p = re.sub(r"^dbfs:", "", p, flags=re.I)
    p = re.sub(r"^/dbfs(?=/)", "", p, flags=re.I)
    p = re.sub(r"^file:", "", p, flags=re.I)
    p = re.sub(r"^/Workspace(?=/)", "", p)
    return p.rstrip("/").lower()


def link_file_references(files: List[FileAsset], refs: Dict[str, List[str]]) -> None:
    """refs maps a referenced path to the code locations that mention it."""
    if not files or not refs:
        return
    by_name: Dict[str, List[FileAsset]] = defaultdict(list)
    norm_files = [(norm_path(f.path), f) for f in files]
    for np_, f in norm_files:
        by_name[np_.rsplit("/", 1)[-1]].append(f)
    for ref, where in refs.items():
        nref = norm_path(ref)
        matched: List[FileAsset] = []
        if any(ch in nref for ch in "*?["):
            matched = [f for np_, f in norm_files if fnmatch.fnmatch(np_, nref) or fnmatch.fnmatch(np_, "*" + nref.lstrip("."))]
        else:
            matched = [f for np_, f in norm_files if np_ == nref or np_.endswith("/" + nref.lstrip("./")) or (nref.startswith("/") and nref.endswith(np_))]
            if not matched and "/" not in nref.strip("./"):
                matched = by_name.get(nref.strip("./"), [])
        for f in matched:
            for w in where:
                if w not in f.referenced_by:
                    f.referenced_by.append(w)


# --------------------------------------------------------------------------- #
# Scoring
# --------------------------------------------------------------------------- #

def band(score: int) -> str:
    return "High" if score >= 60 else "Medium" if score >= 35 else "Low"


def score_code_finding(f: CodeFinding, group_sizes: Dict[str, int]) -> None:
    reasons: List[str] = []
    score = {"mapping": 35, "inline_table": 35, "code_list": 20}.get(f.shape, 20)
    reasons.append({"mapping": "hardcoded mapping", "inline_table": "hardcoded table", "code_list": "hardcoded list"}.get(f.shape, f.shape))
    size_pts = min(20, f.item_count // 2)
    score += size_pts
    if f.item_count >= 20:
        reasons.append(f"{f.item_count} items")
    if has_name_hint(f.name):
        score += 15
        reasons.append("reference-data style name")
    codes = [v for v in f.values if _CODE_RE.match(v)]
    if f.values and len(codes) / len(f.values) >= 0.7:
        score += 5
        reasons.append("values look like codes")
    if f.group_id and group_sizes.get(f.group_id, 0) > 1:
        score += 15
        reasons.append(f"repeated in {group_sizes[f.group_id]} places")
    if f.group_conflict:
        score += 10
        reasons.append("copies disagree")
    if f.in_job:
        score += 5
        reasons.append("runs in a scheduled job")
    if f.likely_noise:
        score = min(score, 10)
        reasons = [f.likely_noise]
    f.score = max(0, min(100, score))
    f.reasons = reasons


def score_file(f: FileAsset) -> None:
    reasons: List[str] = []
    score = 40
    if f.is_dbt_seed:
        score += 25
        reasons.append("dbt seed (reference data by definition)")
    if has_name_hint(f.name):
        score += 15
        reasons.append("reference-data style name")
    if f.area in ("workspace", "dbfs") or "/users/" in f.path.lower() or "/filestore/" in f.path.lower():
        score += 10
        reasons.append("manual upload location")
    if f.referenced_by:
        score += 15
        reasons.append(f"read by {len(f.referenced_by)} code location(s)")
    if f.loaded_into:
        score += 15
        reasons.append(f"loaded into {len(f.loaded_into)} table(s)")
    if f.size_bytes is not None:
        if f.size_bytes <= 5 * 1024 * 1024:
            score += 10
            reasons.append("small file")
        elif f.size_bytes > 200 * 1024 * 1024:
            score -= 25
            reasons.append("large file (probably a data extract)")
    if f.extension in (".xlsx", ".xls", ".xlsm", ".xlsb"):
        score += 5
        reasons.append("spreadsheet (likely maintained by hand)")
    if f.row_estimate is not None and f.row_estimate <= 5000:
        score += 5
    f.score = max(0, min(100, score))
    f.reasons = reasons


def score_table(t: TableAsset, cfg: ScanConfig) -> None:
    reasons: List[str] = []
    score = 15
    if has_name_hint(t.name):
        score += 25
        reasons.append("reference-data style name")
    if t.comment and has_name_hint(t.comment):
        score += 5
        reasons.append("description mentions reference data")
    if t.column_count is not None:
        if t.column_count <= 4:
            score += 20
            reasons.append(f"only {t.column_count} columns")
        elif t.column_count <= cfg.max_lookup_table_columns:
            score += 10
            reasons.append(f"{t.column_count} columns")
        elif t.column_count > 40:
            score -= 20
    if t.loaded_from_files:
        score += 25
        reasons.append("loaded from CSV/Excel")
    if t.size_bytes is not None:
        if t.size_bytes <= cfg.max_lookup_table_bytes:
            score += 10
            reasons.append("small table")
        else:
            score -= 30
            reasons.append("large table")
    if t.row_count is not None and t.row_count > 1_000_000:
        score -= 30
    t.score = max(0, min(100, score))
    t.reasons = reasons
