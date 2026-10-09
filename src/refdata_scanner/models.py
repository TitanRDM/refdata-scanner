"""Data structures shared by the scanners, adapters and report writers.

Adapters (one per platform) produce *assets*: files, code and tables they
found. The core scanners turn assets into *findings*, which are then scored,
grouped and written to the report.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

FILE_EXTENSIONS = (".csv", ".tsv", ".xlsx", ".xls", ".xlsm", ".xlsb")
CODE_EXTENSIONS = (".py", ".sql", ".ipynb")

# Words that suggest a name refers to reference data.
REFDATA_NAME_HINTS = (
    "map", "mapping", "lookup", "lkp", "lu_", "ref", "reference", "code",
    "codes", "category", "categories", "type", "types", "status", "region",
    "country", "countries", "state", "currency", "uom", "unit", "segment",
    "hierarchy", "dim_", "dim", "xref", "crosswalk", "translation", "alias",
    "classification", "group", "tier", "channel", "level", "list", "enum",
    "seed", "master", "taxonomy", "rate", "account", "centre", "center", "department",
    "dept", "division", "product", "brand", "site", "plant", "location", "calendar", "holiday",
    "reason", "grade", "band", "rating", "scheme", "zone", "territory", "market",
)


@dataclass
class ScanConfig:
    """User-tunable settings. Defaults follow the TitanRDM recommendations."""

    # A Python/SQL list must have MORE than this many literal items to count.
    min_list_items: int = 10
    # A dict or other mapping (code -> label) counts from this many entries.
    min_mapping_items: int = 5
    # A CASE statement counts from this many WHEN branches.
    min_case_branches: int = 5
    # An inline table (VALUES rows, createDataFrame rows) counts from this many rows.
    min_inline_rows: int = 3
    # Chained .when(...) calls count from this many branches.
    min_when_chain: int = 5
    # Tables with at most this many columns are lookup-table candidates.
    max_lookup_table_columns: int = 12
    # Tables at or below this size (bytes) are lookup-table candidates (when size is known).
    max_lookup_table_bytes: int = 50 * 1024 * 1024
    # Near-duplicate threshold (Jaccard similarity of value sets).
    similarity_threshold: float = 0.8
    # Read the header row of CSV/Excel files (downloads up to max_inspect_bytes per file).
    inspect_file_contents: bool = False
    max_inspect_bytes: int = 5 * 1024 * 1024
    # How many values to show as a sample in reports. 0 hides values entirely.
    sample_values: int = 5
    # Hide literal values and personal names in the report (for sharing).
    redact: bool = False
    file_extensions: tuple = FILE_EXTENSIONS


# --------------------------------------------------------------------------- #
# Assets: what adapters hand to the core
# --------------------------------------------------------------------------- #


@dataclass
class CodeAsset:
    """A piece of source code: a notebook, a code file, a saved query or a view."""

    path: str
    kind: str  # notebook | file | query | view | dbt_model ...
    language: str  # python | sql | scala | r | unknown (notebooks may mix)
    content: str
    owner: Optional[str] = None
    modified_at: Optional[str] = None
    location: Optional[str] = None  # e.g. workspace folder, catalog.schema
    in_job: bool = False
    format: str = "auto"  # auto | databricks_source | ipynb | plain


@dataclass
class FileAsset:
    """A CSV/Excel file found in storage."""

    path: str
    name: str
    extension: str
    area: str  # volume | workspace | dbfs | local | stage | onelake
    container: Optional[str] = None  # catalog.schema.volume, folder ...
    size_bytes: Optional[int] = None
    modified_at: Optional[str] = None
    owner: Optional[str] = None
    # Filled in by the scan
    referenced_by: List[str] = field(default_factory=list)
    loaded_into: List[str] = field(default_factory=list)
    header: List[str] = field(default_factory=list)
    row_estimate: Optional[int] = None
    sheet_count: Optional[int] = None
    is_dbt_seed: bool = False
    score: int = 0
    reasons: List[str] = field(default_factory=list)


@dataclass
class TableAsset:
    """A table or view registered in the catalog."""

    full_name: str
    catalog: str
    schema: str
    name: str
    table_type: Optional[str] = None
    data_format: Optional[str] = None
    column_count: Optional[int] = None
    columns: List[str] = field(default_factory=list)
    owner: Optional[str] = None
    created_by: Optional[str] = None
    comment: Optional[str] = None
    updated_at: Optional[str] = None
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    loaded_from_files: List[str] = field(default_factory=list)
    score: int = 0
    reasons: List[str] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Findings: hardcoded reference data found inside code
# --------------------------------------------------------------------------- #


@dataclass
class CodeFinding:
    source_path: str
    source_kind: str
    language: str
    line: int
    pattern: str  # python_dict, sql_case, sql_in_list, ...
    shape: str  # mapping | code_list | inline_table
    item_count: int
    name: Optional[str] = None  # variable / column the literal is bound to
    # Normalised values used for fingerprinting. For mappings: keys.
    values: List[str] = field(default_factory=list)
    # For mappings: (key, value) pairs, used for drift detection.
    pairs: List[List[str]] = field(default_factory=list)
    owner: Optional[str] = None
    modified_at: Optional[str] = None
    location: Optional[str] = None
    in_job: bool = False
    snippet: str = ""
    # Filled in later
    id: str = ""
    fingerprint: str = ""
    group_id: Optional[str] = None
    group_conflict: bool = False
    likely_noise: Optional[str] = None  # why this is probably NOT reference data
    score: int = 0
    reasons: List[str] = field(default_factory=list)

    def sample(self, n: int) -> str:
        if n <= 0:
            return ""
        if self.pairs:
            items = [f"{k} -> {v}" for k, v in self.pairs[:n]]
        else:
            items = self.values[:n]
        more = self.item_count - len(items)
        text = ", ".join(items)
        return text + (f", ... (+{more})" if more > 0 else "")


@dataclass
class DuplicateGroup:
    group_id: str
    finding_ids: List[str]
    locations: List[str]
    item_count: int
    exact: bool  # all members have identical values
    conflicting: bool  # same keys but the mapped values disagree
    label: str = ""
    sample: str = ""
    differences: List[str] = field(default_factory=list)  # e.g. "WA: West Australia vs Western Australia"


@dataclass
class ScanResult:
    platform: str
    scope: Dict[str, Any]
    started_at: str
    finished_at: str = ""
    run_by: Optional[str] = None
    files: List[FileAsset] = field(default_factory=list)
    tables: List[TableAsset] = field(default_factory=list)
    code_findings: List[CodeFinding] = field(default_factory=list)
    duplicate_groups: List[DuplicateGroup] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
