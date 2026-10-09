from refdata_scanner.analysis import (
    find_duplicates,
    flag_noise,
    has_name_hint,
    link_file_references,
    score_code_finding,
)
from refdata_scanner.models import CodeFinding, FileAsset


def _f(path, pairs=None, values=None, name="M", shape="mapping", pattern="python_dict", fid=None):
    values = values if values is not None else [k for k, _ in (pairs or [])]
    f = CodeFinding(source_path=path, source_kind="notebook", language="python", line=1, pattern=pattern,
                    shape=shape, item_count=len(values), name=name, values=values, pairs=pairs or [])
    f.id = fid or path
    return f


STATES = [["NSW", "New South Wales"], ["QLD", "Queensland"], ["VIC", "Victoria"], ["WA", "Western Australia"],
          ["SA", "South Australia"]]


def test_identical_copies_group_without_conflict():
    a, b = _f("/a", STATES), _f("/b", STATES)
    groups = find_duplicates([a, b], 0.8)
    assert len(groups) == 1 and groups[0].exact and not groups[0].conflicting


def test_drift_detected_with_differences():
    drifted = [p[:] for p in STATES]
    drifted[3][1] = "West Australia"
    a, b = _f("/a", STATES), _f("/b", drifted)
    g = find_duplicates([a, b], 0.8)[0]
    assert g.conflicting and a.group_conflict and b.group_conflict
    assert g.differences == ["WA: West Australia vs Western Australia"]


def test_same_codes_different_meaning_is_not_drift():
    regions = [[k, r] for (k, _), r in zip(STATES, ["East", "North", "East", "West", "South"])]
    a, b = _f("/a", STATES), _f("/b", regions)
    g = find_duplicates([a, b], 0.8)[0]
    assert not g.conflicting and not g.exact


def test_near_duplicate_lists_grouped():
    base = [f"C{i}" for i in range(20)]
    a = _f("/a", values=base, shape="code_list", pattern="python_list")
    b = _f("/b", values=base[:-2], shape="code_list", pattern="python_list")
    c = _f("/c", values=[f"Z{i}" for i in range(20)], shape="code_list", pattern="python_list")
    groups = find_duplicates([a, b, c], 0.8)
    assert len(groups) == 1 and set(groups[0].finding_ids) == {"/a", "/b"}


def test_column_lists_and_config_flagged_as_noise():
    cols = _f("/a", values=[f"col_{i}" for i in range(12)], name="select_cols", shape="code_list", pattern="python_list")
    known = _f("/b", values=["order_id", "qty", "price"] * 4, name="x", shape="code_list", pattern="python_list")
    conf = _f("/c", [["spark.a", "1"], ["spark.b", "2"], ["spark.c", "3"], ["spark.d", "4"], ["spark.e", "5"]],
              name="spark_conf")
    seq = _f("/d", values=[str(i) for i in range(2000, 2015)], name="years", shape="code_list", pattern="python_list")
    real = _f("/e", STATES, name="STATE_MAP")
    flag_noise([cols, known, conf, seq, real], {"order_id", "qty", "price"})
    assert cols.likely_noise and known.likely_noise and conf.likely_noise and seq.likely_noise
    assert real.likely_noise is None
    score_code_finding(cols, {})
    assert cols.score <= 10


def test_name_hints():
    assert has_name_hint("dim_customer_type")
    assert has_name_hint("countryCodes")
    assert not has_name_hint("statement")
    assert not has_name_hint("raw_orders")


def test_file_reference_linking():
    files = [FileAsset(path="/Volumes/main/ref/up/codes.csv", name="codes.csv", extension=".csv", area="volume"),
             FileAsset(path="/Shared/fin/map.xlsx", name="map.xlsx", extension=".xlsx", area="workspace"),
             FileAsset(path="dbfs:/FileStore/tables/a.csv", name="a.csv", extension=".csv", area="dbfs")]
    link_file_references(files, {
        "dbfs:/Volumes/main/ref/up/codes.csv": ["/nb1:3"],
        "/Workspace/Shared/fin/map.xlsx": ["/nb2:7"],
        "/dbfs/FileStore/tables/*.csv": ["/nb3:1"],
    })
    assert files[0].referenced_by == ["/nb1:3"]
    assert files[1].referenced_by == ["/nb2:7"]
    assert files[2].referenced_by == ["/nb3:1"]
