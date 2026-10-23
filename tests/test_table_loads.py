"""Tables filled only from literal VALUES: INSERT ... VALUES, MERGE ... USING VALUES, CREATE TABLE AS VALUES."""
from refdata_scanner.analysis import flag_noise, score_code_finding
from refdata_scanner.models import CodeAsset, ScanConfig
from refdata_scanner.scanners import scan_code_asset
from refdata_scanner.scanners.sql_scanner import _scan_regex, combine_table_loads, scan_sql

CFG = ScanConfig()


def _loads(sql, cfg=CFG):
    findings, _ = scan_sql(sql, cfg)
    return [f for f in findings if f.pattern.endswith(("insert_values", "merge_values", "ctas_values"))]


def test_single_row_insert_counts_when_table_name_looks_like_reference_data():
    f = _loads("INSERT INTO main.ref.payment_terms VALUES ('N30', 'Net 30 days')")
    assert len(f) == 1
    assert f[0].pattern == "sql_insert_values" and f[0].name == "main.ref.payment_terms"
    assert f[0].pairs == [["N30", "Net 30 days"]]


def test_single_row_insert_into_ordinary_table_is_not_enough():
    assert _loads("INSERT INTO sales.orders VALUES (1, 'x')") == []
    assert len(_loads("INSERT INTO sales.orders VALUES (1, 'x'), (2, 'y')")) == 1  # min_insert_rows = 2


def test_one_row_insert_statements_are_combined_per_table():
    sql = """
        INSERT INTO ops.payment_terms VALUES ('N7', 'Net 7 days');
        INSERT INTO ops.payment_terms VALUES ('N14', 'Net 14 days');
        INSERT INTO other.thing VALUES (1, 'a'), (2, 'b');
        INSERT INTO ops.payment_terms (code, name) VALUES ('N30', 'Net 30 days');
        INSERT INTO ops.payment_terms VALUES ('N7', 'Net 7 days');
    """
    f = {x.name: x for x in _loads(sql)}
    terms = f["ops.payment_terms"]
    assert terms.item_count == 3 and terms.statements == 3  # the repeated N7 statement counts once
    assert terms.pairs == [["N7", "Net 7 days"], ["N14", "Net 14 days"], ["N30", "Net 30 days"]]
    assert terms.line == 2 and "3 statements" in terms.snippet
    assert f["other.thing"].item_count == 2


def test_variants_with_only_values_as_source():
    cases = {
        "INSERT OVERWRITE TABLE ref.status VALUES ('A','Active'), ('I','Inactive')": ("sql_insert_values", "ref.status"),
        "INSERT INTO ref.status SELECT * FROM VALUES ('A','Active'), ('I','Inactive') AS t(code, name)":
            ("sql_insert_values", "ref.status"),
        "INSERT INTO `main`.`ref`.`status` VALUES ('A','Active'), ('I','Inactive')": ("sql_insert_values", "main.ref.status"),
        "MERGE INTO ref.status t USING (VALUES ('A','Active'), ('I','Inactive')) AS s(code, name) ON t.code = s.code "
        "WHEN NOT MATCHED THEN INSERT *": ("sql_merge_values", "ref.status"),
        "CREATE OR REPLACE TABLE ref.status AS SELECT * FROM VALUES ('A','Active'), ('I','Inactive') AS t(code, name)":
            ("sql_ctas_values", "ref.status"),
        "CREATE OR REPLACE TABLE ref.status AS VALUES ('A','Active'), ('I','Inactive')": ("sql_ctas_values", "ref.status"),
    }
    for sql, (pattern, name) in cases.items():
        f = _loads(sql)
        assert len(f) == 1, sql
        assert (f[0].pattern, f[0].name, f[0].item_count) == (pattern, name, 2), sql


def test_inserts_from_other_sources_or_with_expressions_are_ignored():
    assert _loads("INSERT INTO ref.status SELECT code, name FROM staging.status") == []
    assert _loads("INSERT INTO ref.status SELECT * FROM VALUES ('A','x'), ('B','y') AS t(c, n) WHERE c = 'A'") == []
    assert _loads("INSERT INTO ref.job_runs VALUES ('job', current_timestamp()), ('job2', now())") == []


def test_regex_fallback_finds_inserts():
    sql = "INSERT INTO ref.status (code, name) VALUES ('A', 'Active'), ('I', 'It''s inactive') ;"
    f = [x for x in _scan_regex(sql, CFG, 1) if x.pattern == "sql_insert_values"]
    assert f and f[0].name == "ref.status" and f[0].pairs == [["A", "Active"], ["I", "It's inactive"]]
    merge = _scan_regex("MERGE INTO ref.ccy USING (VALUES ('AUD','Dollar'),('EUR','Euro')) s ON 1=1", CFG, 1)
    assert [m.pattern for m in merge] == ["sql_merge_values"]


def test_combined_across_notebook_cells_and_spark_sql():
    nb = (
        "# Databricks notebook source\n"
        "# MAGIC %sql\n# MAGIC INSERT INTO ref.uom VALUES ('EA', 'Each')\n\n"
        "# COMMAND ----------\n\n"
        "spark.sql(\"INSERT INTO ref.uom VALUES ('DZ', 'Dozen')\")\n"
        "spark.sql(\"INSERT INTO ref.uom VALUES ('BX', 'Box')\")\n"
    )
    out = scan_code_asset(CodeAsset(path="/nb", kind="notebook", language="python", content=nb), CFG)
    loads = [f for f in out.findings if f.pattern.endswith("insert_values")]
    assert len(loads) == 1 and loads[0].item_count == 3 and loads[0].statements == 3
    assert loads[0].line == 3


def test_log_tables_are_noise_and_reference_loads_score_higher():
    log, = _loads("INSERT INTO main.ops.etl_audit_log VALUES ('a','b'), ('c','d'), ('e','f')")
    ref, = _loads("INSERT INTO main.ops.lkp_order_status VALUES ('N','New'), ('S','Shipped'), ('D','Delivered')")
    flag_noise([log, ref], set())
    assert log.likely_noise and not ref.likely_noise
    for f in (log, ref):
        score_code_finding(f, {})
    assert log.score <= 10
    assert any("literal VALUES" in r for r in ref.reasons) and ref.score >= 60


def test_min_insert_rows_is_configurable():
    sql = "INSERT INTO sales.thing VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    assert _loads(sql, ScanConfig(min_insert_rows=4)) == []
    assert len(combine_table_loads(scan_sql(sql, CFG, combine_loads=False)[0], ScanConfig(min_insert_rows=3))) == 1
