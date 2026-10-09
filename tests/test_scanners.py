from refdata_scanner.models import CodeAsset, ScanConfig
from refdata_scanner.notebook_parser import split_cells
from refdata_scanner.scanners import find_file_references, scan_code_asset
from refdata_scanner.scanners.python_scanner import scan_python
from refdata_scanner.scanners.sql_scanner import scan_sql

CFG = ScanConfig()


def _py(src):
    findings, embedded, err = scan_python(src, CFG)
    assert err is None
    return findings, embedded


def test_python_dict_mapping():
    f, _ = _py('STATUS = {"A": "Active", "I": "Inactive", "P": "Pending", "C": "Closed", "X": "Cancelled"}')
    assert len(f) == 1
    assert f[0].pattern == "python_dict" and f[0].shape == "mapping"
    assert f[0].name == "STATUS" and f[0].item_count == 5
    assert ["A", "Active"] in f[0].pairs


def test_small_dict_ignored():
    f, _ = _py('x = {"a": 1, "b": 2}')
    assert f == []


def test_python_list_threshold_is_strictly_greater():
    ten = "codes = [" + ",".join(f'"C{i}"' for i in range(10)) + "]"
    eleven = "codes = [" + ",".join(f'"C{i}"' for i in range(11)) + "]"
    assert _py(ten)[0] == []
    f, _ = _py(eleven)
    assert f[0].pattern == "python_list" and f[0].item_count == 11


def test_set_and_isin():
    f, _ = _py("df.filter(F.col('x').isin([" + ",".join(f"'v{i}'" for i in range(12)) + "]))")
    assert f[0].pattern == "python_isin_list"
    f, _ = _py("S = {" + ",".join(str(i * 7) for i in range(15)) + "}")
    assert f[0].pattern == "python_set"


def test_dataframe_literal_rows():
    src = 'df = spark.createDataFrame([("01", "Retail"), ("02", "Online"), ("03", "Phone")], ["code", "name"])'
    f, _ = _py(src)
    assert len(f) == 1
    assert f[0].pattern == "python_dataframe_literal"
    assert f[0].pairs[0] == ["01", "Retail"]


def test_when_chain_named_by_withcolumn():
    src = (
        'df = df.withColumn("tier", F.when(F.col("t") == "G", "Gold").when(F.col("t") == "S", "Silver")'
        '.when(F.col("t") == "B", "Bronze").when(F.col("t") == "P", "Platinum").when(F.col("t") == "D", "Diamond")'
        '.otherwise("None"))'
    )
    f, _ = _py(src)
    assert len(f) == 1
    assert f[0].pattern == "python_when_chain" and f[0].name == "tier" and f[0].item_count == 5
    assert f[0].pairs[0] == ["G", "Gold"]


def test_dict_of_lists_grouping():
    src = 'GROUPS = {"East": ["NSW", "VIC"], "North": ["QLD"], "West": ["WA"], "South": ["SA", "TAS"], "Other": ["NT"]}'
    f, _ = _py(src)
    assert f[0].pattern == "python_dict_of_lists"
    assert ["NSW", "East"] in f[0].pairs


def test_embedded_sql_detected():
    _, emb = _py('spark.sql("select 1")')
    assert emb and emb[0].sql == "select 1"


def test_magic_lines_tolerated():
    findings, _, err = scan_python("%pip install x\n!ls\nM = {" + ",".join(f"'{i}': '{i}'" for i in range(6)) + "}", CFG)
    assert err is None and len(findings) == 1


def test_sql_case_simple_and_searched():
    sql = """
    select case region when 'N' then 'North' when 'S' then 'South' when 'E' then 'East' when 'W' then 'West'
                       when 'C' then 'Central' end as region_name,
           case when x = 1 then 'a' when x = 2 then 'b' when x = 3 then 'c' when x = 4 then 'd' when x = 5 then 'e' end
    from t
    """
    f, warn = scan_sql(sql, CFG)
    assert warn is None
    cases = sorted((x for x in f if x.pattern == "sql_case"), key=lambda x: x.line)
    assert len(cases) == 2
    assert cases[0].name == "region_name" and ["N", "North"] in cases[0].pairs
    assert cases[1].name == "x" and ["1", "a"] in cases[1].pairs


def test_sql_in_list_and_values():
    sql = ("select * from t where c in (" + ",".join(f"'{i}'" for i in range(11)) + ");\n"
           "insert into ref.ccy values ('AUD','Australian Dollar'),('USD','US Dollar'),('EUR','Euro')")
    f, _ = scan_sql(sql, CFG)
    pats = {x.pattern for x in f}
    assert pats == {"sql_in_list", "sql_insert_values"}
    ins = next(x for x in f if x.pattern == "sql_insert_values")
    assert ins.name == "ref.ccy" and ins.line == 2


def test_sql_jinja_dbt_model_parses():
    sql = "{{ config(materialized='view') }}\nselect case s when 'A' then 1 when 'B' then 2 when 'C' then 3 " \
          "when 'D' then 4 when 'E' then 5 end as s_num from {{ ref('x') }}"
    f, warn = scan_sql(sql, CFG)
    assert warn is None and f[0].name == "s_num"


def test_sql_regex_fallback():
    sql = "select case code when 'a' then 'x' when 'b' then 'y' when 'c' then 'z' when 'd' then 'w' " \
          "when 'e' then 'v' end as lbl from t where @@@ broken syntax !!"
    f, warn = scan_sql(sql, CFG)
    assert warn is not None
    assert f and f[0].pattern == "sql_case" and f[0].item_count == 5


def test_databricks_source_notebook_cells():
    src = (
        "# Databricks notebook source\nx = 1\n\n# COMMAND ----------\n\n# MAGIC %md\n# MAGIC # Title\n\n"
        "# COMMAND ----------\n\n# MAGIC %sql\n# MAGIC select 1\n"
    )
    cells = split_cells("nb", src, "python")
    assert [c.language for c in cells] == ["python", "sql"]
    assert cells[1].source.strip() == "select 1" and cells[1].start_line == 12


def test_sql_notebook_with_python_cell():
    src = "-- Databricks notebook source\nselect 1\n\n-- COMMAND ----------\n\n-- MAGIC %python\n-- MAGIC x = 2\n"
    cells = split_cells("nb", src, "sql")
    assert [c.language for c in cells] == ["sql", "python"]


def test_ipynb_cells():
    import json

    nb = {"metadata": {"kernelspec": {"language": "python"}},
          "cells": [{"cell_type": "markdown", "source": ["# hi"]},
                    {"cell_type": "code", "source": ["%%sql\n", "select 1"]},
                    {"cell_type": "code", "source": "y = 3"}]}
    cells = split_cells("a.ipynb", json.dumps(nb))
    assert [c.language for c in cells] == ["sql", "python"]


def test_file_references():
    refs = find_file_references('df = spark.read.csv("/Volumes/a/b/c/x.csv")\npd.read_excel(\'m.xlsx\')')
    assert [(r.path, r.line) for r in refs] == [("/Volumes/a/b/c/x.csv", 1), ("m.xlsx", 2)]


def test_scan_code_asset_line_numbers_and_metadata():
    src = "# Databricks notebook source\n\n# COMMAND ----------\n\nM = {" + \
          ", ".join(f"'k{i}': 'v{i}'" for i in range(5)) + "}\n"
    out = scan_code_asset(CodeAsset(path="/Users/a/nb", kind="notebook", language="python", content=src,
                                    owner="a", in_job=True), CFG)
    f = out.findings[0]
    assert f.line == 5 and f.owner == "a" and f.in_job and f.source_path == "/Users/a/nb"
