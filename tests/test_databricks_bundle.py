import importlib.util
import os
import subprocess
import sys
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("build_bundle", os.path.join(ROOT, "scripts", "build_databricks_bundle.py"))
bundle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bundle)


def test_zip_contains_notebooks_package_and_parser(tmp_path):
    names = zipfile.ZipFile(bundle.build_zip(str(tmp_path))).namelist()
    assert "refdata-scanner/01_quick_scan.sql" in names
    assert "refdata-scanner/02_full_scan.py" in names
    assert "refdata-scanner/lib/refdata_scanner/__init__.py" in names
    assert "refdata-scanner/lib/refdata_scanner/adapters/databricks.py" in names
    assert "refdata-scanner/lib/sqlglot/__init__.py" in names
    assert "refdata-scanner/lib/sqlglot/LICENSE" in names
    assert "refdata-scanner/lib/THIRD_PARTY.md" in names
    assert not any("__pycache__" in n or n.endswith((".pyc", ".so")) for n in names)


def test_bundle_runs_from_lib_alone(tmp_path):
    """With site-packages disabled (-S), the scanner and sqlglot must load from lib/ and scan SQL properly."""
    zipfile.ZipFile(bundle.build_zip(str(tmp_path))).extractall(tmp_path)
    lib = tmp_path / "refdata-scanner" / "lib"
    code = (
        "import sys; sys.path.insert(0, sys.argv[1]);"
        "import sqlglot, refdata_scanner;"
        "from refdata_scanner.models import ScanConfig;"
        "from refdata_scanner.scanners.sql_scanner import scan_sql;"
        "f, w = scan_sql(\"select case c when 'a' then 1 when 'b' then 2 when 'c' then 3 when 'd' then 4 "
        "when 'e' then 5 end as x from t\", ScanConfig());"
        "assert w is None and f[0].name == 'x', (f, w);"
        "print(sqlglot.__file__)"
    )
    out = subprocess.run([sys.executable, "-I", "-S", "-c", code, str(lib)], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert str(lib) in out.stdout
