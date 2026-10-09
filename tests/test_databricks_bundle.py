import importlib.util
import json
import os
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("build_bundle", os.path.join(ROOT, "scripts", "build_databricks_bundle.py"))
bundle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bundle)


def test_zip_contains_notebooks_and_package(tmp_path):
    path = bundle.build_zip(str(tmp_path))
    names = zipfile.ZipFile(path).namelist()
    assert "refdata-scanner/01_quick_scan.sql" in names
    assert "refdata-scanner/02_full_scan.py" in names
    assert "refdata-scanner/lib/refdata_scanner/__init__.py" in names
    assert "refdata-scanner/lib/refdata_scanner/adapters/databricks.py" in names
    assert not any("__pycache__" in n for n in names)


def test_zip_package_is_importable(tmp_path):
    zipfile.ZipFile(bundle.build_zip(str(tmp_path))).extractall(tmp_path)
    lib = tmp_path / "refdata-scanner" / "lib"
    import subprocess
    import sys

    out = subprocess.run([sys.executable, "-I", "-c", "import sys; sys.path.insert(0, sys.argv[1]); "
                          "import refdata_scanner; print(refdata_scanner.__file__)", str(lib)],
                         capture_output=True, text=True, check=True)
    assert str(lib) in out.stdout


def test_dbc_notebooks_are_valid(tmp_path):
    zf = zipfile.ZipFile(bundle.build_dbc(str(tmp_path)))
    assert sorted(zf.namelist()) == ["refdata-scanner/01_quick_scan.sql", "refdata-scanner/02_full_scan.python"]
    nb = json.loads(zf.read("refdata-scanner/02_full_scan.python"))
    assert nb["version"] == "NotebookV1" and nb["language"] == "python" and nb["name"] == "02_full_scan"
    commands = [c["command"] for c in nb["commands"]]
    assert commands[0].startswith("%md\n# Reference data full scan")
    assert any(c.startswith("%pip install") for c in commands)
    assert not any("# MAGIC" in c or "COMMAND ----" in c for c in commands)
    positions = [c["position"] for c in nb["commands"]]
    assert positions == sorted(positions)

    sql = json.loads(zf.read("refdata-scanner/01_quick_scan.sql"))
    assert sql["language"] == "sql"
    assert any(c["command"].startswith("CREATE WIDGET") for c in sql["commands"])


def test_cell_count_matches_source():
    for name, content in bundle._notebooks():
        separators = sum(1 for line in content.split("\n") if bundle._SEPARATOR_RE.match(line.strip()))
        assert len(bundle.split_commands(content)) == separators + 1, name
