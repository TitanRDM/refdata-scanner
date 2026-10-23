"""Run the generated installer notebook against a fake workspace and check what it writes."""
import importlib.util
import os
import subprocess
import sys
import types
import zipfile

import pytest

pytest.importorskip("databricks.sdk")
import databricks.sdk  # noqa: E402

from refdata_scanner.notebook_parser import split_cells  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("build_bundle", os.path.join(ROOT, "scripts", "build_databricks_bundle.py"))
bundle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bundle)


class FakeWorkspace:
    """Records uploads into a local folder that mirrors workspace paths."""

    def __init__(self, root):
        self.root = root
        self.notebooks = {}
        self.files = {}

    def _local(self, path):
        return os.path.join(self.root, path.lstrip("/"))

    def get_status(self, path):
        if not os.path.exists(self._local(path)):
            raise RuntimeError("RESOURCE_DOES_NOT_EXIST")

    def mkdirs(self, path):
        os.makedirs(self._local(path), exist_ok=True)

    def upload(self, path, content, format=None, language=None, overwrite=False):
        assert overwrite
        if language is not None:  # notebook: stored without extension
            self.notebooks[path] = (getattr(language, "value", language), content)
        else:
            self.files[path] = content
        with open(self._local(path), "wb") as fh:
            fh.write(content)


def _run_installer(tmp_path, installer_path, install_folder=""):
    ws = FakeWorkspace(str(tmp_path / "ws"))
    widgets = {"install_folder": install_folder, "overwrite": "yes"}
    context = types.SimpleNamespace(notebookPath=lambda: types.SimpleNamespace(
        get=lambda: "/Users/me@x.com/tools/refdata-scanner-install"))
    dbutils = types.SimpleNamespace(
        widgets=types.SimpleNamespace(text=lambda *a: None, dropdown=lambda *a: None, get=lambda n: widgets[n]),
        notebook=types.SimpleNamespace(entry_point=types.SimpleNamespace(getDbutils=lambda: types.SimpleNamespace(
            notebook=lambda: types.SimpleNamespace(getContext=lambda: context)))))
    original = databricks.sdk.WorkspaceClient
    databricks.sdk.WorkspaceClient = lambda: types.SimpleNamespace(workspace=ws)
    try:
        source = open(installer_path, encoding="utf-8").read()
        scope = {"dbutils": dbutils, "print": lambda *a, **k: None}
        for cell in split_cells("installer.py", source, "python"):
            exec(compile(cell.source, f"installer:{cell.start_line}", "exec"), scope)
    finally:
        databricks.sdk.WorkspaceClient = original
    return ws


def test_installer_writes_notebooks_and_lib(tmp_path):
    out = tmp_path / "dist"
    out.mkdir()
    zip_path = bundle.build_zip(str(out))
    installer = bundle.build_installer(str(out), zip_path, "v9.9.9")
    text = open(installer, encoding="utf-8").read()
    assert text.startswith("# Databricks notebook source") and "v9.9.9" in text
    assert max(len(line) for line in text.split("\n")) < 1100  # no huge single lines in the notebook editor

    ws = _run_installer(tmp_path, installer)
    base = "/Users/me@x.com/tools/refdata-scanner"
    assert ws.notebooks[f"{base}/01_quick_scan"][0] == "SQL"
    assert ws.notebooks[f"{base}/02_full_scan"][0] == "PYTHON"
    assert f"{base}/lib/refdata_scanner/__init__.py" in ws.files
    assert f"{base}/lib/sqlglot/__init__.py" in ws.files
    expected = [n for n in zipfile.ZipFile(zip_path).namelist() if not n.endswith("/")]
    assert len(ws.notebooks) + len(ws.files) == len(expected)

    # The installed lib folder works on its own (no site-packages)
    lib = os.path.join(ws.root, base.lstrip("/"), "lib")
    code = ("import sys; sys.path.insert(0, sys.argv[1]); import sqlglot, refdata_scanner;"
            "assert sqlglot.__file__.startswith(sys.argv[1])")
    run = subprocess.run([sys.executable, "-I", "-S", "-c", code, lib], capture_output=True, text=True)
    assert run.returncode == 0, run.stderr


def test_installer_honours_install_folder_and_checksum(tmp_path):
    out = tmp_path / "dist"
    out.mkdir()
    installer = bundle.build_installer(str(out), bundle.build_zip(str(out)), "v1")
    ws = _run_installer(tmp_path, installer, install_folder="/Workspace/Shared/tools/rds")
    assert "/Shared/tools/rds/02_full_scan" in ws.notebooks

    damaged = out / "damaged.py"
    text = open(installer, encoding="utf-8").read()
    i = text.index('    "', text.index("PAYLOAD.append(")) + 5
    damaged.write_text(text[:i] + ("A" if text[i] != "A" else "B") + text[i + 1:], encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        _run_installer(tmp_path / "second", str(damaged))


def test_zip_has_folder_entries(tmp_path):
    names = zipfile.ZipFile(bundle.build_zip(str(tmp_path))).namelist()
    folders = {n for n in names if n.endswith("/")}
    for n in names:
        parts = n.rstrip("/").split("/")[:-1]
        for i in range(1, len(parts) + 1):
            assert "/".join(parts[:i]) + "/" in folders, n
    assert names.index("refdata-scanner/lib/") < names.index("refdata-scanner/lib/THIRD_PARTY.md")
