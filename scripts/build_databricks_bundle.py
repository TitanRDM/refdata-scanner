"""Build the Databricks downloads from the notebooks in notebooks/databricks.

Produces, in dist/ (or --out):

1. refdata-scanner-install.py: a single notebook that installs everything below into the workspace
   (Import > File, then Run all). This is the main route: importing one notebook always works.

2. refdata-scanner-databricks.zip, which the installer carries inside it:

    refdata-scanner/01_quick_scan               SQL notebook
    refdata-scanner/02_full_scan                Python notebook
    refdata-scanner/lib/refdata_scanner/...     the scanner package
    refdata-scanner/lib/sqlglot/...             the SQL parser it uses (MIT licence), vendored
    refdata-scanner/lib/THIRD_PARTY.md          versions and licences of vendored code

The zip can also be imported directly (Import > File), and is the readable copy of what the installer
writes. The notebooks load everything from the lib folder, so a scan never installs, downloads or sends
anything outside the workspace.

There is deliberately no .dbc archive: a DBC can only hold notebooks, so the
scanner would have to be downloaded at run time.

Usage: python scripts/build_databricks_bundle.py [--out dist] [--version v0.1.1]
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import os
import re
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_DIR = os.path.join(ROOT, "notebooks", "databricks")
PACKAGE_DIR = os.path.join(ROOT, "src", "refdata_scanner")
BUNDLE_FOLDER = "refdata-scanner"
ZIP_NAME = "refdata-scanner-databricks.zip"
INSTALLER_NAME = "refdata-scanner-install.py"
INSTALLER_TEMPLATE = os.path.join(ROOT, "scripts", "installer_template.py")
_CELL_CHARS = 200_000  # base64 characters per notebook cell
_LINE_CHARS = 1_000  # characters per source line

_HEADER_RE = re.compile(r"^(#|--)\s*Databricks notebook source\s*$")
# A fixed timestamp keeps zip output identical between builds of the same source.
_ZIP_DATE = (2026, 1, 1, 0, 0, 0)


def _notebooks():
    for name in sorted(os.listdir(NOTEBOOK_DIR)):
        if not name.endswith((".py", ".sql")):
            continue
        with open(os.path.join(NOTEBOOK_DIR, name), encoding="utf-8") as fh:
            content = fh.read()
        if not _HEADER_RE.match(content.split("\n", 1)[0].strip()):
            raise SystemExit(f"{name} is not in Databricks source format (missing header line)")
        yield name, content


def _write(zf: zipfile.ZipFile, arcname: str, data) -> None:
    # Explicit entries for every folder: some unzip tools, including workspace import, only create folders
    # that have their own entry, and silently drop files in folders that don't.
    parts = arcname.split("/")[:-1]
    for i in range(1, len(parts) + 1):
        folder = "/".join(parts[:i]) + "/"
        if folder not in zf.NameToInfo:
            d = zipfile.ZipInfo(folder, date_time=_ZIP_DATE)
            d.external_attr = (0o40755 << 16) | 0x10  # unix directory + MS-DOS directory flag
            zf.writestr(d, b"")
    info = zipfile.ZipInfo(arcname, date_time=_ZIP_DATE)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    zf.writestr(info, data)


def _add_tree(zf: zipfile.ZipFile, src_dir: str, arc_prefix: str) -> int:
    count = 0
    for root, dirs, files in os.walk(src_dir):
        dirs[:] = sorted(d for d in dirs if d != "__pycache__")
        for f in sorted(files):
            if f.endswith((".pyc", ".pyo", ".so", ".pyd")):
                continue
            full = os.path.join(root, f)
            rel = os.path.relpath(full, src_dir).replace(os.sep, "/")
            with open(full, "rb") as fh:
                _write(zf, f"{arc_prefix}/{rel}", fh.read())
            count += 1
    return count


def _sqlglot_source():
    import sqlglot

    pkg_dir = os.path.dirname(sqlglot.__file__)
    version = getattr(sqlglot, "__version__", "unknown")
    licence = None
    site = os.path.dirname(pkg_dir)
    for entry in os.listdir(site):
        if entry.lower().startswith("sqlglot-") and entry.endswith(".dist-info"):
            for cand in ("licenses/LICENSE", "LICENSE", "license_files/LICENSE"):
                path = os.path.join(site, entry, cand)
                if os.path.exists(path):
                    licence = path
    return pkg_dir, version, licence


def build_zip(out_dir: str) -> str:
    path = os.path.join(out_dir, ZIP_NAME)
    sqlglot_dir, sqlglot_version, sqlglot_licence = _sqlglot_source()
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in _notebooks():
            _write(zf, f"{BUNDLE_FOLDER}/{name}", content)
        _add_tree(zf, PACKAGE_DIR, f"{BUNDLE_FOLDER}/lib/refdata_scanner")
        _add_tree(zf, sqlglot_dir, f"{BUNDLE_FOLDER}/lib/sqlglot")
        if sqlglot_licence:
            with open(sqlglot_licence, "rb") as fh:
                _write(zf, f"{BUNDLE_FOLDER}/lib/sqlglot/LICENSE", fh.read())
        _write(zf, f"{BUNDLE_FOLDER}/lib/THIRD_PARTY.md",
               "# Third-party code in this bundle\n\n"
               f"- `sqlglot/`: [sqlglot](https://github.com/tobymao/sqlglot) {sqlglot_version}, MIT licence "
               "(see `sqlglot/LICENSE`). Pure Python SQL parser, included unmodified so the scanner never needs to "
               "install anything at run time.\n")
        with open(os.path.join(ROOT, "LICENSE"), "rb") as fh:
            _write(zf, f"{BUNDLE_FOLDER}/LICENSE", fh.read())
    return path


def _package_version() -> str:
    with open(os.path.join(PACKAGE_DIR, "__init__.py"), encoding="utf-8") as fh:
        m = re.search(r'__version__\s*=\s*"([^"]+)"', fh.read())
    return "v" + m.group(1) if m else "dev"


def build_installer(out_dir: str, zip_path: str, version: str) -> str:
    """A single notebook carrying the zip as base64, which unpacks it into the workspace when run."""
    with open(zip_path, "rb") as fh:
        data = fh.read()
    digest = hashlib.sha256(data).hexdigest()
    encoded = base64.b64encode(data).decode("ascii")
    cells = []
    for start in range(0, len(encoded), _CELL_CHARS):
        chunk = encoded[start:start + _CELL_CHARS]
        lines = "\n".join(f'    "{chunk[i:i + _LINE_CHARS]}"' for i in range(0, len(chunk), _LINE_CHARS))
        cells.append(f"PAYLOAD.append(\n{lines}\n)")
    with open(INSTALLER_TEMPLATE, encoding="utf-8") as fh:
        template = fh.read()
    notebook = (template.replace("#__PAYLOAD_CELLS__", "\n\n# COMMAND ----------\n\n".join(cells))
                .replace("__SHA256__", digest).replace("__VERSION__", version))
    path = os.path.join(out_dir, INSTALLER_NAME)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(notebook)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Databricks downloads (installer notebook and zip).")
    parser.add_argument("--out", default=os.path.join(ROOT, "dist"))
    parser.add_argument("--version", default=None, help="Version shown in the installer (default: package version)")
    args = parser.parse_args()
    os.makedirs(args.out, exist_ok=True)
    zip_path = build_zip(args.out)
    installer = build_installer(args.out, zip_path, args.version or _package_version())
    for built in (installer, zip_path):
        print(f"built {os.path.relpath(built)} ({os.path.getsize(built):,} bytes)")


if __name__ == "__main__":
    main()
