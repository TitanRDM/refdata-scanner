"""The scanner must never reach outside the user's platform: no installs, downloads or HTTP clients.

Platform APIs are reached only through the Databricks SDK (or Spark) that the user's own
environment provides. This test fails if anyone adds another route out.

The single sanctioned exception: 02_full_scan may pip-install the sqlglot parser when it is not
bundled or already on the cluster (Git folder users), behind a widget the user can turn off.
"""
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FORBIDDEN = [
    (re.compile(r"^\s*(import|from)\s+(requests|urllib|urllib3|http\.client|httpx|aiohttp|socket|subprocess|ftplib|smtplib)\b", re.M),
     "network or process import"),
    (re.compile(r"%pip\b|\bpip\.main\b|\bpip install\b(?![^\n]*refdata-scanner\[)", re.M), "package install"),
    (re.compile(r"\bos\.system\(|\bos\.popen\(|\bsubprocess\.\w+\(", re.M), "shell command"),
    (re.compile(r"\burlopen\(|\.urlretrieve\(", re.M), "download"),
]


def _files():
    for base in ("src", "notebooks"):
        for root, _, files in os.walk(os.path.join(ROOT, base)):
            for f in files:
                if f.endswith((".py", ".sql")):
                    yield os.path.join(root, f)


# (file, exact source line) pairs that are allowed to match the patterns above.
ALLOWED = {
    ("notebooks/databricks/02_full_scan.py", "import subprocess"),
    ("notebooks/databricks/02_full_scan.py",
     'subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "sqlglot>=25"])'),
}


def test_no_outbound_calls_in_scanner_or_notebooks():
    problems = []
    for path in _files():
        rel = os.path.relpath(path, ROOT).replace(os.sep, "/")
        text = open(path, encoding="utf-8").read()
        lines = text.split("\n")
        for pattern, label in FORBIDDEN:
            for m in pattern.finditer(text):
                line_no = text.count("\n", 0, m.start()) + 1
                if (rel, lines[line_no - 1].strip()) in ALLOWED:
                    continue
                problems.append(f"{rel}:{line_no}: {label}: {m.group(0).strip()}")
    assert not problems, "\n".join(problems)


def test_sqlglot_install_is_optional_and_only_sqlglot():
    text = open(os.path.join(ROOT, "notebooks", "databricks", "02_full_scan.py"), encoding="utf-8").read()
    install = text.index('"pip", "install"')
    guard = text.rfind('if dbutils.widgets.get("install_sqlglot") == "yes":', 0, install)
    assert guard != -1, "the pip install must sit behind the install_sqlglot widget"
    assert text.count("check_call(") == 1 and text.count("subprocess") == 2


def test_scanner_works_without_sqlglot():
    """Git-folder users may not have sqlglot; SQL must still be scanned (by pattern matching)."""
    import subprocess
    import sys

    code = (
        "import sys; sys.path.insert(0, sys.argv[1]);"
        "from refdata_scanner.models import ScanConfig;"
        "from refdata_scanner.scanners.sql_scanner import scan_sql;"
        "f, w = scan_sql(\"select case c when 'a' then 1 when 'b' then 2 when 'c' then 3 when 'd' then 4 "
        "when 'e' then 5 end as x from t\", ScanConfig());"
        "assert f and f[0].pattern == 'sql_case', f"
    )
    out = subprocess.run([sys.executable, "-I", "-S", "-c", code, os.path.join(ROOT, "src")],
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
