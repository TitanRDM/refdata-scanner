"""The scanner must never reach outside the user's platform: no installs, downloads or HTTP clients.

Platform APIs are reached only through the Databricks SDK (or Spark) that the user's own
environment provides. This test fails if anyone adds another route out.
"""
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FORBIDDEN = [
    (re.compile(r"^\s*(import|from)\s+(requests|urllib|urllib3|http\.client|httpx|aiohttp|socket|subprocess|ftplib|smtplib)\b", re.M),
     "network or process import"),
    (re.compile(r"%pip\b|\bpip\.main\b|\bpip install\b(?![^\n]*refdata-scanner\[)", re.M), "package install"),
    (re.compile(r"\bos\.system\(|\bos\.popen\(", re.M), "shell command"),
    (re.compile(r"\burlopen\(|\.urlretrieve\(", re.M), "download"),
]


def _files():
    for base in ("src", "notebooks"):
        for root, _, files in os.walk(os.path.join(ROOT, base)):
            for f in files:
                if f.endswith((".py", ".sql")):
                    yield os.path.join(root, f)


def test_no_outbound_calls_in_scanner_or_notebooks():
    problems = []
    for path in _files():
        text = open(path, encoding="utf-8").read()
        for pattern, label in FORBIDDEN:
            for m in pattern.finditer(text):
                line = text.count("\n", 0, m.start()) + 1
                problems.append(f"{os.path.relpath(path, ROOT)}:{line}: {label}: {m.group(0).strip()}")
    assert not problems, "\n".join(problems)


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
