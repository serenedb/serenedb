from __future__ import annotations

import csv
import fcntl
import io
import os
import pty
import select
import struct
import subprocess
import termios
import time
from pathlib import Path

import pytest


SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)


def _have_binary() -> bool:
    return Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)


pytestmark = pytest.mark.skipif(
    not _have_binary(),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


def _shell(script: str, *, timeout: float = 30.0) -> subprocess.CompletedProcess:
    return subprocess.run(
        [SERENED_BIN, "shell", "-no-agent", ":memory:", "--csv"],
        input=script,
        capture_output=True,
        text=True,
        timeout=timeout,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )


def _rows(sql: str) -> list[list[str]]:
    result = _shell(sql)
    assert result.returncode == 0, result.stderr
    return list(csv.reader(io.StringIO(result.stdout)))


def _interactive(lines: list[str], *, timeout: float = 30.0) -> str:
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 50, 200, 0, 0))
    process = subprocess.Popen(
        [SERENED_BIN, "shell", ":memory:"],
        stdin=slave,
        stdout=slave,
        stderr=slave,
        env={**os.environ, "TERM": "xterm", "PAGER": "cat"},
        close_fds=True,
    )
    os.close(slave)
    for line in [*lines, ".quit"]:
        os.write(master, (line + "\r").encode())
    output = bytearray()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ready, _, _ = select.select([master], [], [], 0.5)
        if ready:
            try:
                chunk = os.read(master, 65536)
            except OSError:
                break
            if not chunk:
                break
            output += chunk
        elif process.poll() is not None:
            break
    os.close(master)
    assert process.wait(timeout=timeout) == 0, output.decode(errors="replace")
    return output.decode(errors="replace")


def test_integer_division_truncates() -> None:
    assert _rows("SELECT 7 / 2 AS q;") == [["q"], ["3"]]


def test_unquoted_identifiers_fold_to_lower_case() -> None:
    assert _rows('SELECT 1 AS Mixed, 2 AS "Quoted";')[0] == ["mixed", "Quoted"]


def test_copy_to_csv_writes_no_header_unless_asked(tmp_path: Path) -> None:
    plain = tmp_path / "plain.csv"
    header = tmp_path / "header.csv"
    _rows(
        f"COPY (SELECT 1 AS a) TO '{plain}' (FORMAT csv);\n"
        f"COPY (SELECT 1 AS a) TO '{header}' (FORMAT csv, HEADER);\n"
    )
    assert plain.read_text().splitlines() == ["1"]
    assert header.read_text().splitlines() == ["a", "1"]


def test_show_prints_a_setting() -> None:
    assert _rows("SHOW integer_division;")[1:] == [["on"]]


def test_explain_prints_the_plan() -> None:
    result = _shell("EXPLAIN SELECT 1;")
    assert result.returncode == 0, result.stderr
    assert "DUMMY_SCAN" in result.stdout, result.stdout


def test_explain_in_a_terminal_prints_the_plan_and_one_analyze_hint() -> None:
    output = _interactive(["EXPLAIN SELECT 1;", "EXPLAIN ANALYZE SELECT 1;"])
    assert output.count("DUMMY_SCAN") >= 2, output
    assert output.count("open the query profile in a browser") + output.count(
        ".web to open it in a browser") == 1, output


def test_a_view_keeps_its_table() -> None:
    result = _shell(
        "CREATE TABLE t (i INTEGER);\n"
        "CREATE VIEW v AS SELECT i FROM t;\n"
        "DROP TABLE t;\n"
        "SELECT count(*) AS views FROM duckdb_views() WHERE view_name = 'v';\n"
    )
    assert "depend" in result.stderr, result.stderr
    assert list(csv.reader(io.StringIO(result.stdout))) == [["views"], ["1"]]


def test_extensions_are_never_loaded_on_demand() -> None:
    assert _rows(
        "SELECT current_setting('autoinstall_known_extensions') AS install,"
        " current_setting('autoload_known_extensions') AS load,"
        " current_setting('iceberg_use_metadata_log') AS metadata_log;"
    ) == [["install", "load", "metadata_log"], ["off", "off", "off"]]


def test_serenedb_functions_and_types() -> None:
    assert _rows(
        "SELECT to_char(TIMESTAMP '2024-01-02 03:04:05', 'YYYY-MM-DD HH24:MI') AS formatted,"
        " '12.5'::money AS amount;"
    ) == [["formatted", "amount"], ["2024-01-02 03:04", "$12.50"]]


def test_a_database_opened_after_a_failed_open_is_serenedb_too(tmp_path: Path) -> None:
    result = _shell(f".open {tmp_path / 'missing' / 'x.db'}\nSELECT 7 / 2 AS q;\n")
    assert "Cannot open file" in result.stderr, result.stderr
    assert list(csv.reader(io.StringIO(result.stdout)))[-2:] == [["q"], ["3"]]
