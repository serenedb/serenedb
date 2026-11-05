from __future__ import annotations

import csv
import fcntl
import http.server
import io
import json
import os
import pty
import select
import struct
import subprocess
import termios
import threading
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


def _interactive(
    lines: list[str],
    *,
    rows: int = 50,
    env: dict[str, str] | None = None,
    timeout: float = 30.0,
) -> str:
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", rows, 200, 0, 0))
    process = subprocess.Popen(
        [SERENED_BIN, "shell", "-dark-mode", ":memory:"],
        stdin=slave,
        stdout=slave,
        stderr=slave,
        env={**os.environ, "TERM": "xterm", "PAGER": "cat", **(env or {})},
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


@pytest.mark.parametrize("shape", ["pg", "duckdb_native"])
def test_explain_analyze_for_a_browser_prints_no_hint(tmp_path: Path, shape: str) -> None:
    opener = tmp_path / "bin" / "xdg-open"
    opener.parent.mkdir()
    opener.write_text("#!/bin/sh\nexit 0\n")
    opener.chmod(0o755)
    output = _interactive(
        [
            f"SET explain_output_format = '{shape}';",
            "EXPLAIN ANALYZE (FORMAT WEB) SELECT 1;",
        ],
        env={"PATH": f"{opener.parent}:{os.environ['PATH']}", "TMP": str(tmp_path)},
    )
    assert "Opening query profile in browser" in output, output
    assert "in a browser" not in output, output
    assert list(tmp_path.glob("*.html")), output


def test_explain_analyze_into_a_file_writes_no_hint(tmp_path: Path) -> None:
    plan = tmp_path / "plan.txt"
    output = _interactive([f".output {plan}", "EXPLAIN ANALYZE SELECT 1;", ".output"])
    assert "DUMMY_SCAN" in plan.read_text(), output
    assert "in a browser" not in plan.read_text() + output, output


@pytest.mark.parametrize("highlight", ["on", "off"])
@pytest.mark.parametrize("shape", ["pg", "duckdb_native"])
def test_only_a_plan_taller_than_the_terminal_goes_through_the_pager(
    shape: str, highlight: str
) -> None:
    lines = [
        f".highlight {highlight}",
        f"SET explain_output_format = '{shape}';",
        "EXPLAIN SELECT 1;",
    ]
    pager = {"PAGER": "sed s/^/paged:/"}
    assert "paged:" in _interactive(lines, rows=6, env=pager)
    assert "paged:" not in _interactive(lines, rows=7, env=pager)


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


def test_floats_and_bytea_follow_their_settings() -> None:
    assert _rows(
        "SELECT 0.1::DOUBLE + 0.2::DOUBLE AS f, '\\x0102'::BYTEA AS b;\n"
        "SET extra_float_digits = 0;\n"
        "SET bytea_output = 'escape';\n"
        "SELECT 0.1::DOUBLE + 0.2::DOUBLE AS f, '\\x0102'::BYTEA AS b;\n"
    ) == [
        ["f", "b"],
        ["0.30000000000000004", "\\x0102"],
        ["f", "b"],
        ["0.3", "\\001\\002"],
    ]


def test_xml_follows_xmloption() -> None:
    result = _shell("SET xmloption = document;\nSELECT 'a<b/>'::xml AS x;\n")
    assert "invalid XML document" in result.stderr, result.stderr


class _Provider(http.server.BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        self.rfile.read(int(self.headers["Content-Length"]))
        body = json.dumps({
            "choices": [{
                "message": {"role": "assistant", "content": "done"},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


def test_ai_functions_call_the_provider() -> None:
    provider = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Provider)
    threading.Thread(target=provider.serve_forever, daemon=True).start()
    try:
        rows = _rows(
            "CREATE SECRET chat (TYPE openai, base_url"
            f" 'http://127.0.0.1:{provider.server_address[1]}', model 'm');\n"
            "SELECT ai_generate('hi', secret_name := 'chat') AS answer;\n"
        )
    finally:
        provider.shutdown()
    assert rows[-2:] == [["answer"], ["done"]]


def test_a_database_opened_after_a_failed_open_is_serenedb_too(tmp_path: Path) -> None:
    result = _shell(f".open {tmp_path / 'missing' / 'x.db'}\nSELECT 7 / 2 AS q;\n")
    assert "Cannot open file" in result.stderr, result.stderr
    assert list(csv.reader(io.StringIO(result.stdout)))[-2:] == [["q"], ["3"]]
