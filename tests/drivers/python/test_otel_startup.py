"""Startup checks of an ?api=otel listener against the built-in schema.

Each case runs its own serened on a fresh data directory, so the tables can be
broken before the OpenTelemetry listener first sees them.
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
import time
from pathlib import Path

import psycopg
import pytest

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _connect(port: int, deadline: float) -> psycopg.Connection:
    while True:
        try:
            return psycopg.connect(
                host="127.0.0.1", port=port, user="postgres", dbname="postgres",
                autocommit=True,
            )
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


def _break_schema(datadir: Path, statement: str) -> None:
    port = _free_port()
    server = subprocess.Popen(
        [SERENED_BIN, str(datadir), f"--listen=postgres://127.0.0.1:{port}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        with _connect(port, time.monotonic() + 60) as conn:
            conn.execute(statement)
    finally:
        server.terminate()
        server.wait(timeout=60)


def _start_otel(datadir: Path) -> subprocess.CompletedProcess:
    listen = (f"postgres://127.0.0.1:{_free_port()},"
              f"http://127.0.0.1:{_free_port()}?api=otel")
    return subprocess.run(
        [SERENED_BIN, str(datadir), f"--listen={listen}"],
        capture_output=True, text=True, timeout=60,
    )


def test_mistyped_column_refuses_startup(tmp_path: Path) -> None:
    _break_schema(
        tmp_path,
        'CREATE TABLE otel_logs ("timestamp" VARCHAR NOT NULL) '
        "WITH (storage = 'search')",
    )
    result = _start_otel(tmp_path)
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "invalid OpenTelemetry schema" in output, output
    assert "timestamp" in output, output


FIXTURES = (
    Path(__file__).resolve().parents[3] / "resources" / "otel" / "conformance"
)


class _Server:
    def __init__(self, datadir: Path, params: str):
        self.pg_port, self.http_port = _free_port(), _free_port()
        listen = (f"postgres://127.0.0.1:{self.pg_port},"
                  f"http://127.0.0.1:{self.http_port}?{params}")
        self.proc = subprocess.Popen(
            [SERENED_BIN, str(datadir), f"--listen={listen}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        self.pg = _connect(self.pg_port, time.monotonic() + 60)

    def post(self, path: str, body: bytes, content_type: str) -> int:
        conn = http.client.HTTPConnection("127.0.0.1", self.http_port,
                                          timeout=60)
        try:
            conn.request("POST", path, body=body, headers={
                "Content-Type": content_type,
                "Authorization": "Basic cG9zdGdyZXM6",
            })
            response = conn.getresponse()
            response.read()
            return response.status
        finally:
            conn.close()

    def count(self, table: str) -> int:
        self.pg.execute(f"VACUUM (REFRESH_TABLE) {table}")
        return self.pg.execute(f"SELECT count(*) FROM {table}").fetchone()[0]

    def close(self) -> None:
        self.pg.close()
        self.proc.terminate()
        self.proc.wait(timeout=60)


def test_schema_param_places_the_tables(tmp_path: Path) -> None:
    server = _Server(tmp_path, "api=otel&schema=telemetry")
    try:
        body = (FIXTURES / "logs/basic.json").read_bytes()
        assert server.post("/v1/logs", body, "application/json") == 200
        assert server.count("telemetry.otel_logs") > 0
        missing = server.pg.execute(
            "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename = 'otel_logs'").fetchone()[0]
        assert missing == 0
    finally:
        server.close()


def test_single_worker_thread_does_not_deadlock(tmp_path: Path) -> None:
    server = _Server(tmp_path, "api=otel&api=es")
    try:
        server.pg.execute("SET GLOBAL threads = 1")
        assert str(server.pg.execute(
            "SELECT current_setting('threads')").fetchone()[0]) == "1"
        for fixture, path in (("logs/basic.json", "/v1/logs"),
                              ("traces/upstream.json", "/v1/traces"),
                              ("metrics/mixed_batch.json", "/v1/metrics")):
            body = (FIXTURES / fixture).read_bytes()
            assert server.post(path, body, "application/json") == 200, path
        assert server.count("otel_logs") > 0

        mapping = json.dumps({"mappings": {"properties": {
            "n": {"type": "integer"}}}}).encode()
        conn = http.client.HTTPConnection("127.0.0.1", server.http_port,
                                          timeout=60)
        conn.request("PUT", "/single", body=mapping, headers={
            "Content-Type": "application/json",
            "Authorization": "Basic cG9zdGdyZXM6"})
        assert conn.getresponse().status == 200
        conn.close()
        bulk = "".join('{"index":{}}\n{"n":%d}\n' % i for i in range(1000))
        assert server.post("/single/_bulk", bulk.encode(),
                           "application/x-ndjson") == 200
        assert server.count("es.single") == 1000
    finally:
        server.close()
