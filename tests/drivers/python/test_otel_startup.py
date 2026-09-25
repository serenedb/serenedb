"""Startup checks of an ?api=otel listener against the built-in schema.

Each case runs its own serened on a fresh data directory, so the tables can be
broken before the OpenTelemetry listener first sees them.
"""

from __future__ import annotations

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
