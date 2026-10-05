"""A database whose data file is missing at boot.

The default refuses to start, since coming back with an empty database would
turn a lost file into silent data loss. --missing_database=skip boots without
attaching it, and --missing_database=drop removes it from the catalog.
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
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class _Server:
    def __init__(self, datadir: Path, *flags: str) -> None:
        self.port = _free_port()
        self.log = datadir.parent / f"serened-{self.port}.log"
        with self.log.open("w") as out:
            self.proc = subprocess.Popen(
                [SERENED_BIN, str(datadir),
                 f"--listen=postgres://127.0.0.1:{self.port}", *flags],
                stdout=out, stderr=subprocess.STDOUT)

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(host="127.0.0.1", port=self.port,
                               user="postgres", dbname="postgres",
                               autocommit=True)

    def wait_ready(self, timeout: float = 60.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                return False
            try:
                self.connect().close()
                return True
            except psycopg.OperationalError:
                time.sleep(0.2)
        return False

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=60)

    def output(self) -> str:
        return self.log.read_text()


def _databases(server: _Server) -> list[str]:
    with server.connect() as conn:
        rows = conn.execute(
            "SELECT datname FROM pg_database ORDER BY datname").fetchall()
    return [row[0] for row in rows]


def test_missing_database_file(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute("CREATE DATABASE lost")
            conn.execute("CREATE TABLE lost.public.t(a INTEGER)")
            conn.execute("INSERT INTO lost.public.t VALUES (1)")
            oid = conn.execute(
                "SELECT oid FROM pg_database WHERE datname = 'lost'"
            ).fetchone()[0]
    finally:
        server.stop()
    for path in (datadir / "engine_duckdb").glob(f"{oid}.db*"):
        path.unlink()

    refused = _Server(datadir)
    try:
        assert not refused.wait_ready(timeout=30)
        assert refused.proc.wait(timeout=30) != 0
        assert "--missing_database=skip" in refused.output()
    finally:
        refused.stop()

    skipped = _Server(datadir, "--missing_database=skip")
    try:
        assert skipped.wait_ready(), skipped.output()
        assert _databases(skipped) == ["lost", "postgres"]
    finally:
        skipped.stop()

    dropped = _Server(datadir, "--missing_database=drop")
    try:
        assert dropped.wait_ready(), dropped.output()
        assert _databases(dropped) == ["postgres"]
    finally:
        dropped.stop()

    after = _Server(datadir)
    try:
        assert after.wait_ready(), after.output()
        assert _databases(after) == ["postgres"]
    finally:
        after.stop()
