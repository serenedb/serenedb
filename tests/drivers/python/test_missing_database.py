"""What boot does with a data directory that lost files.

A table or index whose directory is missing refuses to start, since coming
back without its data would turn a lost file into silent data loss. A missing
catalog log beside table directories refuses to start too.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

import psycopg
import pytest
from serened import listens_on, reserve_port  # type: ignore[import-untyped]

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


class _Server:
    def __init__(self, datadir: Path, *flags: str) -> None:
        self.reserved = reserve_port()
        self.port = self.reserved.getsockname()[1]
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
            if listens_on(self.proc.pid, [self.port]):
                try:
                    self.connect().close()
                    return True
                except psycopg.OperationalError:
                    pass
            time.sleep(0.2)
        return False

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=60)
        self.reserved.close()

    def output(self) -> str:
        return self.log.read_text()


def test_missing_catalog_log(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute("CREATE TABLE kept(a INTEGER)")
            conn.execute("INSERT INTO kept VALUES (1)")
        storage = _search_table_dir(server, datadir, "postgres", "kept")
    finally:
        server.stop()
    shutil.rmtree(datadir / "engine_v1" / "wal")

    refused = _Server(datadir)
    try:
        assert not refused.wait_ready(timeout=30)
        assert refused.proc.wait(timeout=30) != 0
        assert "holds no catalog log" in refused.output()
    finally:
        refused.stop()
    assert storage.is_dir()


def _search_table_dir(server: _Server, datadir: Path, database: str,
                      table: str) -> Path:
    with server.connect() as conn:
        table_oid = conn.execute(
            "SELECT table_oid FROM duckdb_tables() "
            "WHERE database_name = %s AND table_name = %s",
            (database, table)).fetchone()[0]
    return datadir / "engine_v1" / "store" / str(table_oid)


def test_missing_storage_directory(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute(
                "CREATE TABLE docs(id BIGINT PRIMARY KEY, body TEXT) ")
            conn.execute("INSERT INTO docs VALUES (1, 'kept')")
        storage = _search_table_dir(server, datadir, "postgres", "docs")
    finally:
        server.stop()
    assert storage.is_dir()
    for path in storage.iterdir():
        path.unlink()
    storage.rmdir()

    refused = _Server(datadir)
    try:
        assert not refused.wait_ready(timeout=30)
        assert refused.proc.wait(timeout=30) != 0
        assert f"'{storage}' of docs" in refused.output()
        assert "is missing" in refused.output()
    finally:
        refused.stop()
