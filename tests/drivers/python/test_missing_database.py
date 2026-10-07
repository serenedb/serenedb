"""What boot does with a data directory that lost files or holds leftovers.

A database whose data file is missing: the default refuses to start, since
coming back with an empty database would turn a lost file into silent data
loss. --missing_database=skip boots without attaching it, and
--missing_database=drop removes it from the catalog.

A missing catalog log beside database directories that hold data refuses to
start too. Empty directories a crashed first boot left are removed, and the
directories of an older layout are left alone.
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
    for path in (datadir / "engine_v1" / str(oid)).glob("data.db*"):
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
    assert not (datadir / "engine_v1" / str(oid)).exists()

    after = _Server(datadir)
    try:
        assert after.wait_ready(), after.output()
        assert _databases(after) == ["postgres"]
    finally:
        after.stop()


def test_missing_catalog_log(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute("CREATE TABLE kept(a INTEGER)")
            conn.execute("INSERT INTO kept VALUES (1)")
    finally:
        server.stop()
    (datadir / "engine_v1" / "catalog.wal").unlink()

    refused = _Server(datadir)
    try:
        assert not refused.wait_ready(timeout=30)
        assert refused.proc.wait(timeout=30) != 0
        assert "holds database directories" in refused.output()
    finally:
        refused.stop()
    assert any((datadir / "engine_v1").glob("*/data.db"))


def test_first_boot_leftovers(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    engine = datadir / "engine_v1"
    (engine / "5").mkdir(parents=True)
    (engine / "4242").mkdir()
    old = datadir / "engine_duckdb"
    old.mkdir()
    (old / "5.db").write_bytes(b"older layout")

    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        assert _databases(server) == ["postgres"]
    finally:
        server.stop()
    assert not (engine / "4242").exists()
    assert (engine / "5" / "data.db").exists()
    assert (old / "5.db").read_bytes() == b"older layout"


def _search_table_dir(server: _Server, datadir: Path, database: str,
                      table: str) -> Path:
    with server.connect() as conn:
        db_oid, table_oid = conn.execute(
            "SELECT d.oid, t.table_oid FROM pg_database d, duckdb_tables() t "
            "WHERE d.datname = %s AND t.database_name = %s "
            "AND t.table_name = %s", (database, database, table)).fetchone()
    return datadir / "engine_v1" / str(db_oid) / str(table_oid)


def test_missing_storage_directory(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute(
                "CREATE TABLE docs(id BIGINT PRIMARY KEY, body TEXT) "
                "WITH (storage = 'search')")
            conn.execute("INSERT INTO docs VALUES (1, 'kept')")
            conn.execute("VACUUM (REFRESH_TABLE) docs")
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


def test_missing_database_directory(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute("CREATE DATABASE gone")
            conn.execute(
                "CREATE TABLE gone.public.docs(id BIGINT PRIMARY KEY, v INT) "
                "WITH (storage = 'search')")
            conn.execute("INSERT INTO gone.public.docs VALUES (1, 1)")
        storage = _search_table_dir(server, datadir, "gone", "docs")
    finally:
        server.stop()
    database = storage.parent
    for path in sorted(database.rglob("*"), reverse=True):
        path.rmdir() if path.is_dir() else path.unlink()
    database.rmdir()

    skipped = _Server(datadir, "--missing_database=skip")
    try:
        assert skipped.wait_ready(), skipped.output()
        assert _databases(skipped) == ["gone", "postgres"]
    finally:
        skipped.stop()
    assert not database.exists()

    dropped = _Server(datadir, "--missing_database=drop")
    try:
        assert dropped.wait_ready(), dropped.output()
        assert _databases(dropped) == ["postgres"]
    finally:
        dropped.stop()
    assert not database.exists()
