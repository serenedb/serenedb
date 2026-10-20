"""Merges and a backfill share a two-thread DuckDB pool without waiting forever.

VACUUM (COMPACT_TABLE) and CALL sdb_compact run a merge's units on the
session's DuckDB threads, CREATE INDEX fills its index on them too. With two
threads in the pool, four VACUUM calls, a backfill and an sdb_compact call
started together all finish, and every table keeps its rows.
"""

from __future__ import annotations

import os
import socket
import subprocess
import threading
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

ROWS = 40000
BATCHES = 4
TABLES = 5


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


def _load(server: _Server) -> None:
    with server.connect() as conn:
        for table in range(1, TABLES + 1):
            conn.execute(
                f"CREATE TABLE t{table} (id INT PRIMARY KEY, body TEXT, n BIGINT)"
                " WITH (storage = 'search', compaction_interval = 0)")
            for batch in range(BATCHES):
                first = batch * ROWS + 1
                conn.execute(
                    f"INSERT INTO t{table} SELECT i, CASE WHEN i % 3 = 0"
                    " THEN 'fox dog' ELSE 'owl cat' END, i"
                    f" FROM generate_series({first}, {first + ROWS - 1}) s(i)")
                conn.execute(f"VACUUM (REFRESH_TABLE) t{table}")
        conn.execute("CREATE TABLE r (id INT, body TEXT)")
        conn.execute("INSERT INTO r SELECT i, 'word' || (i % 1000)"
                     " FROM generate_series(1, 400000) s(i)")


def test_merges_and_a_backfill_share_two_threads(tmp_path: Path) -> None:
    server = _Server(tmp_path / "data", "--cpu_threads=2")
    try:
        assert server.wait_ready(), server.output()
        _load(server)
        statements = [f"VACUUM (COMPACT_TABLE) t{table}" for table in range(1, 5)]
        statements.append("CREATE INDEX r_idx ON r USING inverted (id, body)")
        statements.append(
            f"SELECT * FROM sdb_compact('t{TABLES}', mode := 'compact')")
        errors: list[str] = []

        def run(sql: str) -> None:
            try:
                with server.connect() as conn:
                    conn.execute(sql)
            except psycopg.Error as error:
                errors.append(f"{sql}: {error}")

        workers = [threading.Thread(target=run, args=(sql,), daemon=True)
                   for sql in statements]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=300)
        assert not any(worker.is_alive() for worker in workers), \
            "a statement did not finish within 300 s"
        assert errors == []

        with server.connect() as conn:
            for table in range(1, TABLES + 1):
                segments = conn.execute(
                    f"SELECT count(*) FROM sdb_segments('t{table}')").fetchone()
                rows = conn.execute(f"SELECT count(*) FROM t{table}").fetchone()
                assert segments[0] == 1, table
                assert rows[0] == BATCHES * ROWS, table
            hits = conn.execute(
                "SELECT count(*) FROM r WHERE body = 'word7'").fetchone()
            assert hits[0] == 400
    finally:
        server.stop()
