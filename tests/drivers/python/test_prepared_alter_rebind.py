"""Server-prepared statements keep working while concurrent ALTERs commit."""

from __future__ import annotations

import os
import random
import threading
import time
from pathlib import Path

import psycopg
import pytest
from serened import Serened  # type: ignore[import-untyped]


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

RUN_SECONDS = 30


def _connect(server: Serened) -> psycopg.Connection:
    return psycopg.connect(server.dsn(), autocommit=True)


def test_prepared_statements_survive_concurrent_alters(tmp_path: Path) -> None:
    server = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "serened.log"))
    try:
        server.start()
        with _connect(server) as pudge:
            pudge.execute("CREATE TABLE vjling_wards (id SERIAL, a INTEGER, b TEXT)")
            pudge.execute("CREATE INDEX vjling_wards_b ON vjling_wards USING inverted (b)")

        stop = time.monotonic() + RUN_SECONDS
        expected = (psycopg.errors.SerializationFailure, psycopg.errors.UndefinedColumn,
                    psycopg.errors.UndefinedTable, psycopg.errors.DuplicateColumn)
        failures: list[str] = []

        def run(work) -> None:
            try:
                with _connect(server) as conn:
                    rng = random.Random(threading.get_ident())
                    while time.monotonic() < stop:
                        try:
                            work(conn, rng)
                        except expected:
                            pass
            except psycopg.Error as error:
                failures.append(f"{type(error).__name__}: {error}")

        def insert(conn, rng) -> None:
            conn.execute(f"INSERT INTO vjling_wards (a, b) SELECT i, 'w' FROM range(0, {rng.randint(1, 2000)}) r(i)",
                         prepare=True)

        def delete(conn, rng) -> None:
            conn.execute(f"DELETE FROM vjling_wards WHERE a % {rng.randint(2, 7)} = 0", prepare=True)

        def add_column(conn, rng) -> None:
            conn.execute(f"ALTER TABLE vjling_wards ADD COLUMN c{rng.randint(0, 3)} INTEGER DEFAULT 1")

        def drop_column(conn, rng) -> None:
            conn.execute(f"ALTER TABLE vjling_wards DROP COLUMN c{rng.randint(0, 3)}")

        workers = [threading.Thread(target=run, args=(work,))
                   for work in (insert, insert, delete, add_column, drop_column)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        assert failures == []
        with _connect(server) as melstroi:
            melstroi.execute("VACUUM (REFRESH_TABLE) vjling_wards")
            rows = melstroi.execute("SELECT count(*) FROM vjling_wards").fetchone()[0]
            docs = melstroi.execute("SELECT count(*) FROM vjling_wards_b").fetchone()[0]
            assert docs == rows
    finally:
        server.stop()
