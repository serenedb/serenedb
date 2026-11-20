"""A temporary job is dropped when its session ends, interrupting a run in progress and letting the session's
temporary objects go."""

from __future__ import annotations

import os
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


pytestmark = [
    pytest.mark.skipif(
        not _have_binary(),
        reason=f"serened binary not found or not executable: {SERENED_BIN}",
    ),
    pytest.mark.faults,
]


def _connect(server: Serened) -> psycopg.Connection:
    return psycopg.connect(server.dsn(), autocommit=True)


def _wait_parked(conn: psycopg.Connection, point: str) -> None:
    deadline = time.monotonic() + 30
    while True:
        try:
            conn.execute(f"SET sdb_faults = '-{point}:parked'")
            return
        except psycopg.Error:
            assert time.monotonic() < deadline, f"nothing parked at {point}"
            time.sleep(0.05)


def _wait_sleeping(conn: psycopg.Connection) -> None:
    deadline = time.monotonic() + 30
    while conn.execute("SELECT count(*) FROM pg_stat_activity "
                       "WHERE state = 'active' AND query LIKE 'SELECT pg_sleep%'"
                       ).fetchone()[0] == 0:
        assert time.monotonic() < deadline, "the job never ran"
        time.sleep(0.05)


def test_a_temporary_job_is_dropped_when_its_session_ends(tmp_path: Path) -> None:
    server = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "serened.log"))
    try:
        server.start()
        with _connect(server) as admin:
            admin.execute("CREATE TABLE tj_log (step INT)")
            admin.execute("SET sdb_faults = 'pause_job_after_run'")
            session = _connect(server)
            session.execute(
                "CREATE TEMPORARY JOB tj_steps EVERY 1 SECOND AS $$"
                "INSERT INTO postgres.public.tj_log VALUES (1); "
                "SELECT pg_sleep(60); "
                "INSERT INTO postgres.public.tj_log VALUES (2);$$")
            _wait_sleeping(admin)
            session.close()
            _wait_parked(admin, "pause_job_after_run")
            assert admin.execute("SELECT step FROM tj_log ORDER BY step").fetchall() == [(1,)]
            admin.execute("RESET sdb_faults")
    finally:
        server.stop()


def _cache_bytes(conn: psycopg.Connection) -> int:
    return conn.execute("SELECT value::BIGINT FROM sdb_metrics WHERE relation_id IS NULL "
                        "AND metric = 'external_file_cache_used_bytes'").fetchone()[0]


def test_a_temporary_job_lets_its_session_go_when_the_session_ends(tmp_path: Path) -> None:
    server = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "serened.log"))
    try:
        server.start()
        with _connect(server) as admin:
            session = _connect(server)
            session.execute(
                "CREATE TEMPORARY TABLE tj_big AS SELECT i AS id, "
                "'word' || (i % 1000) || ' other ' || i AS body FROM generate_series(1, 300000) g(i)")
            session.execute("CREATE INDEX tj_big_body ON tj_big USING inverted(body)")
            session.execute("VACUUM (FLUSH_TABLE) tj_big")
            session.execute("CREATE TEMPORARY JOB tj_keeps EVERY 1 HOUR AS SELECT count(*) FROM tj_big")
            assert _cache_bytes(admin) > 0
            session.close()
            deadline = time.monotonic() + 30
            while _cache_bytes(admin) != 0:
                assert time.monotonic() < deadline, _cache_bytes(admin)
                time.sleep(0.05)
    finally:
        server.stop()
