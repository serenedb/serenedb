"""Listing a procedure's body does not change it for the sessions calling it."""

from __future__ import annotations

import os
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

RUN_SECONDS = 15


def _connect(server: Serened) -> psycopg.Connection:
    return psycopg.connect(server.dsn(), autocommit=True)


def test_listing_an_insert_values_body_while_it_is_called(tmp_path: Path) -> None:
    server = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "serened.log"))
    try:
        server.start()
        with _connect(server) as conn:
            conn.execute("CREATE TABLE calls (s TEXT)")
            conn.execute("CREATE PROCEDURE log_call(x INT) AS $$ INSERT INTO calls VALUES ('call:' || x) $$ "
                         "LANGUAGE sql")
            body = conn.execute("SELECT prosrc FROM pg_proc WHERE proname = 'log_call'").fetchone()[0]

        stop = time.monotonic() + RUN_SECONDS
        failures: list[str] = []
        calls: list[int] = []

        def call(conn: psycopg.Connection) -> None:
            conn.execute("CALL log_call(1)")
            calls.append(1)

        def list_body(conn: psycopg.Connection) -> None:
            listed = conn.execute("SELECT prosrc FROM pg_proc WHERE proname = 'log_call'").fetchone()[0]
            if listed != body:
                failures.append(f"listed body changed: {listed!r} != {body!r}")

        def run(work) -> None:
            try:
                with _connect(server) as conn:
                    while time.monotonic() < stop and not failures:
                        work(conn)
            except psycopg.Error as error:
                failures.append(f"{type(error).__name__}: {error}")

        workers = [threading.Thread(target=run, args=(work,))
                   for work in (call, call, list_body, list_body)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        assert failures == []
        assert "valueslist" not in body.lower()
        with _connect(server) as conn:
            assert conn.execute("SELECT count(*) FROM calls").fetchone()[0] == len(calls)
    finally:
        server.stop()
