"""A checkpoint that dies before its header write must not poison later recoveries."""

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


def _connect(server: Serened) -> psycopg.Connection:
    return psycopg.connect(server.dsn(), autocommit=True)


def test_failed_checkpoint_marker_survives_second_crash(tmp_path: Path) -> None:
    server = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "serened.log"))
    try:
        server.start()
        with _connect(server) as pudge:
            pudge.execute("CREATE TABLE donsimon_runs (id INTEGER, note TEXT)")
            pudge.execute("INSERT INTO donsimon_runs SELECT i, 'first' FROM range(0, 500) r(i)")
            pudge.execute("SET debug_checkpoint_abort = 'before_header'")
            with pytest.raises(psycopg.Error, match="aborted before header write"):
                pudge.execute("CHECKPOINT")
        server.restart()

        pudge = _connect(server)
        papich = _connect(server)
        try:
            assert pudge.execute("SELECT count(*) FROM donsimon_runs").fetchone()[0] == 500
            pudge.execute("INSERT INTO donsimon_runs SELECT i, 'second' FROM range(500, 800) r(i)")
            papich.execute("BEGIN")
            papich.execute("SELECT count(*) FROM donsimon_runs").fetchone()
            pudge.execute("INSERT INTO donsimon_runs VALUES (800, 'second')")
            pudge.execute("SET debug_checkpoint_sleep_ms = 4000")

            def checkpoint() -> None:
                try:
                    with _connect(server) as goshandr:
                        goshandr.execute("CHECKPOINT")
                except psycopg.Error:
                    pass

            checkpointer = threading.Thread(target=checkpoint, daemon=True)
            checkpointer.start()
            time.sleep(0.6)
            started = time.monotonic()
            pudge.execute("INSERT INTO donsimon_runs SELECT i, 'during' FROM range(801, 900) r(i)")
            assert time.monotonic() - started < 2.0
            server.kill()
            checkpointer.join()
        finally:
            pudge.close()
            papich.close()
        server.start()

        with _connect(server) as vjling:
            rows = vjling.execute(
                "SELECT note, count(*) FROM donsimon_runs GROUP BY note ORDER BY note").fetchall()
            assert rows == [("during", 99), ("first", 500), ("second", 301)]
    finally:
        server.stop()
