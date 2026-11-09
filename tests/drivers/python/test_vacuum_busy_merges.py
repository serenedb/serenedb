"""VACUUM warns about the merges it skips because another statement holds their segments."""

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


pytestmark = [
    pytest.mark.skipif(
        not _have_binary(),
        reason=f"serened binary not found or not executable: {SERENED_BIN}",
    ),
    pytest.mark.faults,
]


def _connect(server: Serened) -> psycopg.Connection:
    return psycopg.connect(server.dsn(), autocommit=True)


def test_vacuum_warns_about_a_merge_another_statement_runs(tmp_path: Path) -> None:
    server = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "serened.log"))
    try:
        server.start()
        with _connect(server) as conn:
            conn.execute(
                "CREATE TABLE vbm (id INT PRIMARY KEY, body TEXT)"
                " WITH (storage = 'search', compaction_interval = 0)")
            for first in (1, 101):
                conn.execute(
                    "INSERT INTO vbm SELECT i, 'fox dog'"
                    f" FROM generate_series({first}, {first + 99}) s(i)")
                conn.execute("VACUUM (REFRESH_TABLE) vbm")
            conn.execute("SET sdb_faults = 'pause_merge_unit'")
            errors: list[str] = []

            def compact() -> None:
                try:
                    with _connect(server) as other:
                        other.execute(
                            "SELECT * FROM sdb_compact('vbm', mode := 'compact')"
                        ).fetchall()
                except psycopg.Error as error:
                    errors.append(str(error))

            compactor = threading.Thread(target=compact, daemon=True)
            compactor.start()
            deadline = time.monotonic() + 30
            while conn.execute(
                    "SELECT count(*) FROM sdb_merges"
                    " WHERE relation_id = 'vbm'::regclass AND origin = 'sql'"
                    " AND state = 'running'").fetchone()[0] == 0:
                assert time.monotonic() < deadline, "the sdb_compact merge never started"
                time.sleep(0.05)
            notices: list[tuple[str, str]] = []
            conn.add_notice_handler(
                lambda diag: notices.append((diag.severity, diag.message_primary)))
            conn.execute("VACUUM (COMPACT_TABLE) vbm")
            assert notices == [(
                "WARNING",
                'skipping 1 merge of "vbm": another task is changing their segments',
            )]
            conn.execute("SET sdb_faults = '-pause_merge_unit'")
            compactor.join(timeout=60)
            assert not compactor.is_alive(), "sdb_compact did not finish within 60 s"
            assert errors == []
            segments = conn.execute("SELECT count(*) FROM sdb_segments('vbm')").fetchone()
            assert segments[0] == 1
            rows = conn.execute("SELECT count(*), sum(id) FROM vbm").fetchone()
            assert rows == (200, 20100)
    finally:
        server.stop()
