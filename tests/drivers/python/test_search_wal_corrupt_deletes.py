"""Replay refuses a search-table delete record it can't trust."""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest
from serened import Serened  # type: ignore[import-untyped]


SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

NO_SEGMENT_FAULT = "search_delete_wal_no_segment"
MALFORMED_MAPS_FAULT = "search_delete_wal_malformed_maps"
NO_TABLE_FAULT = "search_delete_wal_no_table"

DELETE = "DELETE FROM ranges WHERE id < 100"
INSERT = "INSERT INTO ranges SELECT i FROM range(1000, 1100) r(i)"

pytestmark = [
    pytest.mark.skipif(
        not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
        reason=f"serened binary not found or not executable: {SERENED_BIN}",
    ),
    pytest.mark.faults,
]


@pytest.mark.parametrize(
    ("faults", "statement", "error"),
    [
        (
            (NO_SEGMENT_FAULT,),
            DELETE,
            "Malformed doc removal: it names no segment",
        ),
        (
            (MALFORMED_MAPS_FAULT,),
            DELETE,
            "Malformed doc map: it does not lead to a newer segment",
        ),
        (
            (NO_TABLE_FAULT,),
            DELETE,
            "Corrupt WAL: row position delete without a log table",
        ),
        (
            (NO_TABLE_FAULT, MALFORMED_MAPS_FAULT),
            INSERT,
            "Corrupt WAL: row position maps without a log table",
        ),
    ],
)
def test_corrupt_delete_refuses_replay(
    tmp_path: Path, faults: tuple[str, ...], statement: str, error: str
) -> None:
    server = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "serened.log"))
    try:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            conn.execute("CREATE TABLE ranges (id BIGINT) WITH (storage = 'search')")
            conn.execute("CREATE TABLE plain (id BIGINT)")
            conn.execute("INSERT INTO ranges SELECT i FROM range(0, 1000) r(i)")
            conn.execute("VACUUM (REFRESH_TABLE) ranges")
            conn.execute("CHECKPOINT")
            conn.execute("INSERT INTO plain VALUES (1)")
            for fault in faults:
                conn.execute(f"SET sdb_faults = '{fault}'")
            assert conn.execute(statement).rowcount == 100
            conn.execute("SET debug_checkpoint_abort = 'before_header'")
        assert server.graceful_stop() == 0

        with pytest.raises(RuntimeError, match="exited"):
            server.start()
        log = server.log.tail(0)
        assert error in log, log[-2000:]
    finally:
        server.stop()
