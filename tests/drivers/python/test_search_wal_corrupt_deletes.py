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

TRUNCATED_POSITIONS_FAULT = "search_delete_wal_truncated_positions"
NO_SEGMENT_FAULT = "search_delete_wal_no_segment"
MALFORMED_MAPS_FAULT = "search_delete_wal_malformed_maps"

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


@pytest.mark.parametrize(
    ("fault", "error"),
    [
        (TRUNCATED_POSITIONS_FAULT, "Malformed doc removal: it ends early"),
        (
            NO_SEGMENT_FAULT,
            "Malformed doc removal: a segment number is out of range",
        ),
        (MALFORMED_MAPS_FAULT, "Malformed doc map: it ends early"),
    ],
)
def test_corrupt_delete_refuses_replay(tmp_path: Path, fault: str, error: str) -> None:
    server = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "serened.log"))
    try:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            try:
                conn.execute(f"SET sdb_faults = '{fault}'")
                conn.execute(f"SET sdb_faults = '-{fault}'")
            except psycopg.Error:
                pytest.skip("serened is built without fault injection")
            conn.execute("CREATE TABLE ranges (id BIGINT) WITH (storage = 'search')")
            conn.execute("INSERT INTO ranges SELECT i FROM range(0, 1000) r(i)")
            conn.execute("VACUUM (REFRESH_TABLE) ranges")
            conn.execute("CHECKPOINT")
            conn.execute(f"SET sdb_faults = '{fault}'")
            assert conn.execute("DELETE FROM ranges WHERE id < 100").rowcount == 100
        server.kill()

        with pytest.raises(RuntimeError, match="exited"):
            server.start()
        log = server.log.tail(0)
        assert error in log, log[-2000:]
    finally:
        server.stop()
