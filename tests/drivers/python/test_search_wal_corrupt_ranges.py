"""Replay refuses a search-table row-range delete record it can't trust."""

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

FAULT = "search_delete_wal_empty_range"

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


def test_empty_row_range_refuses_replay(tmp_path: Path) -> None:
    server = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "serened.log"))
    try:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            try:
                conn.execute(f"SET sdb_faults = '{FAULT}'")
                conn.execute(f"SET sdb_faults = '-{FAULT}'")
            except psycopg.Error:
                pytest.skip("serened is built without fault injection")
            conn.execute("CREATE TABLE ranges (id BIGINT) WITH (storage = 'search')")
            conn.execute("INSERT INTO ranges SELECT i FROM range(0, 1000) r(i)")
            conn.execute("VACUUM (REFRESH_TABLE) ranges")
            conn.execute("CHECKPOINT")
            conn.execute(f"SET sdb_faults = '{FAULT}'")
            assert conn.execute("DELETE FROM ranges WHERE id < 100").rowcount == 100
        server.kill()

        with pytest.raises(RuntimeError, match="exited"):
            server.start()
        log = server.log.tail(0)
        assert "Corrupt WAL: row range out of bounds" in log, log[-2000:]
    finally:
        server.stop()
