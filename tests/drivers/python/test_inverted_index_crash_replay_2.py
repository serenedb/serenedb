"""A crash during concurrent writes or an online index build replays equal inverted indexes."""

from __future__ import annotations

import random
import threading
import time
from pathlib import Path

import psycopg
import pytest
from inverted_dml import (  # type: ignore[import-untyped]
    T_IDX,
    T_PART,
    Workload,
    a_crash_during_concurrent_writes_replays_to_an_equal_index,
    assert_index_matches_table,
    create_index,
    create_schema,
    make_server,
    pytestmark,
    seed_rows,
)


@pytest.mark.parametrize("seed", [5, 6])
def test_a_crash_during_concurrent_writes_replays_to_an_equal_index(
    tmp_path: Path, seed: int
) -> None:
    a_crash_during_concurrent_writes_replays_to_an_equal_index(tmp_path, seed)


@pytest.mark.parametrize("seed", [1, 2, 3, 4])
def test_a_crash_during_an_online_index_build_replays_to_equal_indexes(
    tmp_path: Path, seed: int
) -> None:
    rng = random.Random(seed)
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            indexes = create_schema(conn, "refresh_interval = 20, compaction_interval = 20")
            seed_rows(conn, indexes, 300_000)
        workload = Workload(server, seed=seed, writers=4, big_updates=seed % 2 == 0)
        threads = workload.threads()
        for thread in threads:
            thread.start()

        def build() -> None:
            try:
                with psycopg.connect(server.dsn(), autocommit=True) as builder:
                    create_index(builder, T_PART,
                                 "refresh_interval = 0, compaction_interval = 0")
            except psycopg.Error:
                pass

        builder = threading.Thread(target=build, daemon=True)
        builder.start()
        time.sleep(0.2 + rng.random() * 3)
        workload.stop.set()
        server.kill()
        builder.join(timeout=60)
        assert not builder.is_alive()
        for thread in threads:
            thread.join(timeout=60)
            assert not thread.is_alive()
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            built = conn.execute(
                "SELECT count(*) FROM pg_class WHERE relname = %s", (T_PART.name,)
            ).fetchone()[0]
            assert_index_matches_table(conn, [T_IDX, T_PART] if built else [T_IDX])
