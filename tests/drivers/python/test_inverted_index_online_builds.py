"""An online index build next to writers ends with an inverted index equal to its table."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import psycopg
import pytest
from inverted_dml import (  # type: ignore[import-untyped]
    T_IDX,
    T_PART,
    Workload,
    assert_index_matches_table,
    create_index,
    make_server,
    pytestmark,
)


@pytest.mark.parametrize("partial", [False, True])
def test_deletes_during_an_online_index_build_reach_the_index(
    tmp_path: Path, partial: bool
) -> None:
    index = T_PART if partial else T_IDX
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            conn.execute("CREATE TEXT SEARCH DICTIONARY t_words AS split_text()")
            conn.execute("CREATE TABLE t (id BIGINT, body VARCHAR)")
            conn.execute(
                "INSERT INTO t SELECT i, 'w' || (i % 40) || ' w' || ((i * 11) % 40) "
                "FROM range(400000) r(i)"
            )
            workload = Workload(server, seed=7, writers=6, big_updates=partial)

            def build() -> None:
                with psycopg.connect(server.dsn(), autocommit=True) as builder:
                    create_index(builder, index,
                                 "refresh_interval = 0, compaction_interval = 0")

            threads = [threading.Thread(target=workload.writer, args=(slot,), daemon=True)
                       for slot in range(workload.writers)]
            for thread in threads:
                thread.start()
            builder = threading.Thread(target=build)
            builder.start()
            builder.join(timeout=600)
            assert not builder.is_alive()
            time.sleep(2)
            workload.stop.set()
            for thread in threads:
                thread.join(timeout=120)
                assert not thread.is_alive()
            if workload.errors:
                raise workload.errors[0]
            assert_index_matches_table(conn, [index])


def test_online_index_builds_next_to_writers_and_a_live_index(tmp_path: Path) -> None:
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            conn.execute("CREATE TEXT SEARCH DICTIONARY t_words AS split_text()")
            conn.execute("CREATE TABLE t (id BIGINT, body VARCHAR)")
            conn.execute(
                "INSERT INTO t SELECT i, 'w' || (i % 40) || ' w' || ((i * 11) % 40) "
                "FROM range(300000) r(i)"
            )
            workload = Workload(server, seed=11, writers=6, big_updates=True)
            threads = [threading.Thread(target=workload.writer, args=(slot,), daemon=True)
                       for slot in range(workload.writers)]
            for thread in threads:
                thread.start()
            with psycopg.connect(server.dsn(), autocommit=True) as builder:
                for index in (T_IDX, T_PART):
                    create_index(builder, index,
                                 "refresh_interval = 10, compaction_interval = 10")
            time.sleep(3)
            workload.stop.set()
            for thread in threads:
                thread.join(timeout=120)
                assert not thread.is_alive()
            if workload.errors:
                raise workload.errors[0]
            assert_index_matches_table(conn, [T_IDX, T_PART])
