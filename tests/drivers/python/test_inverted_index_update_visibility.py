"""Inverted indexes of regular tables never show half an update and keep unpublished deletes."""

from __future__ import annotations

import random
import threading
import time
from pathlib import Path

import psycopg
import pytest
from inverted_dml import (  # type: ignore[import-untyped]
    assert_index_matches_table,
    body,
    checkpoint,
    create_schema,
    live_docs,
    make_server,
    phrase_count,
    pytestmark,
)


def test_deletes_of_unpublished_rows_land_with_them(tmp_path: Path) -> None:
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            create_schema(conn, "refresh_interval = 0, compaction_interval = 0")
            conn.execute("INSERT INTO t SELECT i, 'w1 w2' FROM range(100) r(i)")
            conn.execute("INSERT INTO t SELECT 100 + i, 'w3' FROM range(100) r(i)")
            assert conn.execute(
                "DELETE FROM t WHERE id < 50 OR id >= 150"
            ).rowcount == 100
            conn.execute("UPDATE t SET body = 'w4' WHERE id >= 100 AND id < 110")
            assert phrase_count(conn, "w3") == 0
            assert live_docs(conn) == 0
            assert_index_matches_table(conn)
            assert phrase_count(conn, "w1") == 50
            assert phrase_count(conn, "w3") == 40
            assert phrase_count(conn, "w4") == 10


def test_unpublished_deletes_survive_a_checkpoint_and_a_crash(
    tmp_path: Path,
) -> None:
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            create_schema(conn, "refresh_interval = 0, compaction_interval = 0")
            conn.execute("INSERT INTO t SELECT i, 'w1' FROM range(3000) r(i)")
            conn.execute("DELETE FROM t WHERE id % 3 = 0")
            conn.execute("UPDATE t SET body = 'w2' WHERE id % 3 = 1")
            conn.execute("CHECKPOINT")
            assert live_docs(conn) == 2000
            conn.execute("DELETE FROM t WHERE id % 3 = 2 AND id < 1500")
            conn.execute("INSERT INTO t SELECT 5000 + i, 'w3' FROM range(10) r(i)")
            conn.execute("DELETE FROM t WHERE id >= 5005")
        server.kill()
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            assert_index_matches_table(conn)
            assert live_docs(conn) == 1505


@pytest.mark.parametrize(
    ("options", "crash"),
    [
        ("refresh_interval = 0, compaction_interval = 0", False),
        ("refresh_interval = 5, compaction_interval = 5, segment_docs_max = 64", False),
        ("refresh_interval = 5, compaction_interval = 5, segment_docs_max = 64", True),
    ],
)
def test_an_update_is_never_half_visible(
    tmp_path: Path, options: str, crash: bool
) -> None:
    rows = 2000
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            create_schema(conn, options)
            conn.execute(
                "INSERT INTO t SELECT i, 'w' || (i %% 40) FROM range(%s) r(i)", (rows,)
            )
            conn.execute("VACUUM (REFRESH_TABLE) t")
            assert live_docs(conn) == rows
        stop = threading.Event()
        errors: list[BaseException] = []
        seen: list[int] = []

        def updater(slot: int) -> None:
            rng = random.Random(slot)
            try:
                with psycopg.connect(server.dsn(), autocommit=True) as c:
                    while not stop.is_set():
                        ids = rng.sample(range(rows), rng.randint(1, 30))
                        try:
                            c.execute(
                                "UPDATE t SET body = %s WHERE id = ANY(%s)",
                                (body(rng), ids),
                            )
                        except psycopg.errors.TransactionRollback:
                            pass
                        except psycopg.Error as error:
                            if "onflict" not in str(error):
                                raise
            except BaseException as error:  # noqa: BLE001
                if not stop.is_set():
                    errors.append(error)

        def maintainer() -> None:
            rng = random.Random(99)
            try:
                with psycopg.connect(server.dsn(), autocommit=True) as c:
                    while not stop.is_set():
                        op = rng.random()
                        if op < 0.6:
                            c.execute("VACUUM (REFRESH_TABLE) t")
                        elif op < 0.8:
                            checkpoint(c)
                        else:
                            c.execute("VACUUM (COMPACT_TABLE) t")
            except BaseException as error:  # noqa: BLE001
                if not stop.is_set():
                    errors.append(error)

        def checker() -> None:
            try:
                with psycopg.connect(server.dsn(), autocommit=True) as c:
                    while not stop.is_set():
                        seen.append(live_docs(c))
            except BaseException as error:  # noqa: BLE001
                if not stop.is_set():
                    errors.append(error)

        threads = [threading.Thread(target=updater, args=(slot,), daemon=True)
                   for slot in range(6)]
        threads += [threading.Thread(target=maintainer, daemon=True),
                    threading.Thread(target=checker, daemon=True)]
        for thread in threads:
            thread.start()
        time.sleep(20)
        stop.set()
        if crash:
            server.kill()
        for thread in threads:
            thread.join(timeout=120)
            assert not thread.is_alive(), "a worker hung"
        if errors:
            raise errors[0]
        assert len(seen) > 10
        assert set(seen) == {rows}
        if crash:
            server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            assert_index_matches_table(conn)
            assert live_docs(conn) == rows
