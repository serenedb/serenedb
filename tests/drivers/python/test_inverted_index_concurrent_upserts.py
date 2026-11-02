"""Concurrent upserts and a partial index rebuilt next to writers keep inverted indexes equal to their tables."""

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
    Index,
    Workload,
    assert_index_matches_table,
    body,
    checkpoint,
    create_index,
    create_schema,
    make_server,
    pytestmark,
    seed_rows,
)


def test_a_partial_index_rebuilt_while_writers_run(tmp_path: Path) -> None:
    rng = random.Random(21)
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            indexes = create_schema(conn, "refresh_interval = 10, compaction_interval = 10")
            seed_rows(conn, indexes, 200_000)
            workload = Workload(server, seed=21, writers=6, big_inserts=True,
                                big_updates=True)
            threads = workload.threads()
            for thread in threads:
                thread.start()
            with psycopg.connect(server.dsn(), autocommit=True) as builder:
                for rebuild in range(6):
                    if rebuild:
                        builder.execute(f"DROP INDEX {T_PART.name}")
                    create_index(builder, T_PART, rng.choice([
                        "refresh_interval = 0, compaction_interval = 0",
                        "refresh_interval = 5, compaction_interval = 5, "
                        "segment_docs_max = 5000",
                    ]))
                    time.sleep(rng.random())
            time.sleep(2)
            workload.stop.set()
            for thread in threads:
                thread.join(timeout=120)
                assert not thread.is_alive()
            if workload.errors:
                raise workload.errors[0]
            assert_index_matches_table(conn, [T_IDX, T_PART])
        server.graceful_stop()
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            assert_index_matches_table(conn, [T_IDX, T_PART])


@pytest.mark.parametrize(
    "options",
    [
        "refresh_interval = 0, compaction_interval = 0",
        "refresh_interval = 5, compaction_interval = 5, segment_docs_max = 64",
    ],
)
def test_concurrent_upserts_keep_the_index_equal_to_the_table(
    tmp_path: Path, options: str
) -> None:
    keys = 5000
    index = Index("v_idx", "v")
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            conn.execute("CREATE TEXT SEARCH DICTIONARY t_words AS split_text()")
            conn.execute("CREATE TABLE v (id BIGINT PRIMARY KEY, body VARCHAR)")
            create_index(conn, index, options)
            conn.execute(
                "INSERT INTO v SELECT i, 'w' || (i %% 40) FROM range(%s) r(i)", (keys,)
            )
        stop = threading.Event()
        errors: list[BaseException] = []
        done = [0] * 6

        def writer(slot: int) -> None:
            rng = random.Random(slot)
            try:
                with psycopg.connect(server.dsn(), autocommit=True) as c:
                    while not stop.is_set():
                        op = rng.random()
                        try:
                            if op < 0.4:
                                rows = [(key, body(rng)) for key in
                                        rng.sample(range(keys * 2), rng.randint(1, 50))]
                                c.execute(
                                    "INSERT INTO v SELECT * FROM (VALUES "
                                    + ", ".join(["(%s::BIGINT, %s)"] * len(rows))
                                    + ") ON CONFLICT (id) DO UPDATE SET body = excluded.body",
                                    [value for row in rows for value in row],
                                )
                            elif op < 0.55:
                                c.execute(
                                    "INSERT OR REPLACE INTO v VALUES (%s, %s)",
                                    (rng.randrange(keys * 2), body(rng)),
                                )
                            elif op < 0.65:
                                first = rng.randrange(keys * 2)
                                c.execute(
                                    "INSERT INTO v SELECT i, %s FROM range(%s, %s) r(i) "
                                    "ON CONFLICT DO NOTHING",
                                    (body(rng), first, first + 100),
                                )
                            elif op < 0.8:
                                c.execute(
                                    "DELETE FROM v WHERE id %% 211 = %s",
                                    (rng.randrange(211),),
                                )
                            elif op < 0.9:
                                c.execute(
                                    "UPDATE v SET body = %s WHERE id %% 97 = %s",
                                    (body(rng), rng.randrange(97)),
                                )
                            else:
                                with c.transaction(force_rollback=rng.random() < 0.3):
                                    key = rng.randrange(keys * 2)
                                    c.execute("DELETE FROM v WHERE id = %s", (key,))
                                    c.execute("INSERT INTO v VALUES (%s, %s)",
                                              (key, body(rng)))
                                    c.execute(
                                        "INSERT INTO v VALUES (%s, %s) "
                                        "ON CONFLICT (id) DO UPDATE SET body = excluded.body",
                                        (key, body(rng)),
                                    )
                            done[slot] += 1
                        except (psycopg.errors.TransactionRollback,
                                psycopg.errors.SerializationFailure,
                                psycopg.errors.IntegrityError):
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
                        if op < 0.5:
                            c.execute("VACUUM (REFRESH_TABLE) v")
                        elif op < 0.7:
                            checkpoint(c)
                        elif op < 0.8:
                            c.execute("VACUUM (COMPACT_TABLE) v")
                        time.sleep(rng.random() * 0.05)
            except BaseException as error:  # noqa: BLE001
                if not stop.is_set():
                    errors.append(error)

        threads = [threading.Thread(target=writer, args=(slot,), daemon=True)
                   for slot in range(6)]
        threads.append(threading.Thread(target=maintainer, daemon=True))
        for thread in threads:
            thread.start()
        time.sleep(20)
        stop.set()
        for thread in threads:
            thread.join(timeout=120)
            assert not thread.is_alive(), "a worker hung"
        if errors:
            raise errors[0]
        assert sum(done) > 100
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            assert_index_matches_table(conn, [index])
        server.kill()
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            assert_index_matches_table(conn, [index])
