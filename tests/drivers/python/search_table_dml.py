"""Concurrent DML on search tables: the workload and the checks that they equal a regular table fed the same changes."""

from __future__ import annotations

import os
import random
import threading
import time
from pathlib import Path

import psycopg
import pytest
from serened import Serened


SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)

WORDS = [f"w{i}" for i in range(40)]
ID_SPAN = 1_000_000_000
ROUND_SPAN = 100_000_000

WRITER_CASES = [
    ("refresh_interval = 0, compaction_interval = 0", 6, ""),
    ("refresh_interval = 20, compaction_interval = 10", 6, "index"),
    ("refresh_interval = 5, compaction_interval = 5", 4, "index big_inserts"),
    ("refresh_interval = 10, compaction_interval = 20", 6, "builds"),
    ("refresh_interval = 5, compaction_interval = 5", 4, "index builds big_inserts"),
]


def make_server(tmp_path: Path) -> Serened:
    return Serened(SERENED_BIN, datadir_root=str(tmp_path),
                   log_path=str(tmp_path / "serened.log"))


def body(rng: random.Random) -> str:
    return " ".join(rng.sample(WORDS, 3))


def pause(rng: random.Random) -> None:
    if rng.random() < 0.3:
        time.sleep(rng.random() * 0.03)


def create_schema(conn: psycopg.Connection, options: str, index: bool) -> None:
    conn.execute("CREATE TEXT SEARCH DICTIONARY s_words AS split_text()")
    conn.execute(f"CREATE TABLE s (id BIGINT, body TEXT) WITH (storage = 'search', {options})")
    conn.execute("CREATE TABLE m (id BIGINT, body TEXT)")
    if index:
        conn.execute("CREATE INDEX s_idx ON s USING inverted (id, body s_words)")


def insert_rows(conn: psycopg.Connection, start: int, count: int, salt: int) -> None:
    for table in ("s", "m"):
        conn.execute(
            f"INSERT INTO {table} SELECT %s + i, 'w' || ((i * 7 + %s) %% 40) || ' w' || "
            "((i * 13 + %s) %% 40) FROM range(%s) r(i)",
            (start, salt, salt * 3, count),
        )


def mirror(conn: psycopg.Connection, statement: str, ids: list[int], *args: object) -> None:
    if ids:
        conn.execute(statement.format(ids="(SELECT unnest(%s::BIGINT[]))"), (*args, ids))


def returned_ids(conn: psycopg.Connection, statement: str, args: tuple) -> list[int]:
    return [row[0] for row in conn.execute(statement, args).fetchall()]


def delete_rows(conn: psycopg.Connection, lo: int, hi: int, mod: int, rem: int) -> None:
    ids = returned_ids(
        conn,
        "DELETE FROM s WHERE id >= %s AND id < %s AND id %% 2 = 0 AND id %% %s = %s "
        "RETURNING id",
        (lo, hi, mod, rem),
    )
    mirror(conn, "DELETE FROM m WHERE id IN {ids}", ids)


def update_rows(conn: psycopg.Connection, lo: int, hi: int, mod: int, rem: int,
                text: str) -> None:
    ids = returned_ids(
        conn,
        "UPDATE s SET body = %s WHERE id >= %s AND id < %s AND id %% 2 = 1 AND "
        "id %% %s = %s RETURNING id",
        (text, lo, hi, mod, rem),
    )
    mirror(conn, "UPDATE m SET body = %s WHERE id IN {ids}", ids, text)


def assert_matches_model(conn: psycopg.Connection, index: bool) -> None:
    conn.execute("VACUUM (REFRESH_TABLE) s")
    searched, modeled = conn.execute(
        "SELECT (SELECT count(*) FROM s), (SELECT count(*) FROM m)"
    ).fetchone()
    assert searched == modeled
    extra, missing = conn.execute(
        "SELECT (SELECT count(*) FROM (SELECT id, body FROM s EXCEPT ALL "
        "SELECT id, body FROM m)), (SELECT count(*) FROM (SELECT id, body FROM m "
        "EXCEPT ALL SELECT id, body FROM s))"
    ).fetchone()
    assert (extra, missing) == (0, 0)
    if not index:
        return
    for word in WORDS:
        found = conn.execute(
            "SELECT count(*) FROM s WHERE body @@ ts_phrase(%s)", (word,)
        ).fetchone()[0]
        expected = conn.execute(
            "SELECT count(*) FROM m WHERE list_contains(string_split(body, ' '), %s)",
            (word,),
        ).fetchone()[0]
        assert found == expected, word


class Workload:
    def __init__(self, server: Serened, seed: int, writers: int,
                 big_inserts: bool = False, builds: bool = False,
                 round_: int = 0) -> None:
        self.server = server
        self.seed = seed
        self.round = round_
        self.writers = writers
        self.big_inserts = big_inserts
        self.builds = builds
        self.stop = threading.Event()
        self.errors: list[BaseException] = []
        self.commits = 0
        self.lock = threading.Lock()

    def note(self, error: BaseException) -> None:
        with self.lock:
            self.errors.append(error)

    def counted(self) -> None:
        with self.lock:
            self.commits += 1

    def step(self, conn: psycopg.Connection, rng: random.Random, lo: int,
             next_id: int) -> tuple[int, bool]:
        op = rng.random()
        rollback = rng.random() < 0.15
        if op < 0.25:
            n = rng.randint(1, 200)
            with conn.transaction(force_rollback=rollback):
                insert_rows(conn, next_id, n, rng.randrange(40))
            return next_id + n, False
        if op < 0.32:
            rows = [(next_id + i, body(rng)) for i in range(rng.randint(1, 300))]
            with conn.transaction(force_rollback=rollback):
                for table in ("s", "m"):
                    with conn.cursor().copy(f"COPY {table} (id, body) FROM STDIN") as copy:
                        for row in rows:
                            copy.write_row(row)
            return next_id + len(rows), False
        if op < 0.38 and self.big_inserts:
            n = rng.choice([20_000, 40_000])
            with conn.transaction(force_rollback=rollback):
                insert_rows(conn, next_id, n, rng.randrange(40))
                delete_rows(conn, lo, next_id, 101, rng.randrange(101))
            return next_id + n, True
        if op < 0.5:
            with conn.transaction(force_rollback=rollback):
                update_rows(conn, lo, next_id, 97, rng.randrange(97), body(rng))
            return next_id, True
        if op < 0.62:
            with conn.transaction(force_rollback=rollback):
                delete_rows(conn, lo, next_id, 89, rng.randrange(89))
            return next_id, True
        if op < 0.72:
            with conn.transaction(force_rollback=rollback):
                ids = returned_ids(
                    conn,
                    "DELETE FROM s WHERE id IN (SELECT id FROM s WHERE id >= %s AND "
                    "id < %s ORDER BY id DESC LIMIT %s) RETURNING id",
                    (lo, next_id, rng.randint(1, 20)),
                )
                mirror(conn, "DELETE FROM m WHERE id IN {ids}", ids)
            return next_id, True
        if op < 0.8 and next_id > lo:
            picks = [rng.randrange(lo, next_id) for _ in range(rng.randint(1, 30))]
            with conn.transaction(force_rollback=rollback):
                ids = returned_ids(
                    conn,
                    "DELETE FROM s USING (SELECT unnest(%s::BIGINT[]) AS x) v "
                    "WHERE s.id = v.x RETURNING s.id",
                    (picks,),
                )
                mirror(conn, "DELETE FROM m WHERE id IN {ids}", ids)
            return next_id, True
        n = rng.randint(1, 100)
        with conn.transaction(force_rollback=rollback):
            insert_rows(conn, next_id, n, rng.randrange(40))
            pause(rng)
            delete_rows(conn, lo, next_id, 83, rng.randrange(83))
            pause(rng)
            update_rows(conn, lo, next_id, 79, rng.randrange(79), body(rng))
            pause(rng)
            conn.execute("SELECT count(*) FROM s WHERE id >= %s", (lo,)).fetchone()
        return next_id + n, True

    def writer(self, slot: int) -> None:
        rng = random.Random(self.seed * 1000 + slot)
        lo = (slot + 1) * ID_SPAN
        next_id = lo + self.round * ROUND_SPAN
        stale = False
        try:
            with psycopg.connect(self.server.dsn(), autocommit=True) as conn:
                conn.execute(f"SET threads = {rng.choice([1, 4])}")
                while not self.stop.is_set():
                    if stale:
                        conn.execute("VACUUM (REFRESH_TABLE) s")
                        stale = False
                    try:
                        next_id, stale = self.step(conn, rng, lo, next_id)
                        self.counted()
                    except (psycopg.errors.TransactionRollback,
                            psycopg.errors.SerializationFailure):
                        stale = True
        except BaseException as error:  # noqa: BLE001
            if not self.stop.is_set():
                self.note(error)

    def maintainer(self) -> None:
        rng = random.Random(self.seed)
        built = False
        try:
            with psycopg.connect(self.server.dsn(), autocommit=True) as conn:
                while not self.stop.is_set():
                    op = rng.random()
                    if op < 0.4:
                        conn.execute("VACUUM (REFRESH_TABLE) s")
                    elif op < 0.55:
                        try:
                            conn.execute("CHECKPOINT")
                        except psycopg.errors.SerializationFailure:
                            pass
                    elif op < 0.7:
                        conn.execute("VACUUM (COMPACT_TABLE) s")
                    elif op < 0.8 and self.builds:
                        try:
                            if built:
                                conn.execute("DROP INDEX s_ids")
                            else:
                                conn.execute("CREATE INDEX s_ids ON s USING inverted (id)")
                            built = not built
                        except (psycopg.errors.TransactionRollback,
                                psycopg.errors.SerializationFailure):
                            pass
                    else:
                        conn.execute(
                            "SELECT count(*) FROM s WHERE id %% 3 = %s", (rng.randrange(3),)
                        ).fetchone()
                    time.sleep(rng.random() * 0.05)
        except BaseException as error:  # noqa: BLE001
            if not self.stop.is_set():
                self.note(error)

    def threads(self) -> list[threading.Thread]:
        threads = [threading.Thread(target=self.writer, args=(slot,), daemon=True)
                   for slot in range(self.writers)]
        threads.append(threading.Thread(target=self.maintainer, daemon=True))
        return threads

    def run(self, seconds: float) -> None:
        threads = self.threads()
        for thread in threads:
            thread.start()
        time.sleep(seconds)
        self.stop.set()
        for thread in threads:
            thread.join(timeout=120)
            assert not thread.is_alive(), "a worker hung: the commits deadlocked"
        if self.errors:
            raise self.errors[0]
        assert self.commits > 0


def seed_rows(conn: psycopg.Connection, rows: int) -> None:
    insert_rows(conn, 0, rows, 1)
    conn.execute("VACUUM (REFRESH_TABLE) s")


def concurrent_writers_keep_the_search_table_equal_to_its_model(
    tmp_path: Path, options: str, writers: int, flags: str
) -> None:
    index = "index" in flags
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            create_schema(conn, options, index)
            seed_rows(conn, 10000)
            Workload(server, seed=len(options) + writers + len(flags), writers=writers,
                     big_inserts="big_inserts" in flags,
                     builds="builds" in flags).run(seconds=25)
            assert_matches_model(conn, index)
        server.graceful_stop()
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            assert_matches_model(conn, index)


def a_crash_during_concurrent_writes_replays_to_an_equal_search_table(
    tmp_path: Path, seed: int
) -> None:
    rng = random.Random(seed)
    index = seed % 2 == 0
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            create_schema(conn, "refresh_interval = 50, compaction_interval = 20", index)
            seed_rows(conn, 5000)
        for round_ in range(3):
            workload = Workload(server, seed=rng.randrange(1 << 30), writers=6,
                                big_inserts=rng.random() < 0.5,
                                builds=seed % 3 == 0, round_=round_)
            threads = workload.threads()
            for thread in threads:
                thread.start()
            time.sleep(2 + rng.random() * 4)
            workload.stop.set()
            server.kill()
            for thread in threads:
                thread.join(timeout=60)
                assert not thread.is_alive()
            server.start()
            with psycopg.connect(server.dsn(), autocommit=True) as conn:
                assert_matches_model(conn, index)
