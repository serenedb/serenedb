"""Concurrent DML on regular tables: the workload and the checks that their inverted indexes equal them."""

from __future__ import annotations

import os
import random
import threading
import time
from dataclasses import dataclass
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

WRITER_CASES = [
    ("refresh_interval = 0, compaction_interval = 0", 8, ""),
    ("refresh_interval = 20, compaction_interval = 10, segment_docs_max = 64", 8, ""),
    ("refresh_interval = 0, compaction_interval = 5, segment_docs_max = 1000", 4,
     "big_inserts"),
    ("refresh_interval = 10, compaction_interval = 10", 6, "partial second"),
    ("refresh_interval = 0, compaction_interval = 0", 4, "big_inserts big_updates"),
    ("refresh_interval = 5, compaction_interval = 5, segment_docs_max = 32", 8,
     "partial second big_updates"),
    ("refresh_interval = 0, compaction_interval = 5", 6,
     "second big_inserts big_updates"),
]


@dataclass(frozen=True)
class Index:
    name: str
    table: str
    where: str = "true"


T_IDX = Index("t_idx", "t")
T_PART = Index("t_part", "t", "id % 2 = 0")
U_IDX = Index("u_idx", "u")


def make_server(tmp_path: Path) -> Serened:
    return Serened(SERENED_BIN, datadir_root=str(tmp_path),
                   log_path=str(tmp_path / "serened.log"))


def body(rng: random.Random) -> str:
    return " ".join(rng.sample(WORDS, 3))


def pause(rng: random.Random) -> None:
    if rng.random() < 0.3:
        time.sleep(rng.random() * 0.03)


def create_index(conn: psycopg.Connection, index: Index, options: str) -> None:
    where = "" if index.where == "true" else f" WHERE {index.where}"
    conn.execute(
        f"CREATE INDEX {index.name} ON {index.table} "
        f"USING inverted(id, body t_words) WITH ({options}){where}"
    )


def create_schema(conn: psycopg.Connection, options: str, partial: bool = False,
                  second: bool = False) -> list[Index]:
    conn.execute("CREATE TEXT SEARCH DICTIONARY t_words AS split_text()")
    conn.execute("CREATE TABLE t (id BIGINT, body VARCHAR)")
    indexes = [T_IDX]
    if partial:
        indexes.append(T_PART)
    if second:
        conn.execute("CREATE TABLE u (id BIGINT, body VARCHAR)")
        conn.execute("CREATE INDEX u_id ON u (id)")
        indexes.append(U_IDX)
    for index in indexes:
        create_index(conn, index, options)
    return indexes


def checkpoint(conn: psycopg.Connection) -> None:
    try:
        conn.execute("CHECKPOINT")
    except psycopg.errors.SerializationFailure:
        pass


def live_docs(conn: psycopg.Connection, index: Index = T_IDX) -> int:
    row = conn.execute(
        "SELECT value FROM sdb_metrics WHERE metric = 'num_live_docs' AND "
        "relation_id = (SELECT oid FROM pg_class WHERE relname = %s)",
        (index.name,),
    ).fetchone()
    return int(row[0])


def phrase_count(conn: psycopg.Connection, word: str, index: Index = T_IDX) -> int:
    return conn.execute(
        f"SELECT count(*) FROM {index.name} WHERE body @@ ts_phrase(%s)", (word,)
    ).fetchone()[0]


def assert_index_matches_table(conn: psycopg.Connection,
                               indexes: list[Index] | None = None) -> None:
    indexes = indexes or [T_IDX]
    for table in sorted({index.table for index in indexes}):
        conn.execute(f"VACUUM (REFRESH_TABLE) {table}")
    for index in indexes:
        rows = conn.execute(
            f"SELECT count(*) FROM {index.table} WHERE {index.where}"
        ).fetchone()[0]
        assert live_docs(conn, index) == rows, index.name
        where = index.where.replace("%", "%%")
        for word in WORDS:
            scanned = conn.execute(
                f"SELECT count(*) FROM {index.table} WHERE {where} AND "
                "list_contains(string_split(body, ' '), %s)",
                (word,),
            ).fetchone()[0]
            assert phrase_count(conn, word, index) == scanned, (index.name, word)


class Workload:
    def __init__(self, server: Serened, seed: int, writers: int,
                 big_inserts: bool = False, big_updates: bool = False,
                 second: bool = False) -> None:
        self.server = server
        self.seed = seed
        self.writers = writers
        self.big_inserts = big_inserts
        self.big_updates = big_updates
        self.second = second
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

    def step(self, conn: psycopg.Connection, rng: random.Random, next_id: int) -> int:
        op = rng.random()
        if op < 0.2:
            n = rng.randint(1, 200)
            conn.execute(
                "INSERT INTO t SELECT %s + i, %s FROM range(%s) r(i)",
                (next_id, body(rng), n),
            )
            return next_id + n
        if op < 0.25:
            n = rng.randint(1, 300)
            with conn.cursor().copy("COPY t (id, body) FROM STDIN") as copy:
                for i in range(n):
                    copy.write_row((next_id + i, body(rng)))
            return next_id + n
        if op < 0.29 and self.big_inserts:
            n = rng.choice([20_000, 40_000])
            with conn.transaction(force_rollback=rng.random() < 0.2):
                conn.execute(
                    "INSERT INTO t SELECT %s + i, 'w' || (i %% 40) || ' w' || "
                    "((i * 3) %% 40) FROM range(%s) r(i)",
                    (next_id, n),
                )
                conn.execute(
                    "DELETE FROM t WHERE id %% 101 = %s AND id < %s",
                    (rng.randrange(101), next_id),
                )
                conn.execute(
                    "UPDATE t SET body = %s WHERE id %% 89 = %s AND id < %s",
                    (body(rng), rng.randrange(89), next_id),
                )
                conn.execute(
                    "DELETE FROM t WHERE id >= %s AND id < %s",
                    (next_id + n // 3, next_id + n // 2),
                )
            return next_id + n
        if op < 0.31 and self.big_inserts:
            n = rng.choice([20_000, 40_000])
            table = "u" if self.second and rng.random() < 0.5 else "t"
            conn.execute(
                f"INSERT INTO {table} SELECT %s + i, 'w' || (i %% 40) || ' w' || "
                "((i * 7) %% 40) || ' w' || ((i * 13) %% 40) FROM range(%s) r(i)",
                (next_id, n),
            )
            return next_id + n
        if op < 0.35 and self.big_updates:
            table = "u" if self.second and rng.random() < 0.5 else "t"
            conn.execute(
                f"UPDATE {table} SET body = %s WHERE id %% 3 = %s",
                (body(rng), rng.randrange(3)),
            )
            return next_id
        if op < 0.45:
            conn.execute(
                "UPDATE t SET body = %s WHERE id %% 97 = %s",
                (body(rng), rng.randrange(97)),
            )
            return next_id
        if op < 0.55:
            conn.execute(
                "DELETE FROM t WHERE id %% 101 = %s AND random() < 0.5",
                (rng.randrange(101),),
            )
            return next_id
        if op < 0.65:
            conn.execute(
                "DELETE FROM t WHERE id IN (SELECT id FROM t ORDER BY id DESC LIMIT %s)",
                (rng.randint(1, 20),),
            )
            return next_id
        if op < 0.73:
            conn.execute(
                "UPDATE t SET body = %s WHERE id IN "
                "(SELECT id FROM t ORDER BY id DESC LIMIT %s)",
                (body(rng), rng.randint(1, 20)),
            )
            return next_id
        if op < 0.85 and self.second:
            with conn.transaction(force_rollback=rng.random() < 0.2):
                n = rng.randint(1, 100)
                conn.execute(
                    "INSERT INTO u SELECT %s + i, %s FROM range(%s) r(i)",
                    (next_id, body(rng), n),
                )
                conn.execute(
                    "UPDATE u SET body = %s WHERE id %% 13 = %s",
                    (body(rng), rng.randrange(13)),
                )
                conn.execute(
                    "DELETE FROM t WHERE id %% 103 = %s AND random() < 0.5",
                    (rng.randrange(103),),
                )
                conn.execute(
                    "DELETE FROM u WHERE id IN (SELECT id FROM u ORDER BY id DESC LIMIT %s)",
                    (rng.randint(1, 10),),
                )
            return next_id + n
        with conn.transaction(force_rollback=rng.random() < 0.3):
            n = rng.randint(1, 50)
            conn.execute(
                "INSERT INTO t SELECT %s + i, %s FROM range(%s) r(i)",
                (next_id, body(rng), n),
            )
            pause(rng)
            conn.execute(
                "DELETE FROM t WHERE id >= %s AND id < %s", (next_id, next_id + n // 2)
            )
            pause(rng)
            conn.execute(
                "UPDATE t SET body = %s WHERE id %% 89 = %s",
                (body(rng), rng.randrange(89)),
            )
            pause(rng)
            conn.execute(
                "DELETE FROM t WHERE id %% 83 = %s AND random() < 0.5",
                (rng.randrange(83),),
            )
        return next_id + n

    def writer(self, slot: int) -> None:
        rng = random.Random(self.seed * 1000 + slot)
        next_id = (slot + 1) * ID_SPAN
        try:
            with psycopg.connect(self.server.dsn(), autocommit=True) as conn:
                while not self.stop.is_set():
                    try:
                        next_id = self.step(conn, rng, next_id)
                        self.counted()
                    except psycopg.errors.TransactionRollback:
                        pass
                    except psycopg.Error as error:
                        if "onflict" in str(error):
                            continue
                        raise
        except BaseException as error:  # noqa: BLE001
            if not self.stop.is_set():
                self.note(error)

    def maintainer(self) -> None:
        rng = random.Random(self.seed)
        tables = ["t", "u"] if self.second else ["t"]
        try:
            with psycopg.connect(self.server.dsn(), autocommit=True) as conn:
                while not self.stop.is_set():
                    op = rng.random()
                    if op < 0.45:
                        conn.execute(f"VACUUM (REFRESH_TABLE) {rng.choice(tables)}")
                    elif op < 0.6:
                        checkpoint(conn)
                    elif op < 0.7:
                        conn.execute(f"VACUUM (COMPACT_TABLE) {rng.choice(tables)}")
                    elif op < 0.72:
                        try:
                            conn.execute(
                                f"DELETE FROM {rng.choice(tables)} WHERE id %% 7 = %s",
                                (rng.randrange(7),),
                            )
                        except psycopg.errors.TransactionRollback:
                            pass
                        except psycopg.Error as error:
                            if "onflict" not in str(error):
                                raise
                    else:
                        conn.execute(
                            "SELECT count(*) FROM t_idx WHERE body @@ ts_phrase(%s)",
                            (rng.choice(WORDS),),
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


def seed_rows(conn: psycopg.Connection, indexes: list[Index], rows: int) -> None:
    for table in sorted({index.table for index in indexes}):
        conn.execute(
            f"INSERT INTO {table} SELECT i, 'w' || (i %% 40) || ' w' || "
            "((i * 3) %% 40) FROM range(%s) r(i)",
            (rows,),
        )


def concurrent_writers_keep_the_index_equal_to_the_table(
    tmp_path: Path, options: str, writers: int, flags: str
) -> None:
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            indexes = create_schema(conn, options, partial="partial" in flags,
                                    second="second" in flags)
            seed_rows(conn, indexes, 10000)
            Workload(server, seed=len(options) + writers + len(flags), writers=writers,
                     big_inserts="big_inserts" in flags,
                     big_updates="big_updates" in flags,
                     second="second" in flags).run(seconds=25)
            assert_index_matches_table(conn, indexes)
        server.graceful_stop()
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            assert_index_matches_table(conn, indexes)


def a_crash_during_concurrent_writes_replays_to_an_equal_index(
    tmp_path: Path, seed: int
) -> None:
    rng = random.Random(seed)
    partial = seed % 2 == 0
    second = seed % 3 == 0
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn:
            indexes = create_schema(
                conn, "refresh_interval = 50, compaction_interval = 20, "
                      "segment_docs_max = 256", partial=partial, second=second)
            seed_rows(conn, indexes, 5000)
        for _ in range(3):
            workload = Workload(server, seed=rng.randrange(1 << 30), writers=6,
                                big_inserts=rng.random() < 0.5,
                                big_updates=rng.random() < 0.3, second=second)
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
                assert_index_matches_table(conn, indexes)
