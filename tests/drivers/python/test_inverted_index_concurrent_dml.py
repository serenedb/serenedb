"""Concurrent DML on regular tables keeps their inverted indexes equal to the tables."""

from __future__ import annotations

import os
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pytest
from serened import Serened  # type: ignore[import-untyped]


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


@pytest.mark.parametrize(
    ("options", "writers", "flags"),
    [
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
    ],
)
def test_concurrent_writers_keep_the_index_equal_to_the_table(
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


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5, 6])
def test_a_crash_during_concurrent_writes_replays_to_an_equal_index(
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


def test_a_statement_prepared_before_an_online_build_reaches_the_index(
    tmp_path: Path,
) -> None:
    with make_server(tmp_path) as server:
        server.start()
        with psycopg.connect(server.dsn(), autocommit=True) as conn, \
                psycopg.connect(server.dsn(), autocommit=True) as writer:
            conn.execute("CREATE TEXT SEARCH DICTIONARY t_words AS split_text()")
            conn.execute("CREATE TABLE t (id BIGINT, body VARCHAR)")
            conn.execute("INSERT INTO t SELECT i, 'w' || (i % 4) FROM range(4000) r(i)")
            update = "UPDATE t SET body = %s WHERE id %% 4 = %s"
            writer.execute(update, ("w10", 0), prepare=True)
            conn.execute("SET sdb_faults = 'pause_create_index_before_commit'")
            errors: list[BaseException] = []

            def build() -> None:
                try:
                    with psycopg.connect(server.dsn(), autocommit=True) as builder:
                        create_index(builder, T_IDX,
                                     "refresh_interval = 0, compaction_interval = 0")
                except BaseException as error:
                    errors.append(error)

            builder = threading.Thread(target=build)
            builder.start()
            deadline = time.monotonic() + 120
            while conn.execute(
                "SELECT count(*) FROM pg_stat_progress_create_index "
                "WHERE relid = 't'::regclass AND phase = 'finalizing'"
            ).fetchone()[0] == 0:
                assert time.monotonic() < deadline and builder.is_alive()
                time.sleep(0.05)
            writer.execute(update, ("w11", 1), prepare=True)
            conn.execute("SET sdb_faults = '-pause_create_index_before_commit'")
            builder.join(timeout=600)
            assert not builder.is_alive()
            if errors:
                raise errors[0]
            assert phrase_count(conn, "w11") == 1000
            assert_index_matches_table(conn)


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
