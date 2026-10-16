"""Hot backup under a concurrent workload: copy, restore, check against the clients."""

from __future__ import annotations

import os
import random
import shutil
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
import pytest
from serened import Serened  # type: ignore[import-untyped]


SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

WORKERS = 4
WARMUP_SECONDS = 2.0
TAIL_SECONDS = 1.0
SEARCH_ID_STRIDE = 1_000_000


def _have_binary() -> bool:
    return Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)


pytestmark = pytest.mark.skipif(
    not _have_binary(),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


def _connect(server: Serened, database: str = "postgres") -> psycopg.Connection:
    return psycopg.connect(server.dsn(database), autocommit=True)


@dataclass
class Txn:
    kind: str
    txid: int
    rows: int = 0
    cross: bool = False
    target: int | None = None
    sent: float | None = None
    acked: float | None = None


@dataclass
class Journal:
    lock: threading.Lock = field(default_factory=threading.Lock)
    txns: dict[int, Txn] = field(default_factory=dict)
    next_txid: int = 1
    inserts: list[int] = field(default_factory=list)
    tables: list[int] = field(default_factory=list)
    databases: list[int] = field(default_factory=list)

    def new(self, kind: str) -> Txn:
        with self.lock:
            txn = Txn(kind=kind, txid=self.next_txid)
            self.next_txid += 1
            self.txns[txn.txid] = txn
            return txn

    def take(self, pool: list[int], rng: random.Random) -> int | None:
        with self.lock:
            if not pool:
                return None
            return pool.pop(rng.randrange(len(pool)))

    def done(self, txn: Txn) -> None:
        with self.lock:
            if txn.kind == "insert":
                self.inserts.append(txn.txid)
            elif txn.kind == "create":
                self.tables.append(txn.txid)
            elif txn.kind == "fill":
                self.databases.append(txn.target)


def _schema(server: Serened) -> None:
    with _connect(server) as conn:
        conn.execute("CREATE DATABASE db2")
        conn.execute("CREATE TABLE ledger (txid BIGINT, body TEXT)")
        conn.execute("CREATE INDEX ledger_idx ON ledger USING inverted (body)")
        conn.execute(
            "CREATE TABLE ledger_s (id BIGINT PRIMARY KEY, txid BIGINT) "
            "WITH (storage = 'search')")
        conn.execute("CREATE TABLE witness (txid BIGINT, target BIGINT, kind TEXT)")
    with _connect(server, "db2") as conn:
        conn.execute("CREATE TABLE ledger2 (txid BIGINT, body TEXT)")


def _statements(txn: Txn, journal: Journal, rng: random.Random) -> list[str] | None:
    t = txn.txid
    if txn.kind == "insert":
        txn.rows = rng.choice([1, 3, 7, rng.randint(2000, 6000)])
        txn.cross = rng.random() < 0.4
        statements = [
            f"INSERT INTO ledger SELECT {t}, 'orig' FROM range({txn.rows})",
            f"INSERT INTO ledger_s SELECT {t * SEARCH_ID_STRIDE} + i, {t} "
            f"FROM range({txn.rows}) r(i)",
        ]
        if txn.cross:
            statements.append(
                f"INSERT INTO db2.ledger2 SELECT {t}, 'orig' FROM range({txn.rows})")
        return statements
    if txn.kind in ("update", "delete"):
        target = journal.take(journal.inserts, rng)
        if target is None:
            return None
        txn.target = target
        source = journal.txns[target]
        tables = ["ledger"] + (["db2.ledger2"] if source.cross else [])
        if txn.kind == "update":
            statements = [f"UPDATE {table} SET body = 'upd' WHERE txid = {target}"
                          for table in tables]
        else:
            statements = [f"DELETE FROM {table} WHERE txid = {target}" for table in tables]
        statements.append(f"INSERT INTO witness VALUES ({t}, {target}, '{txn.kind}')")
        return statements
    if txn.kind == "create":
        return [f"CREATE TABLE ddl_{t} (x BIGINT)", f"INSERT INTO ddl_{t} VALUES ({t})"]
    target = journal.take(journal.tables, rng)
    if target is None:
        return None
    txn.target = target
    return [f"DROP TABLE ddl_{target}", f"INSERT INTO witness VALUES ({t}, {target}, 'drop')"]


def _commit(conn: psycopg.Connection, journal: Journal, txn: Txn,
            statements: list[str], errors: list[str]) -> bool:
    try:
        conn.execute("BEGIN")
        for statement in statements:
            conn.execute(statement)
    except psycopg.Error:
        conn.execute("ROLLBACK")
        return False
    txn.sent = time.monotonic()
    try:
        conn.execute("COMMIT")
    except psycopg.Error as error:
        errors.append(f"txn {txn.txid} ({txn.kind}): {error}")
        return False
    txn.acked = time.monotonic()
    journal.done(txn)
    return True


def _create_database(server: Serened, conn: psycopg.Connection, journal: Journal,
                     errors: list[str]) -> None:
    create = journal.new("createdb")
    t = create.txid
    if not _commit(conn, journal, create,
                   [f"CREATE DATABASE db_{t}",
                    f"INSERT INTO witness VALUES ({t}, {t}, 'createdb')"], errors):
        return
    fill = journal.new("fill")
    fill.target = t
    with _connect(server, f"db_{t}") as database:
        _commit(database, journal, fill,
                ["CREATE TABLE t (x BIGINT)", f"INSERT INTO t VALUES ({fill.txid})"],
                errors)


def _drop_database(conn: psycopg.Connection, journal: Journal, rng: random.Random,
                   errors: list[str]) -> None:
    target = journal.take(journal.databases, rng)
    if target is None:
        return
    drop = journal.new("dropdb")
    drop.target = target
    drop.sent = time.monotonic()
    try:
        conn.execute(f"DROP DATABASE db_{target}")
    except psycopg.Error as error:
        errors.append(f"txn {drop.txid} (dropdb): {error}")
        return
    drop.acked = time.monotonic()


def _worker(server: Serened, journal: Journal, seed: int, stop: threading.Event,
            errors: list[str]) -> None:
    rng = random.Random(seed)
    kinds = ["insert"] * 6 + ["update", "delete", "create", "drop", "createdb", "dropdb"]
    with _connect(server) as conn:
        while not stop.is_set():
            kind = rng.choice(kinds)
            if kind == "createdb":
                _create_database(server, conn, journal, errors)
                continue
            if kind == "dropdb":
                _drop_database(conn, journal, rng, errors)
                continue
            txn = journal.new(kind)
            statements = _statements(txn, journal, rng)
            if statements is not None:
                _commit(conn, journal, txn, statements, errors)


def _maintenance(server: Serened, stop: threading.Event) -> None:
    with _connect(server) as conn:
        while not stop.wait(0.2):
            for statement in ("VACUUM (REFRESH_TABLE) ledger_s",
                              "VACUUM (REFRESH_INDEX) ledger_idx", "CHECKPOINT"):
                try:
                    conn.execute(statement)
                except psycopg.Error:
                    pass


def _copy_file(source: Path, target: Path, rng: random.Random) -> None:
    try:
        size = source.stat().st_size
        with open(source, "rb") as src, open(target, "wb") as dst:
            copied = 0
            while copied < size:
                chunk = src.read(min(size - copied, rng.choice([4096, 65536, 1 << 20])))
                if not chunk:
                    break
                dst.write(chunk)
                copied += len(chunk)
                if rng.random() < 0.05:
                    time.sleep(0.01)
    except FileNotFoundError:
        target.unlink(missing_ok=True)


def _copy_tree(source: Path, target: Path, rng: random.Random) -> None:
    files: list[Path] = []
    for root, dirs, names in os.walk(source):
        relative = Path(root).relative_to(source)
        (target / relative).mkdir(parents=True, exist_ok=True)
        files.extend(relative / name for name in names)
    rng.shuffle(files)
    late = [f for f in files if ".wal" in f.name]
    for relative in [f for f in files if ".wal" not in f.name]:
        _copy_file(source / relative, target / relative, rng)
    time.sleep(0.5)
    for relative in late:
        _copy_file(source / relative, target / relative, rng)


@dataclass
class State:
    ledger: Counter
    ledger2: Counter
    search: Counter
    index: Counter
    witness: dict[int, tuple[int, str]]
    ddl: dict[int, list[int]]
    databases: dict[int, list[int] | None]


def _read_state(server: Serened) -> State:
    with _connect(server) as conn:
        conn.execute("VACUUM (REFRESH_TABLE) ledger_s")
        conn.execute("VACUUM (REFRESH_INDEX) ledger_idx")
        ledger = Counter({(t, b): n for t, b, n in conn.execute(
            "SELECT txid, body, count(*) FROM ledger GROUP BY ALL")})
        search = Counter({t: n for t, n in conn.execute(
            "SELECT txid, count(*) FROM ledger_s GROUP BY ALL")})
        index = Counter()
        for body in ("orig", "upd"):
            for t, n in conn.execute(
                    f"SELECT txid, count(*) FROM ledger_idx WHERE body @@ '{body}' "
                    "GROUP BY ALL"):
                index[(t, body)] = n
        witness = {t: (target, kind) for t, target, kind in conn.execute(
            "SELECT txid, target, kind FROM witness")}
        ddl = {}
        for (name,) in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog = 'postgres' AND starts_with(table_name, 'ddl_')"):
            ddl[int(name[4:])] = [x for (x,) in conn.execute(f"SELECT x FROM {name}")]
        names = [name for (name,) in conn.execute(
            "SELECT database_name FROM duckdb_databases() "
            "WHERE starts_with(database_name, 'db_')")]
    databases: dict[int, list[int] | None] = {}
    for name in names:
        with _connect(server, name) as conn:
            try:
                databases[int(name[3:])] = [x for (x,) in conn.execute("SELECT x FROM t")]
            except psycopg.errors.UndefinedTable:
                databases[int(name[3:])] = None
    with _connect(server, "db2") as conn:
        ledger2 = Counter({(t, b): n for t, b, n in conn.execute(
            "SELECT txid, body, count(*) FROM ledger2 GROUP BY ALL")})
    return State(ledger, ledger2, search, index, witness, ddl, databases)


def _present(journal: Journal, state: State) -> dict[int, bool]:
    present: dict[int, bool] = {}
    for txn in journal.txns.values():
        if txn.sent is None:
            continue
        if txn.kind == "insert":
            present[txn.txid] = state.search[txn.txid] > 0
        elif txn.kind == "create":
            present[txn.txid] = txn.txid in state.ddl or any(
                w == (txn.txid, "drop") for w in state.witness.values())
        elif txn.kind == "dropdb":
            present[txn.txid] = (txn.target in state.witness
                                 and txn.target not in state.databases)
        elif txn.kind == "fill":
            if txn.target in state.databases:
                present[txn.txid] = txn.txid in (state.databases[txn.target] or [])
        else:
            present[txn.txid] = txn.txid in state.witness
    return present


def _check(seed: int, journal: Journal, state: State, cut_called: float,
           cut_returned: float, restored_writes: frozenset[int] = frozenset()) -> None:
    present = _present(journal, state)
    problems: list[str] = []

    for txid, is_present in present.items():
        txn = journal.txns[txid]
        if txid in restored_writes:
            if not is_present:
                problems.append(f"txn {txid} committed on the restored server is missing")
            continue
        if txn.acked is not None and txn.acked < cut_called and not is_present:
            problems.append(f"txn {txid} ({txn.kind}) acknowledged before the cut is missing")
        if txn.sent is not None and txn.sent > cut_returned and is_present:
            problems.append(f"txn {txid} ({txn.kind}) committed after the cut is present")
        if txn.target is not None and is_present and not present.get(txn.target, False):
            problems.append(f"txn {txid} ({txn.kind}) is present but its target "
                            f"{txn.target} is not")

    effect: dict[int, str] = {}
    for txid, (target, kind) in state.witness.items():
        if kind in ("update", "delete"):
            if target in effect:
                problems.append(f"insert {target} has two applied updates/deletes")
            effect[target] = kind

    for txn in journal.txns.values():
        if txn.kind != "insert" or txn.sent is None:
            continue
        t = txn.txid
        expected_ledger: Counter = Counter()
        if present[t]:
            if state.search[t] != txn.rows:
                problems.append(f"insert {t}: search table has {state.search[t]} "
                                f"of {txn.rows} rows")
            kind = effect.get(t)
            if kind != "delete":
                expected_ledger[(t, "upd" if kind == "update" else "orig")] = txn.rows
        for name, table, expected in (
                ("ledger", state.ledger, expected_ledger),
                ("index", state.index, expected_ledger),
                ("ledger2", state.ledger2, expected_ledger if txn.cross else Counter())):
            got = Counter({k: v for k, v in table.items() if k[0] == t})
            if got != expected:
                problems.append(f"insert {t}: {name} has {dict(got)}, "
                                f"expected {dict(expected)}")

    for txn in journal.txns.values():
        if txn.kind != "create" or txn.sent is None or not present[txn.txid]:
            continue
        dropped = any(w == (txn.txid, "drop") for w in state.witness.values())
        if dropped and txn.txid in state.ddl:
            problems.append(f"table ddl_{txn.txid} exists although its drop applied")
        if not dropped and state.ddl.get(txn.txid) != [txn.txid]:
            problems.append(f"table ddl_{txn.txid} holds {state.ddl.get(txn.txid)}")

    for txid, rows in state.databases.items():
        if txid not in state.witness:
            problems.append(f"database db_{txid} exists without its create")
        fills = [f.txid for f in journal.txns.values()
                 if f.kind == "fill" and f.target == txid and present.get(f.txid)]
        if rows is not None and rows != fills:
            problems.append(f"database db_{txid} holds {rows}, expected {fills}")
        if rows is None and fills:
            problems.append(f"database db_{txid} lost its table")

    known = set(journal.txns)
    for txid in (set(state.search) | {k[0] for k in state.ledger}
                 | {k[0] for k in state.ledger2} | set(state.ddl)
                 | set(state.databases)):
        if txid not in known:
            problems.append(f"rows of unknown txn {txid}")

    assert not problems, f"seed {seed}:\n" + "\n".join(problems[:50])


def test_backup_under_load_restores_exactly_the_cut(tmp_path: Path) -> None:
    seed = int(os.environ.get("SDB_DRV_BACKUP_SEED",
                              random.SystemRandom().randrange(1 << 32)))
    print(f"SDB_DRV_BACKUP_SEED={seed}")
    rng = random.Random(seed)
    source = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "source.log"))
    restored = None
    journal = Journal()
    stop = threading.Event()
    errors: list[str] = []
    threads: list[threading.Thread] = []
    try:
        source.start()
        _schema(source)
        threads = [threading.Thread(target=_worker,
                                    args=(source, journal, rng.random(), stop, errors))
                   for _ in range(WORKERS)]
        threads.append(threading.Thread(target=_maintenance, args=(source, stop)))
        for thread in threads:
            thread.start()
        time.sleep(WARMUP_SECONDS)

        copy = tmp_path / "copy"
        with _connect(source) as backup:
            cut_called = time.monotonic()
            backup.execute("SELECT pg_backup_start('driver')")
            cut_returned = time.monotonic()
            _copy_tree(Path(source.datadir), copy, rng)
            (label,) = backup.execute("SELECT labelfile FROM pg_backup_stop()").fetchone()
        time.sleep(TAIL_SECONDS)
        stop.set()
        for thread in threads:
            thread.join()
        assert not errors, f"seed {seed}:\n" + "\n".join(errors[:20])
        source.graceful_stop()

        (copy / "backup_label").write_text(label)
        restored = Serened(SERENED_BIN, datadir=str(copy),
                           log_path=str(tmp_path / "restored.log"))
        restored.start()
        _check(seed, journal, _read_state(restored), cut_called, cut_returned)

        after = journal.new("insert")
        after.rows = 5
        with _connect(restored) as conn:
            conn.execute("BEGIN")
            conn.execute(f"INSERT INTO ledger SELECT {after.txid}, 'orig' FROM range(5)")
            conn.execute(f"INSERT INTO ledger_s SELECT {after.txid * SEARCH_ID_STRIDE} + i, "
                         f"{after.txid} FROM range(5) r(i)")
            after.sent = time.monotonic()
            conn.execute("COMMIT")
            after.acked = time.monotonic()

        restored.restart()
        _check(seed, journal, _read_state(restored), cut_called, cut_returned,
               frozenset({after.txid}))
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=30)
        source.stop()
        if restored is not None:
            restored.stop()


def _retained_backup(tmp_path: Path) -> tuple[Path, int, int]:
    source = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "source.log"))
    try:
        source.start()
        with _connect(source) as conn:
            conn.execute("CREATE DATABASE st")
        with _connect(source, "st") as conn:
            conn.execute("CREATE TABLE docs (id BIGINT, body TEXT) WITH "
                         "(storage = 'search', refresh_interval = 0, compaction_interval = 0)")
            conn.execute("INSERT INTO docs SELECT i, 'first' FROM range(0, 1000) r(i)")
            conn.execute("VACUUM (REFRESH_TABLE) docs")
            conn.execute("CHECKPOINT st")
            conn.execute("DELETE FROM docs WHERE id < 100")
            conn.execute("INSERT INTO docs SELECT i, 'second' FROM range(1000, 1100) r(i)")
            conn.execute("CHECKPOINT st")
            conn.execute("INSERT INTO docs SELECT i, 'third' FROM range(2000, 2100) r(i)")
            conn.execute("CHECKPOINT st")
            conn.execute("INSERT INTO docs SELECT i, 'fourth' FROM range(3000, 3010) r(i)")
            (oid,) = conn.execute(
                "SELECT database_oid FROM duckdb_databases() "
                "WHERE database_name = 'st'").fetchone()

        copy = tmp_path / "copy"
        with _connect(source) as backup:
            backup.execute("SELECT pg_backup_start('retained')")
            with _connect(source, "st") as conn:
                conn.execute("INSERT INTO docs SELECT i, 'after' FROM range(4000, 4010) r(i)")
            shutil.copytree(source.datadir, copy, symlinks=True)
            (label,) = backup.execute("SELECT labelfile FROM pg_backup_stop()").fetchone()
    finally:
        source.stop()

    (copy / "backup_label").write_text(label)
    line = next(line for line in label.splitlines()
                if line.startswith(f"SERENEDB DATABASE: {oid} "))
    _, _, _, checkpoint, _, retained_from, _ = line.split()
    wal = copy / "engine_v1" / str(oid) / "data.db.wal"
    for generation in range(int(retained_from), int(checkpoint)):
        assert Path(f"{wal}.retained.{generation}").is_file(), line
    assert int(checkpoint) - int(retained_from) >= 2, line
    return copy, int(retained_from), int(checkpoint)


def _bodies(server: Serened) -> dict[str, int]:
    with _connect(server, "st") as conn:
        return dict(conn.execute(
            "SELECT body, count(*) FROM docs GROUP BY body ORDER BY body").fetchall())


def test_backup_restores_search_rows_held_only_in_retained_wals(tmp_path: Path) -> None:
    copy, _, _ = _retained_backup(tmp_path)
    restored = Serened(SERENED_BIN, datadir=str(copy),
                       log_path=str(tmp_path / "restored.log"))
    try:
        restored.start()
        expected = {"first": 900, "fourth": 10, "second": 100, "third": 100}
        assert _bodies(restored) == expected
        with _connect(restored, "st") as conn:
            conn.execute("INSERT INTO docs VALUES (5000, 'restored')")
        restored.restart()
        assert _bodies(restored) == expected | {"restored": 1}
    finally:
        restored.stop()


@pytest.mark.parametrize("missing", ["oldest", "newest"])
def test_backup_without_a_retained_wal_refuses_to_boot(tmp_path: Path, missing: str) -> None:
    copy, retained_from, checkpoint = _retained_backup(tmp_path)
    generation = retained_from if missing == "oldest" else checkpoint - 1
    removed = next(copy.glob(f"engine_v1/*/data.db.wal.retained.{generation}"))
    removed.unlink()
    restored = Serened(SERENED_BIN, datadir=str(copy),
                       log_path=str(tmp_path / "restored.log"))
    try:
        with pytest.raises(RuntimeError, match=f"the backup needs the retained WALs "
                                               f"{retained_from} to {checkpoint - 1}"):
            restored.start()
        assert (copy / "backup_label").is_file()
    finally:
        restored.stop()
