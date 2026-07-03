"""pg_stat_progress_* end-to-end over the full COPY matrix, plus CTAS and
CREATE INDEX.

Matrix: protocol (simple via psycopg2 / extended via psycopg3) x direction
(FROM / TO) x channel (file / stdin / stdout) x format (csv / text / binary /
json / parquet -- parquet is file-only). Every case asserts an IN-FLIGHT row
(command, type, relid) in the progress view while the statement runs, and
that the row is gone once it finishes.

Two pacing modes:
- failpoint (preferred, deterministic): the statement parks on its failpoint
  after the first reported chunk, the observer reads the view, then releases.
  Used when the server build has SDB_FAULT_INJECTION (SET sdb_faults works).
- poll (fallback for faultless builds): larger dataset, observer polls until
  the in-flight row appears.
"""

from __future__ import annotations

import contextlib
import io
import threading
import time
import uuid

import psycopg
import psycopg2
import pytest
from spec_loader import conn_kwargs

RUN = uuid.uuid4().hex[:10]
SRC = f"pgsp_src_{RUN}"
FILE_PREFIX = f"/tmp/pgsp_{RUN}"

FAULT_ROWS = 20_000
POLL_ROWS = 400_000

COPY_FROM_FAULT = "pause_sst_sink_mid_copy"
COPY_TO_FAULT = "pause_copy_to_mid_stream"
CTAS_FAULT = "pause_ctas_mid_ingest"
CREATE_INDEX_FAULT = "pause_create_index_mid_build"
ANALYZE_FAULT = "pause_recompute_stats_mid_walk"
VACUUM_FAULT = "pause_vacuum_mid_walk"


class Psycopg3Driver:
    """Extended query protocol."""

    name = "extended"

    def connect(self):
        return psycopg.connect(**conn_kwargs(), autocommit=True)

    def execute(self, sql: str) -> None:
        with self.connect() as c:
            c.execute(sql)

    def copy_in(self, sql: str, payload: bytes) -> None:
        with self.connect() as c, c.cursor() as cur:
            with cur.copy(sql) as cp:
                cp.write(payload)

    def copy_out(self, sql: str) -> bytes:
        with self.connect() as c, c.cursor() as cur:
            with cur.copy(sql) as cp:
                return b"".join(bytes(block) for block in cp)


class Psycopg2Driver:
    """Simple query protocol."""

    name = "simple"

    def connect(self):
        kw = conn_kwargs()
        c = psycopg2.connect(
            host=kw["host"], port=kw["port"], dbname=kw["dbname"],
            user=kw["user"])
        c.autocommit = True
        return c

    def execute(self, sql: str) -> None:
        c = self.connect()
        try:
            with c.cursor() as cur:
                cur.execute(sql)
        finally:
            c.close()

    def copy_in(self, sql: str, payload: bytes) -> None:
        c = self.connect()
        try:
            with c.cursor() as cur:
                cur.copy_expert(sql, io.BytesIO(payload))
        finally:
            c.close()

    def copy_out(self, sql: str) -> bytes:
        c = self.connect()
        try:
            sink = io.BytesIO()
            with c.cursor() as cur:
                cur.copy_expert(sql, sink)
            return sink.getvalue()
        finally:
            c.close()


DRIVERS = [Psycopg3Driver(), Psycopg2Driver()]


@pytest.fixture(scope="module")
def obs():
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    yield c
    c.close()


@pytest.fixture(scope="module")
def faults(obs) -> bool:
    try:
        obs.execute(f"SET sdb_faults = '{COPY_FROM_FAULT}'")
        obs.execute(f"SET sdb_faults = '-{COPY_FROM_FAULT}'")
        return True
    except psycopg.Error:
        return False


@pytest.fixture(scope="module")
def src(obs, faults) -> str:
    rows = FAULT_ROWS if faults else POLL_ROWS
    obs.execute(f"CREATE TABLE {SRC}(x BIGINT, label TEXT)")
    obs.execute(
        f"INSERT INTO {SRC} SELECT g, repeat(md5(g::text), 8) "
        f"FROM generate_series(1, {rows}) g")
    for fmt in ("csv", "text", "binary", "json", "parquet"):
        obs.execute(
            f"COPY {SRC} TO '{FILE_PREFIX}_src.{fmt}' (FORMAT {fmt})")
    yield SRC
    obs.execute(f"DROP TABLE IF EXISTS {SRC}")


@pytest.fixture(scope="module")
def payloads(obs, src) -> dict[str, bytes]:
    drv = Psycopg3Driver()
    return {
        "csv": drv.copy_out(f"COPY {src} TO STDOUT (FORMAT csv)"),
        "text": drv.copy_out(f"COPY {src} TO STDOUT (FORMAT text)"),
        "binary": drv.copy_out(f"COPY {src} TO STDOUT (FORMAT binary)"),
        "json": drv.copy_out(f"COPY {src} TO STDOUT (FORMAT json)"),
    }


@contextlib.contextmanager
def fault(obs, name: str, enabled: bool):
    if not enabled:
        yield
        return
    obs.execute(f"SET sdb_faults = '{name}'")
    try:
        yield
    finally:
        obs.execute(f"SET sdb_faults = '-{name}'")


def relid_of(obs, table: str) -> int:
    return obs.execute(
        f"SELECT '{table}'::regclass::oid").fetchone()[0]


def assert_inflight(obs, *, fault_name: str, use_fault: bool, run,
                    view_sql: str, expect: tuple, empty_sql: str,
                    timeout: float = 60.0):
    errors: list[BaseException] = []

    def target():
        try:
            run()
        except BaseException as e:  # noqa: BLE001 - reported to pytest below
            errors.append(e)

    with fault(obs, fault_name, use_fault):
        thread = threading.Thread(target=target)
        thread.start()
        row = None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            got = obs.execute(view_sql).fetchall()
            if got:
                row = got[0]
                break
            if not thread.is_alive() and not use_fault:
                break
            time.sleep(0.02)
    thread.join(timeout=timeout)
    assert not thread.is_alive(), "statement did not finish after release"
    assert not errors, f"statement failed: {errors[0]}"
    assert row == expect, f"in-flight row mismatch: {row} != {expect}"
    assert obs.execute(empty_sql).fetchone()[0] == 0, \
        "progress row survived statement end"


FROM_CASES = [
    ("file", "csv"), ("file", "text"), ("file", "binary"),
    ("file", "json"), ("file", "parquet"),
    ("stdin", "csv"), ("stdin", "text"), ("stdin", "binary"),
    ("stdin", "json"),
]

TO_CASES = [
    ("file", "csv"), ("file", "text"), ("file", "binary"),
    ("file", "json"), ("file", "parquet"),
    ("stdout", "csv"), ("stdout", "text"), ("stdout", "binary"),
    ("stdout", "json"),
]


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
@pytest.mark.parametrize("channel,fmt", FROM_CASES,
                         ids=[f"{c}-{f}" for c, f in FROM_CASES])
def test_copy_from_progress(obs, faults, src, payloads, driver, channel, fmt):
    dst = f"pgsp_dst_{uuid.uuid4().hex[:10]}"
    obs.execute(f"CREATE TABLE {dst}(x BIGINT, label TEXT)")
    try:
        if channel == "file":
            run = lambda: driver.execute(
                f"COPY {dst} FROM '{FILE_PREFIX}_src.{fmt}' (FORMAT {fmt})")
            expected_type = "FILE"
        else:
            payload = payloads[fmt]
            run = lambda: driver.copy_in(
                f"COPY {dst} FROM STDIN (FORMAT {fmt})", payload)
            expected_type = "PIPE"
        relid = relid_of(obs, dst)
        assert_inflight(
            obs, fault_name=COPY_FROM_FAULT, use_fault=faults, run=run,
            view_sql=(
                f"SELECT command, \"type\" FROM pg_stat_progress_copy "
                f"WHERE relid = {relid} AND tuples_processed > 0"),
            expect=("COPY FROM", expected_type),
            empty_sql=(
                f"SELECT count(*) FROM pg_stat_progress_copy "
                f"WHERE relid = {relid}"))
        n = obs.execute(f"SELECT count(*) FROM {dst}").fetchone()[0]
        assert n == obs.execute(
            f"SELECT count(*) FROM {src}").fetchone()[0]
    finally:
        obs.execute(f"DROP TABLE IF EXISTS {dst}")


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
@pytest.mark.parametrize("channel,fmt", TO_CASES,
                         ids=[f"{c}-{f}" for c, f in TO_CASES])
def test_copy_to_progress(obs, faults, src, driver, channel, fmt):
    if channel == "file":
        out = f"{FILE_PREFIX}_out_{uuid.uuid4().hex[:8]}.{fmt}"
        run = lambda: driver.execute(
            f"COPY {src} TO '{out}' (FORMAT {fmt})")
        expected_type = "FILE"
    else:
        run = lambda: driver.copy_out(
            f"COPY {src} TO STDOUT (FORMAT {fmt})")
        expected_type = "PIPE"
    relid = relid_of(obs, src)
    assert_inflight(
        obs, fault_name=COPY_TO_FAULT, use_fault=faults, run=run,
        view_sql=(
            f"SELECT command, \"type\" FROM pg_stat_progress_copy "
            f"WHERE relid = {relid} AND tuples_processed > 0"),
        expect=("COPY TO", expected_type),
        empty_sql=(
            f"SELECT count(*) FROM pg_stat_progress_copy "
            f"WHERE relid = {relid}"))


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
def test_copy_to_query_form_relid_zero(obs, faults, src, driver):
    run = lambda: driver.copy_out(
        f"COPY (SELECT * FROM {src}) TO STDOUT (FORMAT csv)")
    assert_inflight(
        obs, fault_name=COPY_TO_FAULT, use_fault=faults, run=run,
        view_sql=(
            "SELECT command, relid FROM pg_stat_progress_copy "
            "WHERE tuples_processed > 0"),
        expect=("COPY TO", 0),
        empty_sql="SELECT count(*) FROM pg_stat_progress_copy")


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
def test_create_table_as_progress(obs, faults, src, driver):
    dst = f"pgsp_ctas_{uuid.uuid4().hex[:10]}"
    try:
        run = lambda: driver.execute(
            f"CREATE TABLE {dst} AS SELECT * FROM {src}")
        assert_inflight(
            obs, fault_name=CTAS_FAULT, use_fault=faults, run=run,
            view_sql=(
                "SELECT command, phase FROM pg_stat_progress_create_table_as "
                "WHERE tuples_processed > 0"),
            expect=("CREATE TABLE AS", "ingesting"),
            empty_sql=(
                "SELECT count(*) FROM pg_stat_progress_create_table_as"))
        n = obs.execute(f"SELECT count(*) FROM {dst}").fetchone()[0]
        assert n == obs.execute(
            f"SELECT count(*) FROM {src}").fetchone()[0]
    finally:
        obs.execute(f"DROP TABLE IF EXISTS {dst}")


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
def test_create_index_progress(obs, faults, src, driver):
    idx = f"pgsp_idx_{uuid.uuid4().hex[:10]}"
    relid = relid_of(obs, src)
    try:
        run = lambda: driver.execute(
            f"CREATE INDEX {idx} ON {src} USING inverted(label)")
        assert_inflight(
            obs, fault_name=CREATE_INDEX_FAULT, use_fault=faults, run=run,
            view_sql=(
                f"SELECT command, phase FROM pg_stat_progress_create_index "
                f"WHERE relid = {relid} AND tuples_done > 0"),
            expect=("CREATE INDEX", "building index"),
            empty_sql=(
                f"SELECT count(*) FROM pg_stat_progress_create_index "
                f"WHERE relid = {relid}"))
    finally:
        obs.execute(f"DROP INDEX IF EXISTS {idx}")


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
def test_plain_dml_never_in_copy_view(obs, faults, src, driver):
    dst = f"pgsp_ins_{uuid.uuid4().hex[:10]}"
    obs.execute(f"CREATE TABLE {dst}(x BIGINT, label TEXT)")
    try:
        seen: list[tuple] = []
        done = threading.Event()

        def watch():
            while not done.is_set():
                seen.extend(
                    obs.execute(
                        "SELECT * FROM pg_stat_progress_copy").fetchall())
                time.sleep(0.02)

        watcher = threading.Thread(target=watch)
        watcher.start()
        try:
            driver.execute(f"INSERT INTO {dst} SELECT * FROM {src}")
        finally:
            done.set()
            watcher.join()
        assert not seen, f"plain INSERT leaked into pg_stat_progress_copy: {seen}"
    finally:
        obs.execute(f"DROP TABLE IF EXISTS {dst}")


VA_SCHEMA = f"pgsp_va_{RUN}"
VA_TABLES = 3


@pytest.fixture(scope="module")
def va_schema(obs, faults) -> str:
    rows = FAULT_ROWS if faults else POLL_ROWS
    obs.execute(f"CREATE SCHEMA {VA_SCHEMA}")
    for i in range(VA_TABLES):
        obs.execute(f"CREATE TABLE {VA_SCHEMA}.t{i}(id BIGINT, body TEXT)")
        obs.execute(
            f"INSERT INTO {VA_SCHEMA}.t{i} "
            f"SELECT g, repeat(md5(g::text), 6) "
            f"FROM generate_series(1, {rows}) g")
        obs.execute(
            f"CREATE INDEX va_idx{i} ON {VA_SCHEMA}.t{i} "
            f"USING inverted(body)")
    yield VA_SCHEMA
    obs.execute(f"DROP SCHEMA {VA_SCHEMA} CASCADE")


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
def test_recompute_stats_progress(obs, faults, va_schema, driver):
    table = f"{va_schema}.t0"
    relid = relid_of(obs, table)
    obs.execute(
        f"INSERT INTO {table} SELECT g, repeat(md5(g::text), 6) "
        f"FROM generate_series(1, 10000) g")
    run = lambda: driver.execute(f"VACUUM (RECOMPUTE_STATS_TABLE) {table}")
    assert_inflight(
        obs, fault_name=ANALYZE_FAULT, use_fault=faults, run=run,
        view_sql=(
            f"SELECT phase, relid = {relid}, child_tables_total "
            f"FROM pg_stat_progress_analyze"),
        expect=("computing statistics", True, 1),
        empty_sql="SELECT count(*) FROM pg_stat_progress_analyze")


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
def test_vacuum_progress(obs, faults, va_schema, driver):
    for i in range(VA_TABLES):
        obs.execute(
            f"INSERT INTO {va_schema}.t{i} "
            f"SELECT g, repeat(md5(g::text), 6) "
            f"FROM generate_series(1, 10000) g")
    run = lambda: driver.execute(f"VACUUM (COMPACT_SCHEMA) {va_schema}")
    assert_inflight(
        obs, fault_name=VACUUM_FAULT, use_fault=faults, run=run,
        view_sql=(
            f"SELECT phase, indexes_total FROM pg_stat_progress_vacuum "
            f"WHERE indexes_processed <= indexes_total"),
        expect=("vacuuming indexes", VA_TABLES),
        empty_sql="SELECT count(*) FROM pg_stat_progress_vacuum")


def sqlstate(exc) -> str | None:
    return getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)


def assert_cancelled(obs, *, fault_name: str, use_fault: bool, run,
                     pid_sql: str, empty_sql: str, timeout: float = 60.0):
    errors: list[BaseException] = []

    def target():
        try:
            run()
        except BaseException as e:  # noqa: BLE001 - reported to pytest below
            errors.append(e)

    with fault(obs, fault_name, use_fault):
        thread = threading.Thread(target=target)
        thread.start()
        pid = None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            got = obs.execute(pid_sql).fetchall()
            if got:
                pid = got[0][0]
                break
            if not thread.is_alive():
                break
            time.sleep(0.02)
        assert pid is not None, "operation finished before it became visible"
        assert obs.execute(
            f"SELECT pg_cancel_backend({pid})").fetchone()[0] is True
    thread.join(timeout=timeout)
    assert not thread.is_alive(), "statement did not abort after cancel"
    assert errors, "statement completed instead of aborting"
    state = sqlstate(errors[0])
    assert state == "57014", f"unexpected error {state}: {errors[0]}"
    deadline = time.monotonic() + 10
    while obs.execute(empty_sql).fetchone()[0] != 0:
        assert time.monotonic() < deadline, "progress row survived cancel"
        time.sleep(0.02)


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
def test_cancel_copy_from(obs, faults, src, driver):
    dst = f"pgsp_c_dst_{uuid.uuid4().hex[:10]}"
    obs.execute(f"CREATE TABLE {dst}(x BIGINT, label TEXT)")
    try:
        relid = relid_of(obs, dst)
        assert_cancelled(
            obs, fault_name=COPY_FROM_FAULT, use_fault=faults,
            run=lambda: driver.execute(
                f"COPY {dst} FROM '{FILE_PREFIX}_src.csv' (FORMAT csv)"),
            pid_sql=(
                f"SELECT pid FROM pg_stat_progress_copy "
                f"WHERE relid = {relid} AND tuples_processed > 0"),
            empty_sql=(
                f"SELECT count(*) FROM pg_stat_progress_copy "
                f"WHERE relid = {relid}"))
        assert obs.execute(
            f"SELECT count(*) FROM {dst}").fetchone()[0] == 0
    finally:
        obs.execute(f"DROP TABLE IF EXISTS {dst}")


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
def test_cancel_copy_to(obs, faults, src, driver):
    relid = relid_of(obs, src)
    out = f"{FILE_PREFIX}_cancel_{uuid.uuid4().hex[:8]}.csv"
    assert_cancelled(
        obs, fault_name=COPY_TO_FAULT, use_fault=faults,
        run=lambda: driver.execute(f"COPY {src} TO '{out}' (FORMAT csv)"),
        pid_sql=(
            f"SELECT pid FROM pg_stat_progress_copy "
            f"WHERE relid = {relid} AND tuples_processed > 0"),
        empty_sql=(
            f"SELECT count(*) FROM pg_stat_progress_copy "
            f"WHERE relid = {relid}"))


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
def test_cancel_ctas(obs, faults, src, driver):
    dst = f"pgsp_c_ctas_{uuid.uuid4().hex[:10]}"
    assert_cancelled(
        obs, fault_name=CTAS_FAULT, use_fault=faults,
        run=lambda: driver.execute(
            f"CREATE TABLE {dst} AS SELECT * FROM {src}"),
        pid_sql=(
            "SELECT pid FROM pg_stat_progress_create_table_as "
            "WHERE tuples_processed > 0"),
        empty_sql="SELECT count(*) FROM pg_stat_progress_create_table_as")
    assert obs.execute(
        f"SELECT count(*) FROM pg_tables WHERE tablename = '{dst}'"
    ).fetchone()[0] == 0


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
def test_cancel_create_index(obs, faults, src, driver):
    idx = f"pgsp_c_idx_{uuid.uuid4().hex[:10]}"
    relid = relid_of(obs, src)
    try:
        assert_cancelled(
            obs, fault_name=CREATE_INDEX_FAULT, use_fault=faults,
            run=lambda: driver.execute(
                f"CREATE INDEX {idx} ON {src} USING inverted(label)"),
            pid_sql=(
                f"SELECT pid FROM pg_stat_progress_create_index "
                f"WHERE relid = {relid} AND tuples_done > 0"),
            empty_sql=(
                f"SELECT count(*) FROM pg_stat_progress_create_index "
                f"WHERE relid = {relid}"))
        assert obs.execute(
            f"SELECT count(*) FROM pg_indexes WHERE indexname = '{idx}'"
        ).fetchone()[0] == 0
    finally:
        obs.execute(f"DROP INDEX IF EXISTS {idx}")


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
def test_cancel_recompute_stats(obs, faults, va_schema, driver):
    for i in range(VA_TABLES):
        obs.execute(
            f"INSERT INTO {va_schema}.t{i} "
            f"SELECT g, repeat(md5(g::text), 6) "
            f"FROM generate_series(1, 10000) g")
    assert_cancelled(
        obs, fault_name=ANALYZE_FAULT, use_fault=faults,
        run=lambda: driver.execute(
            f"VACUUM (RECOMPUTE_STATS_SCHEMA) {va_schema}"),
        pid_sql="SELECT pid FROM pg_stat_progress_analyze",
        empty_sql="SELECT count(*) FROM pg_stat_progress_analyze")
    driver.execute(f"VACUUM (RECOMPUTE_STATS_TABLE) {va_schema}.t0")


@pytest.mark.parametrize("driver", DRIVERS, ids=lambda d: d.name)
def test_cancel_compact(obs, faults, va_schema, driver):
    for i in range(VA_TABLES):
        obs.execute(
            f"INSERT INTO {va_schema}.t{i} "
            f"SELECT g, repeat(md5(g::text), 6) "
            f"FROM generate_series(1, 10000) g")
    assert_cancelled(
        obs, fault_name=VACUUM_FAULT, use_fault=faults,
        run=lambda: driver.execute(f"VACUUM (COMPACT_SCHEMA) {va_schema}"),
        pid_sql="SELECT pid FROM pg_stat_progress_vacuum",
        empty_sql="SELECT count(*) FROM pg_stat_progress_vacuum")
    driver.execute(f"VACUUM (REFRESH_SCHEMA) {va_schema}")
