"""psycopg3 driver tests against SereneDB.

For each (type, protocol, sample) tuple from types.yaml we round-trip the
sample value: encode in psycopg3, send it to SereneDB, read it back, and
assert the *normalised* representation matches the input.

Normalisation: we compare values as their text-form ("send a value as
text, read it back as text"). For binary-format params we let psycopg3
encode binary on the wire but still receive the result column as text
via `cast_sql`, so that comparison is independent of psycopg3's per-type
Python decoder. This keeps the matrix focused on PG-wire correctness,
not on whether psycopg3 happened to materialise a `datetime.date`.
"""

from __future__ import annotations

import os
import re
import struct
import threading
import time

import psycopg
import pytest
from psycopg.adapt import Loader
from psycopg.pq import Format, TransactionStatus
from spec_loader import (
    PROTOCOL_MODES,
    TypeSpec,
    cases_for_driver,
    conn_kwargs,
    schema_name,
)

DRIVER_KEY = "python_psycopg3"


# ---- session-scoped connection + schema ------------------------------------

@pytest.fixture(scope="session")
def conn() -> psycopg.Connection:
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    schema = schema_name(DRIVER_KEY)
    with c.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cur.execute(f'SET search_path TO "{schema}", public, pg_catalog')
    yield c
    with c.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    c.close()


# ---- smoke test -------------------------------------------------------------

def test_smoke_select_one(conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)


def test_smoke_version(conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        row = cur.fetchone()
        assert row is not None
        assert isinstance(row[0], str)


# ---- parameter generation ---------------------------------------------------

def _id(case: tuple[TypeSpec, str, int, object]) -> str:
    ts, proto, idx, _ = case
    return f"{ts.oid}-{ts.name}-{proto}-{idx}"


CASES = list(cases_for_driver(DRIVER_KEY))


# ---- round-trip core -------------------------------------------------------

def _normalise(s: object) -> str | None:
    """Text-form representation we compare against. None for SQL NULL."""
    if s is None:
        return None
    if isinstance(s, bool):
        return "t" if s else "f"
    return str(s)


def _server_text(conn: psycopg.Connection, ts: TypeSpec, sample: str | None) -> str | None:
    """What the server normalises {sample}::pg_typname to as text.

    Inline literal cast -- avoids psycopg's per-type bind-time coercion
    (which corrupts e.g. bytea PG-escape samples). The server's own
    opinion of the cast result is the reference for round-trip equality.
    """
    if sample is None:
        return None
    lit = sample.replace("'", "''")
    with conn.cursor() as cur:
        cur.execute(f"SELECT ('{lit}'::{ts.pg_typname})::text")
        row = cur.fetchone()
        return row[0] if row else None


@pytest.mark.parametrize("case", CASES, ids=_id)
def test_roundtrip(conn: psycopg.Connection, case: tuple[TypeSpec, str, int, object]):
    ts, proto, _idx, sample = case
    expected = _server_text(conn, ts, sample)

    if proto == "simple":
        # Simple Query: no parameters, embed as literal cast. NULL is sent
        # via the SQL keyword.
        if sample is None:
            sql = f"SELECT NULL::{ts.pg_typname}::text AS v"
            params = None
        else:
            # Inline as quoted string; rely on the server's input parser.
            # Single-quote escape: PG doubles single quotes inside string
            # literals when standard_conforming_strings is on.
            lit = sample.replace("'", "''")
            sql = f"SELECT '{lit}'::{ts.pg_typname}::text AS v"
            params = None
        # In simple-query mode psycopg3 picks the protocol when no params
        # are passed; we still want to force the simple path. Easiest is
        # to use a text-only path.
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        assert row is not None
        actual = row[0]
        assert actual == expected, f"simple mode: expected {expected!r}, got {actual!r}"
        return

    # Extended modes: psycopg3 always uses extended protocol.
    # extended-noparam: no $1, just an executed Parse/Bind/Execute.
    # extended-text:    bind sample as text.
    # extended-binary:  bind sample as binary; force binary=True.
    if proto == "extended-noparam":
        if sample is None:
            sql = f"SELECT NULL::{ts.pg_typname}::text AS v"
        else:
            lit = sample.replace("'", "''")
            sql = f"SELECT '{lit}'::{ts.pg_typname}::text AS v"
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        assert row is not None
        assert row[0] == expected
        return

    binary = proto == "extended-binary"
    # psycopg3 uses %s placeholders client-side and substitutes them with $1
    # on the wire. The cast_sql in types.yaml uses $1 (the PG-native form),
    # which is what every other-language harness sees; for psycopg3 we map
    # back to its placeholder.
    sql = f"SELECT ({ts.cast_sql.replace('$1', '%s')})::text AS v"
    # bytea binding: psycopg3 maps str -> bytes by encoding, which corrupts
    # PG-escape strings like "\xdeadbeef". Bind as bytes from the sample's
    # PG-escape form so the server's bytea-in path sees the original octets.
    bind = sample
    if ts.pg_typname == "bytea" and isinstance(sample, str):
        if sample.startswith("\\x"):
            bind = bytes.fromhex(sample[2:])
    with conn.cursor(binary=binary) as cur:
        # psycopg3 lets us specify the param's type via Cast; but the
        # cast_sql already provides server-side cast, so plain string
        # binding is sufficient. For binary mode psycopg3 must know how
        # to encode; it does for all primitive types via its built-in
        # adapters.
        cur.execute(sql, (bind,))
        row = cur.fetchone()
    assert row is not None
    actual = row[0]
    assert actual == expected, (
        f"{proto}: expected {expected!r}, got {actual!r} for sample {sample!r}"
    )


# ---- connection-property assertions ----------------------------------------

def test_parameter_status_server_version(conn: psycopg.Connection):
    """server_version must be reported and non-empty (psycopg3 reads it)."""
    pgconn = conn.pgconn
    val = pgconn.parameter_status(b"server_version")
    assert val is not None
    assert val != b""


def test_standard_conforming_strings(conn: psycopg.Connection):
    pgconn = conn.pgconn
    val = pgconn.parameter_status(b"standard_conforming_strings")
    # PG reports "on"/"off"; SereneDB currently reports "true"/"false".
    # Real drivers (pgjdbc, npgsql) tolerate both, but the spec is "on"/"off".
    # Tracked as a SereneDB compat divergence; once fixed, drop the alt.
    assert val in (b"on", b"off", b"true", b"false")


# ---- ReadyForQuery transaction-status wiring -------------------------------

def test_ready_for_query_transaction_status():
    """ReadyForQuery must carry the live txn status (I/T/E), not a constant."""
    table = f"rfq_status_{os.getpid()}"
    c = psycopg.connect(**conn_kwargs())  # default autocommit=False
    try:
        # Nothing executed yet: idle, outside any transaction block -> 'I'.
        assert c.pgconn.transaction_status == TransactionStatus.IDLE

        # Seed a table + row in autocommit so the failing INSERT below hits a
        # live PK constraint (not an in-transaction-DDL "does not exist").
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'CREATE TABLE "{table}" (id int primary key)')
            cur.execute(f'INSERT INTO "{table}" VALUES (1)')
        c.autocommit = False

        # The first statement opens an implicit transaction block -> 'T'.
        with c.cursor() as cur:
            cur.execute("SELECT 1")
        assert c.pgconn.transaction_status == TransactionStatus.INTRANS

        # A primary-key violation invalidates the transaction -> failed 'E'.
        with pytest.raises(psycopg.Error):
            with c.cursor() as cur:
                cur.execute(f'INSERT INTO "{table}" VALUES (1)')
        assert c.pgconn.transaction_status == TransactionStatus.INERROR

        # ROLLBACK ends the block -> back to idle.
        c.rollback()
        assert c.pgconn.transaction_status == TransactionStatus.IDLE
    finally:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        c.close()


def test_managed_transaction_commit_persists():
    """commit() under autocommit=False must actually send COMMIT and persist.

    With the bug the status byte stayed 'I', so psycopg treated commit() as a
    no-op; the INSERT was never committed and a second session saw nothing.
    """
    table = f"rfq_commit_{os.getpid()}"
    writer = psycopg.connect(**conn_kwargs())  # default autocommit=False
    try:
        writer.autocommit = True
        with writer.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'CREATE TABLE "{table}" (id int primary key, v text)')
        writer.autocommit = False

        with writer.cursor() as cur:
            cur.execute(f'INSERT INTO "{table}" VALUES (1, %s)', ("hello",))
        writer.commit()

        # A separate session only sees the row if COMMIT really went out.
        reader = psycopg.connect(**conn_kwargs())
        try:
            with reader.cursor() as cur:
                cur.execute(f'SELECT v FROM "{table}" WHERE id = 1')
                assert cur.fetchone() == ("hello",)
        finally:
            reader.close()
    finally:
        writer.autocommit = True
        with writer.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        writer.close()


# ---- command tags (CommandComplete) ----------------------------------------


def test_returning_command_tags():
    """INSERT/UPDATE/DELETE ... RETURNING must report PG's affected-row command
    tag ("INSERT 0 N" / "UPDATE N" / "DELETE N"), not the bare verb. RETURNING
    makes DuckDB return a result set, so the count used to be dropped. Plain DML
    and SELECT keep their tags too.
    """
    table = f"rfq_tags_{os.getpid()}"
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        with c.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'CREATE TABLE "{table}" (id int, v int)')

            cur.execute(f'INSERT INTO "{table}" VALUES (1,10),(2,20),(3,30)')
            assert cur.statusmessage == "INSERT 0 3"

            cur.execute(f'INSERT INTO "{table}" VALUES (4,40),(5,50) RETURNING id')
            assert cur.statusmessage == "INSERT 0 2"

            cur.execute(f'UPDATE "{table}" SET v = v + 1 WHERE id <= 2 RETURNING id')
            assert cur.statusmessage == "UPDATE 2"

            cur.execute(f'DELETE FROM "{table}" WHERE id = 5 RETURNING id')
            assert cur.statusmessage == "DELETE 1"

            # RETURNING that matches no rows still reports the (zero) count.
            cur.execute(f'UPDATE "{table}" SET v = 0 WHERE id = 999 RETURNING id')
            assert cur.statusmessage == "UPDATE 0"

            cur.execute(f'SELECT * FROM "{table}"')
            assert cur.statusmessage == "SELECT 4"

            # MERGE ... RETURNING reports PG's "MERGE N" (total rows
            # inserted/updated/deleted; no leading 0 like INSERT). One match
            # (update) + one non-match (insert) = 2.
            cur.execute(f'CREATE TABLE "{table}_src" (id int, v int)')
            cur.execute(f'INSERT INTO "{table}_src" VALUES (1, 100), (9, 900)')
            cur.execute(
                f'MERGE INTO "{table}" t USING "{table}_src" s ON t.id = s.id '
                "WHEN MATCHED THEN UPDATE SET v = s.v "
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v) RETURNING t.id"
            )
            assert cur.statusmessage == "MERGE 2"
    finally:
        with c.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'DROP TABLE IF EXISTS "{table}_src"')
        c.close()


def test_ddl_and_utility_command_tags():
    """Non-row-count tags match PG: CREATE/DROP carry no count, TRUNCATE is
    "TRUNCATE TABLE" (no count -- it used to append a bogus one), and RESET
    reports "RESET", not "SET".
    """
    table = f"rfq_ddl_{os.getpid()}"
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        with c.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'CREATE TABLE "{table}" (i int)')
            assert cur.statusmessage == "CREATE TABLE"
            cur.execute(f'INSERT INTO "{table}" VALUES (1), (2), (3)')
            cur.execute(f'TRUNCATE "{table}"')
            assert cur.statusmessage == "TRUNCATE TABLE"
            cur.execute("SET search_path = public")
            assert cur.statusmessage == "SET"
            cur.execute("RESET search_path")
            assert cur.statusmessage == "RESET"
            cur.execute(f'CREATE INDEX "{table}_i" ON "{table}" (i)')
            assert cur.statusmessage == "CREATE INDEX"
            cur.execute(f'DROP TABLE "{table}"')
            assert cur.statusmessage == "DROP TABLE"
    finally:
        with c.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        c.close()


# ---- error SQLSTATE classes ------------------------------------------------


def test_invalid_input_sqlstate_not_internal():
    """A bad SET value (uncastable) is a user error -- invalid_parameter_value
    (22023) -- not internal_error (XX000). DuckDB raises InvalidInputException
    for it, which used to fall through to XX000.
    """
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        with c.cursor() as cur:
            with pytest.raises(psycopg.Error) as ei:
                # The cast fails before the pool is resized, so this has no
                # lasting effect on the (session-shared) server.
                cur.execute("SET threads = 'not-a-number'")
            assert ei.value.sqlstate == "22023", ei.value.sqlstate
    finally:
        c.close()


def test_unknown_guc_sqlstate():
    """An unknown configuration parameter is undefined_object (42704), like
    PG -- not undefined_table (42P01). Applies to SET / RESET /
    current_setting, which all route through the same catalog lookup.
    """
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        with c.cursor() as cur:
            for sql in (
                "SET bogus_param_xyz = 1",
                "RESET bogus_param_xyz",
                "SELECT current_setting('bogus_param_xyz')",
            ):
                with pytest.raises(psycopg.Error) as ei:
                    cur.execute(sql)
                assert ei.value.sqlstate == "42704", f"{sql}: {ei.value.sqlstate}"
    finally:
        c.close()


def test_binder_error_sqlstates():
    """BINDER errors get distinct SQLSTATEs, not a blanket undefined_column:
    ambiguous column -> 42702, grouping -> 42803, genuinely-missing -> 42703.
    """
    table = f"rfq_bind_{os.getpid()}"
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        with c.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'CREATE TABLE "{table}" (i int, j int)')
            cases = [
                (f'SELECT nope FROM "{table}"', "42703"),
                (f'SELECT i FROM "{table}" x, "{table}" y', "42702"),
                (f'SELECT i, sum(j) FROM "{table}"', "42803"),
            ]
            for sql, code in cases:
                with pytest.raises(psycopg.Error) as ei:
                    cur.execute(sql)
                assert ei.value.sqlstate == code, f"{sql}: {ei.value.sqlstate}"
    finally:
        with c.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        c.close()


def test_array_csv_roundtrip():
    # Arrays survive a CSV copy round-trip: csv-out emits no spurious header
    # (PG default), and the input cast accepts both PG {..} and DuckDB [..].
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    src = f"acsv_src_{os.getpid()}"
    dst = f"acsv_dst_{os.getpid()}"
    try:
        with c.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
            cur.execute(f'DROP TABLE IF EXISTS "{dst}"')
            cur.execute(f'CREATE TABLE "{src}" (a int[], b text[])')
            cur.execute(f'CREATE TABLE "{dst}" (a int[], b text[])')
            cur.execute(
                f"""INSERT INTO "{src}" VALUES """
                "(ARRAY[10,20,30], ARRAY['x','y z','d,e'])")
            buf = b""
            with cur.copy(f'COPY "{src}" TO STDOUT (FORMAT csv)') as cp:
                for chunk in cp:
                    buf += bytes(chunk)
            assert not buf.startswith(b"a,b"), buf  # no spurious header line
            with cur.copy(f'COPY "{dst}" FROM STDIN (FORMAT csv)') as cp:
                cp.write(buf)
            cur.execute(f'SELECT a, b FROM "{dst}"')
            assert cur.fetchall() == [([10, 20, 30], ["x", "y z", "d,e"])]
            # The PG {..} text form also parses on input.
            assert cur.execute("SELECT '{1,2,3}'::int[]").fetchone()[0] == [1, 2, 3]
            assert cur.execute("SELECT '[1,2,3]'::int[]").fetchone()[0] == [1, 2, 3]
    finally:
        with c.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
            cur.execute(f'DROP TABLE IF EXISTS "{dst}"')
        c.close()


class _RawBinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data):
        return bytes(data)


def _array_header(buf):
    ndim, flags, _oid = struct.unpack_from(">iii", buf, 0)
    dims = [tuple(struct.unpack_from(">ii", buf, 12 + i * 8)) for i in range(ndim)]
    return ndim, flags, dims


def test_binary_array_header_matches_pg():
    """Binary array output matches PG byte-for-byte: lower bound is 1 (not 0),
    an empty array is ndim=0 with no dimension descriptors, and the flags word
    is set iff the array actually contains a NULL. Cross-checked against
    PostgreSQL 16.
    """
    cases = [
        ("SELECT ARRAY[10,20,30]::int4[]", 1, 0, [(3, 1)]),
        ("SELECT '{}'::int4[]", 0, 0, []),
        ("SELECT ARRAY[1,NULL,3]::int4[]", 1, 1, [(3, 1)]),
        ("SELECT ARRAY['a','bb','ccc']::text[]", 1, 0, [(3, 1)]),
        ("SELECT ARRAY[ARRAY[1,2,3],ARRAY[4,5,6]]", 2, 0, [(2, 1), (3, 1)]),
        ("SELECT ARRAY[ARRAY[1,NULL],ARRAY[3,4]]", 2, 1, [(2, 1), (2, 1)]),
        ("SELECT ARRAY[true,false,NULL]::bool[]", 1, 1, [(3, 1)]),
    ]
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        for oid in (1007, 1009, 1000):  # int4[], text[], bool[]
            c.adapters.register_loader(oid, _RawBinaryLoader)
        with c.cursor(binary=True) as cur:
            for sql, ndim, flags, dims in cases:
                cur.execute(sql)
                raw = cur.fetchone()[0]
                assert _array_header(raw) == (ndim, flags, dims), (sql, raw.hex())
    finally:
        c.close()


# ---- pg_cancel_backend() / pg_terminate_backend() --------------------------


def test_pg_cancel_terminate_unknown_pid():
    # Both functions exist and return false for a pid that matches no backend,
    # and warn "PID N is not a PostgreSQL backend process" (matching PG).
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    notices: list[str] = []
    c.add_notice_handler(lambda d: notices.append(d.message_primary))
    try:
        with c.cursor() as cur:
            assert cur.execute("select pg_cancel_backend(2147483647)").fetchone()[0] is False
            assert cur.execute("select pg_terminate_backend(2147483647)").fetchone()[0] is False
        assert len(notices) == 2, notices
        assert all("is not a PostgreSQL backend process" in n for n in notices), notices
    finally:
        c.close()


def test_pg_terminate_backend_drops_connection():
    # pg_terminate_backend() really terminates: the victim's connection is
    # closed by the server, not merely its query cancelled. The close is
    # asynchronous, so poll the victim until it observes the drop.
    victim = psycopg.connect(**conn_kwargs(), autocommit=True)
    killer = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        with victim.cursor() as cur:
            pid = cur.execute("select pg_backend_pid()").fetchone()[0]
        with killer.cursor() as cur:
            assert cur.execute(f"select pg_terminate_backend({pid})").fetchone()[0] is True
        deadline = time.time() + 10
        while True:
            try:
                victim.execute("select 1")
            except psycopg.Error:
                break
            assert time.time() < deadline, "victim connection survived pg_terminate_backend"
            time.sleep(0.05)
    finally:
        killer.close()
        try:
            victim.close()
        except psycopg.Error:
            pass


def test_pg_backend_pid_is_positive():
    # pg_backend_pid() is the high half of the random cancel key; PG pids are
    # positive int32, so it must land in [1, INT32_MAX] -- never zero/negative.
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        with c.cursor() as cur:
            pid = cur.execute("select pg_backend_pid()").fetchone()[0]
            assert isinstance(pid, int) and 1 <= pid <= 2147483647, pid
    finally:
        c.close()


def test_set_boolean_guc_on_off():
    # PG accepts on/off (bare and quoted) as boolean SET values. serenedb matches
    # via a DuckDB bool-cast patch (off / 'on' / 'off') plus a SET-value grammar
    # rule for the bare ON/OFF keywords (ON is reserved).
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        with c.cursor() as cur:
            for v in ("on", "off", "ON", "OFF", "'on'", "'off'", "true", "1"):
                cur.execute(f"SET standard_conforming_strings = {v}")
                assert cur.statusmessage == "SET", v
    finally:
        c.close()


def test_datestyle_intervalstyle_validation():
    # Invalid DateStyle/IntervalStyle are rejected with 22023 (like PG), not
    # silently stored; valid values are accepted. (TimeZone validation is a
    # separate followup.)
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        with c.cursor() as cur:
            for good in ("ISO, MDY", "German", "SQL, DMY", "Postgres, YMD"):
                cur.execute(f"SET DateStyle = '{good}'")
                assert cur.statusmessage == "SET", good
            for good in ("iso_8601", "postgres", "sql_standard", "postgres_verbose"):
                cur.execute(f"SET IntervalStyle = '{good}'")
                assert cur.statusmessage == "SET", good
            for bad in ("SET DateStyle = 'bogus'", "SET IntervalStyle = 'nope'"):
                with pytest.raises(psycopg.Error) as ei:
                    cur.execute(bad)
                assert ei.value.sqlstate == "22023", (bad, ei.value.sqlstate)
    finally:
        c.close()


def test_standalone_commit_rollback_no_txn_warns_25p01():
    # COMMIT/ROLLBACK with no active transaction -> PG WARNING 25P01
    # "there is no transaction in progress" (not 01000 + the raw DuckDB text).
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    diags: list[tuple] = []
    c.add_notice_handler(lambda d: diags.append((d.sqlstate, d.message_primary)))
    try:
        with c.cursor() as cur:
            cur.execute("COMMIT")
            cur.execute("ROLLBACK")
        assert [s for s, _ in diags] == ["25P01", "25P01"], diags
        assert all("there is no transaction in progress" in m for _, m in diags), diags
    finally:
        c.close()


def test_copy_text_header():
    # PG honors HEADER in FORMAT text (DuckDB treats it as csv-only): TO STDOUT
    # emits the column-name header line, FROM STDIN skips the first line.
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    table = f"cphdr_{os.getpid()}"
    try:
        with c.cursor() as cur:
            with cur.copy(
                "COPY (SELECT 1 AS a, 'x' AS b) TO STDOUT (FORMAT text, HEADER)"
            ) as cp:
                out = b"".join(cp)
            assert out == b"a\tb\n1\tx\n", out

            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'CREATE TABLE "{table}" (a int, b text)')
            with cur.copy(
                f'COPY "{table}" FROM STDIN (FORMAT text, HEADER)'
            ) as cp:
                cp.write("a\tb\n1\tx\n2\ty\n")
            rows = cur.execute(
                f'SELECT a, b FROM "{table}" ORDER BY a'
            ).fetchall()
            assert rows == [(1, "x"), (2, "y")], rows
    finally:
        with c.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        c.close()


def test_pg_cancel_backend_cancels_running_query():
    # A second session cancels the victim's in-flight query by pid; the victim's
    # query fails with 57014 (query_canceled).
    admin = psycopg.connect(**conn_kwargs(), autocommit=True)
    victim = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        vpid = victim.execute("select pg_backend_pid()").fetchone()[0]
        outcome: dict[str, object] = {}

        def run_long():
            try:
                victim.execute(
                    "select count(*) from generate_series(1, 100000000000) g(i)"
                )
                outcome["sqlstate"] = "completed"
            except psycopg.Error as e:
                outcome["sqlstate"] = e.sqlstate

        worker = threading.Thread(target=run_long)
        worker.start()
        try:
            for _ in range(50):  # let the query start, then cancel (poll up to ~5s)
                time.sleep(0.1)
                admin.execute("select pg_cancel_backend(%s)", (vpid,))
                if not worker.is_alive():
                    break
        finally:
            worker.join(timeout=15)
        assert not worker.is_alive(), "victim query was not cancelled"
        assert outcome.get("sqlstate") == "57014", outcome
    finally:
        admin.close()
        victim.close()
