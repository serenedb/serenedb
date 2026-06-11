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

import psycopg
import pytest
from psycopg.pq import TransactionStatus
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
#
# Regression: the server hard-coded the ReadyForQuery status byte to 'I'
# (idle), so psycopg -- which (default autocommit=False) manages transactions
# client-side off that byte -- never saw an open transaction. commit() became
# a no-op (it never sent COMMIT) and redundant BEGINs piled up, leaving tables
# empty. The fixtures above force autocommit=True, where the status byte is
# irrelevant, which is why this went unnoticed. The tests below take the
# default autocommit=False path so the live I/T/E status is exercised.


def test_ready_for_query_transaction_status():
    """ReadyForQuery must carry the live txn status (I/T/E), not a constant."""
    c = psycopg.connect(**conn_kwargs())  # default autocommit=False
    try:
        # Nothing executed yet: idle, outside any transaction block -> 'I'.
        assert c.pgconn.transaction_status == TransactionStatus.IDLE

        # The first statement opens an implicit transaction block -> 'T'.
        with c.cursor() as cur:
            cur.execute("SELECT 1")
        assert c.pgconn.transaction_status == TransactionStatus.INTRANS

        # A primary-key violation invalidates the transaction; the block is now
        # in the failed state -> 'E'. The CREATE/INSERT live in the same
        # uncommitted block, so the ROLLBACK below discards them -- no cleanup.
        with c.cursor() as cur:
            cur.execute("CREATE TABLE rfq_fail (id int primary key)")
            cur.execute("INSERT INTO rfq_fail VALUES (1)")
        with pytest.raises(psycopg.Error):
            with c.cursor() as cur:
                cur.execute("INSERT INTO rfq_fail VALUES (1)")
        assert c.pgconn.transaction_status == TransactionStatus.INERROR

        # ROLLBACK ends the block -> back to idle.
        c.rollback()
        assert c.pgconn.transaction_status == TransactionStatus.IDLE
    finally:
        c.close()


def test_managed_transaction_commit_persists():
    """commit() under autocommit=False must actually send COMMIT and persist.

    With the bug the status byte stayed 'I', so psycopg treated commit() as a
    no-op; CREATE/INSERT were never committed and a second session saw nothing.
    """
    table = f"rfq_commit_{os.getpid()}"
    writer = psycopg.connect(**conn_kwargs())  # default autocommit=False
    try:
        with writer.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        writer.commit()

        with writer.cursor() as cur:
            cur.execute(f'CREATE TABLE "{table}" (id int primary key, v text)')
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
        with writer.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        writer.commit()
        writer.close()
