"""Prepared-statement parameters in inverted-index search predicates.

`WHERE body @@ $1` over the extended protocol. The
parameter is typed as TSQUERY by the `@@` overload; the wire form of a
TSQUERY value is its query text (OID 25) in both parameter formats, and
DuckDB rebinds the statement per Execute with the value substituted as a
constant, so the claim compiles the filter exactly as for inline queries.
"""

from __future__ import annotations

import struct

import psycopg
import pytest
from spec_loader import conn_kwargs, schema_name

from test_pgwire_raw import WireConn, _cstr, errors, rows, types

DRIVER_KEY = "python_search_params"

DDL = [
    """CREATE TEXT SEARCH DICTIONARY {schema}.sp_english(
        template = 'text', locale = 'en_US.UTF-8', case = 'lower',
        stemming = false, accent = false, frequency = true,
        position = true)""",
    "CREATE TABLE {schema}.sp(a INTEGER PRIMARY KEY, b VARCHAR)",
    """CREATE INDEX sp_idx ON {schema}.sp
        USING inverted(a, b {schema}.sp_english)
        WITH (refresh_interval=0)""",
    """INSERT INTO {schema}.sp VALUES (1, 'quick brown fox'),
        (2, 'lazy dog'), (3, NULL), (4, 'quick dog')""",
    "VACUUM (REFRESH_TABLE) {schema}.sp",
]

MATCH_SQL = "SELECT a FROM {schema}.sp_idx WHERE b @@ $1 ORDER BY a"


@pytest.fixture(scope="module")
def schema() -> str:
    return schema_name(DRIVER_KEY)


@pytest.fixture(scope="module")
def conn(schema: str) -> psycopg.Connection:
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    with c.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        for ddl in DDL:
            cur.execute(ddl.format(schema=schema))
    yield c
    with c.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    c.close()


def _match(conn: psycopg.Connection, schema: str, value, **kwargs):
    sql = MATCH_SQL.replace("$1", "%s").format(schema=schema)
    with conn.cursor() as cur:
        cur.execute(sql, (value,), **kwargs)
        return [r[0] for r in cur.fetchall()]


def test_match_param_text(conn, schema):
    assert _match(conn, schema, "quick") == [1, 4]
    assert _match(conn, schema, "dog") == [2, 4]
    assert _match(conn, schema, "nomatch") == []


def test_match_param_null(conn, schema):
    assert _match(conn, schema, None) == []


def test_match_param_reexecute_prepared(conn, schema):
    sql = MATCH_SQL.replace("$1", "%s").format(schema=schema)
    with conn.cursor() as cur:
        cur.execute(sql, ("quick",), prepare=True)
        assert [r[0] for r in cur.fetchall()] == [1, 4]
        cur.execute(sql, ("lazy",), prepare=True)
        assert [r[0] for r in cur.fetchall()] == [2]


def test_function_form_param(conn, schema):
    sql = (
        f"SELECT a FROM {schema}.sp_idx WHERE b @@ ts_phrase(%s) ORDER BY a"
    )
    with conn.cursor() as cur:
        cur.execute(sql, ("lazy dog",))
        assert [r[0] for r in cur.fetchall()] == [2]


def test_scored_param(conn, schema):
    sql = (
        f"SELECT a FROM {schema}.sp_idx WHERE b @@ %s "
        "ORDER BY bm25(tableoid) DESC, a LIMIT 2"
    )
    with conn.cursor() as cur:
        cur.execute(sql, ("quick",))
        assert [r[0] for r in cur.fetchall()] == [1, 4]


# ---- raw wire: parameter OID and both parameter formats ---------------------


def _bind_with_format(conn: WireConn, portal: str, stmt: str, value: bytes,
                      fmt: int):
    payload = _cstr(portal) + _cstr(stmt)
    payload += struct.pack("!HH", 1, fmt)
    payload += struct.pack("!H", 1)
    payload += struct.pack("!I", len(value)) + value
    payload += struct.pack("!H", 0)
    conn.send("B", payload)


def test_wire_param_oid_and_formats(schema):
    c = WireConn()
    try:
        c.run(f'SET search_path TO "{schema}", public, pg_catalog')
        c.parse("sp1", MATCH_SQL.format(schema=schema))
        c.describe("S", "sp1")
        c.sync()
        msgs = c.drain_to_ready()
        assert not errors(msgs), errors(msgs)
        # ParameterDescription: TSQUERY presents as text on the wire.
        (param_desc,) = [p for t, p in msgs if t == "t"]
        count, oid = struct.unpack("!HI", param_desc[:6])
        assert (count, oid) == (1, 25)

        for fmt in (0, 1):
            _bind_with_format(c, "", "sp1", b"quick", fmt)
            c.describe("P", "")
            c.execute("")
            c.sync()
            msgs = c.drain_to_ready()
            assert not errors(msgs), (fmt, errors(msgs))
            # RowDescription resolves the real column name despite the
            # parameter-typed template.
            (row_desc,) = [p for t, p in msgs if t == "T"]
            assert row_desc[2:].startswith(b"a\0")
            assert len(rows(msgs)) == 2, (fmt, types(msgs))

        # NULL parameter (wire length -1): matches nothing, no error.
        c.bind("", "sp1", params=(None,))
        c.execute("")
        c.sync()
        msgs = c.drain_to_ready()
        assert not errors(msgs), errors(msgs)
        assert len(rows(msgs)) == 0, types(msgs)

        # Binary result format over a parameterized claim.
        c.bind("", "sp1", params=(b"dog",), result_format=1)
        c.execute("")
        c.sync()
        msgs = c.drain_to_ready()
        assert not errors(msgs), errors(msgs)
        got = [struct.unpack("!I", r[6:10])[0] for r in rows(msgs)]
        assert got == [2, 4], got

        # Portal paging: suspend after one row, resume before Sync (a Sync
        # outside a transaction block ends the implicit transaction and
        # destroys the portal, per PG semantics).
        c.bind("cur1", "sp1", params=(b"quick",))
        c.execute("cur1", max_rows=1)
        c.execute("cur1")
        c.sync()
        msgs = c.drain_to_ready()
        assert not errors(msgs), errors(msgs)
        kinds = types(msgs)
        assert "s" in kinds and "C" in kinds, kinds
        got = [r[6 : 6 + struct.unpack("!I", r[2:6])[0]] for r in rows(msgs)]
        assert got == [b"1", b"4"], got
    finally:
        c.close()
