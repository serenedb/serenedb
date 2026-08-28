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


# ---- modifiers over the client protocol -------------------------------------
#
# A modifier travels inside the TSQUERY value, not only in its type: the struct
# carries a slop and a scorer alongside the text, tokenizer and boost. These
# pin what a client sees of that -- including that a plain value still renders
# as its bare text, because the TSQUERY -> VARCHAR cast feeds overload
# resolution and quoting an unmodified value would turn a term into a quoted
# term.
#
# Both spellings are covered: a modifier on a literal operand, and one on a
# bound parameter, which reaches the value through a different road entirely.


def test_modifier_scorer_literal(conn, schema):
    sql = (
        f"SELECT a, bm25(tableoid) FROM {schema}.sp_idx "
        "WHERE b @@ 'quick'::score('constant(42)') ORDER BY a"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        assert [(r[0], round(r[1], 4)) for r in cur.fetchall()] == [
            (1, 42.0), (4, 42.0)
        ]


def test_modifier_unscored_literal(conn, schema):
    sql = (
        f"SELECT a, bm25(tableoid) FROM {schema}.sp_idx "
        "WHERE b @@ 'quick'::score(NULL) ORDER BY a"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        assert [(r[0], round(r[1], 4)) for r in cur.fetchall()] == [
            (1, 0.0), (4, 0.0)
        ]


def test_modifier_in_list_element(conn, schema):
    # The modifier applies to its own element only: doc 4 matches both, so it
    # collects the constant plus the other element's ordinary score.
    sql = (
        f"SELECT a, bm25(tableoid) FROM {schema}.sp_idx "
        "WHERE b @@ ts_any(['quick'::score('constant(42)'), 'dog']) ORDER BY a"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        got = dict((r[0], round(r[1], 4)) for r in cur.fetchall())
    assert sorted(got) == [1, 2, 4]
    assert got[1] == 42.0
    assert got[2] < 42.0
    assert got[4] > 42.0


def test_modifier_param_scorer(conn, schema):
    # `$1::score(...)` names the modifier type directly, so DuckDB types the
    # parameter as that type and leaves no cast behind. The modifier therefore
    # has to be folded in when the wire value is materialised, or it is gone
    # before anything can read it.
    sql = (
        f"SELECT a, bm25(tableoid) FROM {schema}.sp_idx "
        "WHERE b @@ %s::score('constant(42)') ORDER BY a"
    )
    with conn.cursor() as cur:
        cur.execute(sql, ("quick",))
        assert [(r[0], round(r[1], 4)) for r in cur.fetchall()] == [
            (1, 42.0), (4, 42.0)
        ]


def test_modifier_param_unscored(conn, schema):
    sql = (
        f"SELECT a, bm25(tableoid) FROM {schema}.sp_idx "
        "WHERE b @@ %s::score(NULL) ORDER BY a"
    )
    with conn.cursor() as cur:
        cur.execute(sql, ("quick",))
        assert [(r[0], round(r[1], 4)) for r in cur.fetchall()] == [
            (1, 0.0), (4, 0.0)
        ]


def test_modifier_param_boost(conn, schema):
    # Boost travels the same road, so it is asserted against the unmodified
    # score rather than a constant: five times, whatever the column scores.
    plain = f"SELECT a, bm25(tableoid) FROM {schema}.sp_idx WHERE b @@ %s ORDER BY a"
    boosted = (
        f"SELECT a, bm25(tableoid) FROM {schema}.sp_idx "
        "WHERE b @@ %s::boost(5.0) ORDER BY a"
    )
    with conn.cursor() as cur:
        cur.execute(plain, ("quick",))
        base = dict(cur.fetchall())
        cur.execute(boosted, ("quick",))
        got = dict(cur.fetchall())
    assert sorted(got) == sorted(base) == [1, 4]
    for a, score in got.items():
        assert round(score, 4) == round(base[a] * 5.0, 4)


def test_modifier_param_reexecute_prepared(conn, schema):
    sql = (
        f"SELECT a, bm25(tableoid) FROM {schema}.sp_idx "
        "WHERE b @@ %s::score('constant(7)') ORDER BY a"
    )
    with conn.cursor() as cur:
        cur.execute(sql, ("quick",), prepare=True)
        assert [(r[0], round(r[1], 4)) for r in cur.fetchall()] == [
            (1, 7.0), (4, 7.0)
        ]
        cur.execute(sql, ("lazy",), prepare=True)
        assert [(r[0], round(r[1], 4)) for r in cur.fetchall()] == [(2, 7.0)]


def test_modifier_param_in_list_element(conn, schema):
    sql = (
        f"SELECT a, bm25(tableoid) FROM {schema}.sp_idx "
        "WHERE b @@ ts_any([%s::score('constant(42)'), 'dog']) ORDER BY a"
    )
    with conn.cursor() as cur:
        cur.execute(sql, ("quick",))
        got = dict((r[0], round(r[1], 4)) for r in cur.fetchall())
    assert sorted(got) == [1, 2, 4]
    assert got[1] == 42.0
    assert got[2] < 42.0
    assert got[4] > 42.0


def test_tsquery_value_text_form(conn):
    with conn.cursor() as cur:
        # Plain value: bare text, no quoting.
        cur.execute("SELECT 'fox'::TSQUERY::VARCHAR")
        assert cur.fetchone()[0] == "fox"
        # Carrying every modifier: reads back as an expression that rebuilds it.
        cur.execute(
            "SELECT (ts_phrase('a', 'b')::slop(2)::boost(3.0)"
            "::score('constant(7)'))::TSQUERY::VARCHAR"
        )
        assert cur.fetchone()[0] == (
            "((ts_phrase('a', 'b'))::slop(2) ^ 3)::score('constant(7)')"
        )
        # ::score(NULL) is its own state, distinct from no scorer at all.
        cur.execute("SELECT ('fox'::TSQUERY)::score(NULL)::TSQUERY::VARCHAR")
        assert cur.fetchone()[0] == "'fox'::score(NULL)"


# ---- raw wire: parameter OID and both parameter formats ---------------------


def _bind_with_format(conn: WireConn, portal: str, stmt: str, value: bytes,
                      fmt: int):
    payload = _cstr(portal) + _cstr(stmt)
    payload += struct.pack("!HH", 1, fmt)
    payload += struct.pack("!H", 1)
    payload += struct.pack("!I", len(value)) + value
    payload += struct.pack("!H", 0)
    conn.send("B", payload)


def _data_row_fields(payload: bytes) -> list[bytes | None]:
    """DataRow payload -> its column values (None for a -1 length)."""
    (count,) = struct.unpack_from("!H", payload, 0)
    out: list[bytes | None] = []
    off = 2
    for _ in range(count):
        (size,) = struct.unpack_from("!i", payload, off)
        off += 4
        if size < 0:
            out.append(None)
            continue
        out.append(payload[off:off + size])
        off += size
    return out


def test_wire_param_modifier_both_formats(schema):
    # The deepest form of the same check: a modifier-typed parameter over the
    # raw protocol, in both parameter formats. The scored rows come back with
    # the constant, which is only possible if the modifier survived being
    # materialised from the wire bytes.
    scored_sql = (
        f"SELECT a, bm25(tableoid) FROM {schema}.sp_idx "
        "WHERE b @@ $1::score('constant(42)') ORDER BY a"
    )
    c = WireConn()
    try:
        c.run(f'SET search_path TO "{schema}", public, pg_catalog')
        c.parse("spm", scored_sql)
        c.describe("S", "spm")
        c.sync()
        msgs = c.drain_to_ready()
        assert not errors(msgs), errors(msgs)
        # The parameter still presents as text on the wire even though its type
        # carries a modifier.
        (param_desc,) = [p for t, p in msgs if t == "t"]
        count, oid = struct.unpack("!HI", param_desc[:6])
        assert (count, oid) == (1, 25)

        for fmt in (0, 1):
            _bind_with_format(c, "", "spm", b"quick", fmt)
            c.execute("")
            c.sync()
            msgs = c.drain_to_ready()
            assert not errors(msgs), (fmt, errors(msgs))
            data = rows(msgs)
            assert len(data) == 2, (fmt, types(msgs))
            # Second column is the score: exactly the constant.
            scores = [_data_row_fields(p)[1] for p in data]
            assert all(float(s) == 42.0 for s in scores), (fmt, scores)
    finally:
        c.close()


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
