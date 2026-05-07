"""psycopg2 (legacy) driver tests against SereneDB.

Same matrix as test_psycopg3, but exercising psycopg2's protocol surface.
psycopg2 always sends parameters as text format (no binary param support),
so we exercise simple, extended-noparam, and extended-text. Skip
extended-binary for this driver.
"""

from __future__ import annotations

import psycopg2
import psycopg2.extensions
import pytest
from spec_loader import (
    PROTOCOL_MODES,
    TypeSpec,
    cases_for_driver,
    conn_kwargs,
    schema_name,
)

DRIVER_KEY = "python_psycopg2"


@pytest.fixture(scope="session")
def conn():
    kw = conn_kwargs()
    c = psycopg2.connect(
        host=kw["host"], port=kw["port"], dbname=kw["dbname"], user=kw["user"]
    )
    c.autocommit = True
    schema = schema_name(DRIVER_KEY)
    with c.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cur.execute(f'SET search_path TO "{schema}", public, pg_catalog')
    yield c
    try:
        with c.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        c.close()


def test_smoke_select_one(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)


def test_smoke_server_version(conn):
    with conn.cursor() as cur:
        cur.execute("SHOW server_version")
        v = cur.fetchone()[0]
        assert isinstance(v, str) and v


CASES = list(cases_for_driver(DRIVER_KEY))


def _id(case):
    ts, p, idx, _ = case
    return f"{ts.oid}-{ts.name}-{p}-{idx}"


def _server_text(conn, ts: TypeSpec, sample):
    if sample is None:
        return None
    lit = sample.replace("'", "''")
    with conn.cursor() as cur:
        cur.execute(f"SELECT ('{lit}'::{ts.pg_typname})::text")
        row = cur.fetchone()
        return row[0] if row else None


@pytest.mark.parametrize("case", CASES, ids=_id)
def test_roundtrip(conn, case):
    ts, proto, _idx, sample = case
    expected = _server_text(conn, ts, sample)

    if proto in ("simple", "extended-noparam"):
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

    # psycopg2 has no binary-format param support; extended-text is the
    # only extended mode meaningful here. The spec already filters
    # extended-binary out via skip[python_psycopg2]: ["extended-binary"].
    sql = f"SELECT ({ts.cast_sql.replace('$1', '%s')})::text AS v"
    bind = sample
    if ts.pg_typname == "bytea" and isinstance(sample, str) and sample.startswith("\\x"):
        bind = bytes.fromhex(sample[2:])
    with conn.cursor() as cur:
        cur.execute(sql, (bind,))
        row = cur.fetchone()
    assert row is not None
    assert row[0] == expected
