"""asyncpg driver tests against SereneDB.

asyncpg always uses extended protocol with binary params for known types
and text for unknown ones. Round-trip same as psycopg3 but driven through
asyncio.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest
import pytest_asyncio
from spec_loader import (
    TypeSpec,
    cases_for_driver,
    conn_kwargs,
    schema_name,
)

DRIVER_KEY = "python_asyncpg"


# Function-scoped fixture: every test gets its own connection. Slower than
# session-scope but avoids pytest-asyncio's "Future attached to different
# loop" trap when the session fixture's loop differs from the per-test loop.
#
# We deliberately do NOT drop the schema on teardown: serened sometimes
# wedges on DROP SCHEMA CASCADE after binary-array tests (suspected pending-
# op cleanup race). The schema name is per-run-id and the data dir is
# throwaway, so leaking the schema for the run is harmless.
@pytest_asyncio.fixture
async def conn():
    kw = conn_kwargs()
    c = await asyncpg.connect(
        host=kw["host"], port=kw["port"],
        database=kw["dbname"], user=kw["user"],
    )
    schema = schema_name(DRIVER_KEY)
    await c.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await c.execute(f'SET search_path TO "{schema}", public, pg_catalog')
    yield c
    # Use terminate(), not close(): close() awaits a Terminate-ack from the
    # server which intermittently hangs the per-test fixture under load
    # (asyncpg sits on epoll waiting for a reply that never comes). For
    # test cleanup we just want the FD released; terminate() does that
    # without the round-trip.
    c.terminate()


@pytest.mark.asyncio
async def test_smoke_select_one(conn):
    v = await conn.fetchval("SELECT 1")
    assert v == 1


@pytest.mark.asyncio
async def test_smoke_server_version(conn):
    v = await conn.fetchval("SHOW server_version")
    assert isinstance(v, str) and v


CASES = list(cases_for_driver(DRIVER_KEY))


def _id(case):
    ts, p, idx, _ = case
    return f"{ts.oid}-{ts.name}-{p}-{idx}"


async def _server_text(conn, ts: TypeSpec, sample):
    if sample is None:
        return None
    lit = sample.replace("'", "''")
    return await conn.fetchval(f"SELECT ('{lit}'::{ts.pg_typname})::text")


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES, ids=_id)
async def test_roundtrip(conn, case):
    ts, proto, _idx, sample = case
    expected = await _server_text(conn, ts, sample)

    if proto in ("simple", "extended-noparam"):
        if sample is None:
            sql = f"SELECT NULL::{ts.pg_typname}::text"
        else:
            lit = sample.replace("'", "''")
            sql = f"SELECT '{lit}'::{ts.pg_typname}::text"
        actual = await conn.fetchval(sql)
        assert actual == expected
        return

    # asyncpg insists on the Python type matching the PG OID, so we coerce
    # the sample string to a native value per type. Mirrors what a real
    # asyncpg user does and is the only protocol-meaningful path.
    bind = _coerce_for_asyncpg(ts.pg_typname, sample)
    sql = f"SELECT ({ts.cast_sql})::text"
    actual = await conn.fetchval(sql, bind)
    assert actual == expected


def _coerce_for_asyncpg(pg_typname: str, sample):
    """Convert a sample string to the Python value asyncpg's strict
    binder expects for a given PG type. Returns the sample unchanged
    when the type accepts plain strings (text/varchar/json)."""
    if sample is None:
        return None
    s = str(sample)
    import datetime as _dt
    import decimal
    import uuid as _uuid
    if pg_typname == "bool":
        return s in ("t", "true", "TRUE", "1")
    if pg_typname in ("int2", "int4", "int8", "oid"):
        return int(s)
    if pg_typname in ("float4", "float8"):
        if s in ("Infinity", "inf"):
            return float("inf")
        if s in ("-Infinity", "-inf"):
            return float("-inf")
        if s == "NaN":
            return float("nan")
        return float(s)
    if pg_typname == "numeric":
        if s == "NaN":
            return decimal.Decimal("NaN")
        return decimal.Decimal(s)
    if pg_typname == "uuid":
        return _uuid.UUID(s)
    if pg_typname == "date":
        # asyncpg encodes Python date.MAXYEAR / MINYEAR as PG +/-infinity
        # sentinels (DATEVAL_NOEND / DATEVAL_NOBEGIN). Map our matrix
        # samples back to those Python values for the bind path.
        if s == "infinity":
            return _dt.date(_dt.MAXYEAR, 12, 31)
        if s == "-infinity":
            return _dt.date(_dt.MINYEAR, 1, 1)
        return _dt.date.fromisoformat(s)
    if pg_typname == "time":
        return _dt.time.fromisoformat(s)
    if pg_typname == "timetz":
        # Python's time.fromisoformat handles "HH:MM:SS+HH" / "HH:MM:SS+HH:MM"
        # and produces a tzinfo-bearing time. asyncpg's timetz binder
        # requires that.
        return _dt.time.fromisoformat(s)
    if pg_typname == "timestamp":
        return _dt.datetime.fromisoformat(s.replace(" ", "T"))
    if pg_typname == "timestamptz":
        return _dt.datetime.fromisoformat(s.replace(" ", "T"))
    if pg_typname == "interval":
        # asyncpg accepts datetime.timedelta but not all PG intervals fit
        # (no months/years in timedelta). Skip the year/month-bearing
        # samples by raising; the test will mark that case as a bind
        # error which we intentionally surface in the matrix.
        if any(unit in s for unit in ("year", "mon")):
            raise ValueError(f"interval with year/mon not bindable: {s}")
        return s
    if pg_typname == "bytea":
        if s.startswith("\\x"):
            return bytes.fromhex(s[2:])
        return s.encode("utf-8")
    return s
