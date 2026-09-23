from __future__ import annotations

import psycopg
import pytest
from spec_loader import conn_kwargs, schema_name

DRIVER_KEY = "python_search_range_params"

DDL = [
    """CREATE TABLE {schema}.srp (pk INTEGER PRIMARY KEY, num INTEGER, tag VARCHAR, emb FLOAT[2])
        WITH (storage = 'search', compaction_interval = 0)""",
    """CREATE INDEX srp_idx ON {schema}.srp
        USING inverted(pk, num, tag, emb hnsw (metric = 'l2', quant = 'none'))""",
    """INSERT INTO {schema}.srp SELECT i, i, CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END,
        [i::FLOAT, 0]::FLOAT[2] FROM range(2000) t(i)""",
    "VACUUM (REFRESH_TABLE) {schema}.srp",
]

KNN = "SELECT pk FROM {schema}.srp WHERE {where} ORDER BY emb <-> [1600.4, 0]::FLOAT[2] LIMIT 3"


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


@pytest.mark.parametrize(
    "op,value,expected",
    [
        ("<=", 1500, [1500, 1499, 1498]),
        ("<", 1500, [1499, 1498, 1497]),
        ("<=", 1000, [1000, 999, 998]),
        (">=", 1700, [1700, 1701, 1702]),
        (">", 1700, [1701, 1702, 1703]),
    ],
)
def test_range_param(conn, schema, op, value, expected):
    sql = KNN.format(schema=schema, where=f"num {op} %s")
    with conn.cursor() as cur:
        for _ in range(2):
            cur.execute(sql, (value,), prepare=True)
            assert [r[0] for r in cur.fetchall()] == expected


def test_term_and_range_params(conn, schema):
    sql = KNN.format(schema=schema, where="tag = %s AND num <= %s")
    with conn.cursor() as cur:
        for _ in range(2):
            cur.execute(sql, ("odd", 1500), prepare=True)
            assert [r[0] for r in cur.fetchall()] == [1499, 1497, 1495]


def test_between_params(conn, schema):
    sql = KNN.format(schema=schema, where="num BETWEEN %s AND %s")
    with conn.cursor() as cur:
        for _ in range(2):
            cur.execute(sql, (1200, 1500), prepare=True)
            assert [r[0] for r in cur.fetchall()] == [1500, 1499, 1498]
