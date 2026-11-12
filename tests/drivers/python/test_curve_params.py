from __future__ import annotations

import psycopg
import pytest
from spec_loader import conn_kwargs, schema_name

DRIVER_KEY = "python_curve_params"

ROWS = 3000
CALLS = {
    "cp_morton": "curve(ROW(x, y))",
    "cp_hilbert": "curve(ROW(x, y), kind := 'hilbert')",
    "cp_coarse": "curve(ROW(x, y), max_level := 3)",
}
DDL = [
    """CREATE TABLE {schema}.cp (pk INTEGER PRIMARY KEY, x BIGINT, y BIGINT, tag INTEGER,
        body VARCHAR, emb FLOAT[2])""",
    *(
        f"""CREATE INDEX {index} ON {{schema}}.cp USING inverted(pk, tag, ({call}), (lower(body)),
            emb hnsw (metric = 'l2', quant = 'none'))"""
        for index, call in CALLS.items()
    ),
    f"""INSERT INTO {{schema}}.cp SELECT i, i % 50, (i * 7) % 50, i % 5,
        CASE WHEN i % 2 = 0 THEN 'Alpha' ELSE 'Beta' END, [i::FLOAT, 0]::FLOAT[2]
        FROM range({ROWS}) t(i)""",
    "VACUUM (REFRESH_TABLE) {schema}.cp",
]


def row(pk: int) -> dict:
    return {
        "pk": pk,
        "x": pk % 50,
        "y": pk * 7 % 50,
        "tag": pk % 5,
        "body": "alpha" if pk % 2 == 0 else "beta",
    }


TABLE = [row(pk) for pk in range(ROWS)]


def in_box(r: dict, lx=10, hx=30, ly=10, hy=30, left=True, right=True) -> bool:
    def holds(value, low, high):
        return (value >= low if left else value > low) and (value <= high if right else value < high)

    return holds(r["x"], lx, hx) and holds(r["y"], ly, hy)


BOX = "{call} @@ ts_between(ROW(10, 10), ROW(30, 30), true, true)"
CASES = [
    ("tag = %s AND " + BOX, (3,), lambda r: r["tag"] == 3 and in_box(r)),
    (BOX + " AND tag = %s", (4,), lambda r: r["tag"] == 4 and in_box(r)),
    ("pk >= %s AND " + BOX, (1000,), lambda r: r["pk"] >= 1000 and in_box(r)),
    ("tag = %s AND lower(body) = %s AND " + BOX, (3, "beta"),
     lambda r: r["tag"] == 3 and r["body"] == "beta" and in_box(r)),
    ("lower(body) = %s AND tag = %s", ("alpha", 2), lambda r: r["body"] == "alpha" and r["tag"] == 2),
    ("tag = %s AND lower(body) = 'beta'", (2,), lambda r: r["body"] == "beta" and r["tag"] == 2),
    ("x BETWEEN %s AND %s AND y BETWEEN %s AND %s", (5, 20, 15, 40),
     lambda r: in_box(r, 5, 20, 15, 40)),
    ("tag = %s AND x BETWEEN 10 AND 30 AND y BETWEEN 10 AND 30", (1,),
     lambda r: r["tag"] == 1 and in_box(r)),
    ("{call} @@ ts_between(ROW(%s::BIGINT, 10), ROW(%s::BIGINT, 30), true, true)", (10, 30),
     lambda r: in_box(r)),
    ("{call} @@ ts_between(ROW(%s, %s), ROW(%s, %s), true, true)", (5, 15, 20, 40),
     lambda r: in_box(r, 5, 20, 15, 40)),
    ("{call} @@ ts_between(ROW(10, 10), ROW(30, 30), %s, %s)", (False, True),
     lambda r: in_box(r, left=False, right=True)),
    ("tag = %s AND {call} @@ ts_between(ROW(%s, 10), ROW(30, 30), %s, %s)", (2, 10, True, False),
     lambda r: r["tag"] == 2 and in_box(r, right=False)),
    ("{call} @@ ts_between(ROW(%s::BIGINT, 10), ROW(NULL, 30), true, true)", (40,),
     lambda r: r["x"] >= 40 and 10 <= r["y"] <= 30),
    ("{call} @@ ts_between(ROW(%s::BIGINT, 10), ROW(30, 30), true, true)", (None,),
     lambda r: r["x"] <= 30 and 10 <= r["y"] <= 30),
]


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


@pytest.mark.parametrize("index", CALLS)
@pytest.mark.parametrize("where,params,test", CASES)
@pytest.mark.parametrize("prepare", [True, False])
def test_box_params(conn, schema, index, where, params, test, prepare):
    sql = (
        f"SELECT count(*), coalesce(sum(pk), 0) FROM {schema}.{index} "
        f"WHERE {where.format(call=CALLS[index])}"
    )
    ids = [r["pk"] for r in TABLE if test(r)]
    with conn.cursor() as cur:
        for _ in range(3):
            cur.execute(sql, params, prepare=prepare)
            assert cur.fetchall() == [(len(ids), sum(ids))]


@pytest.mark.parametrize("index", CALLS)
def test_values_change_between_executions(conn, schema, index):
    sql = (
        f"SELECT count(*) FROM {schema}.{index} "
        f"WHERE tag = %s AND {BOX.format(call=CALLS[index])}"
    )
    with conn.cursor() as cur:
        for tag in (3, 4, 3, 77):
            cur.execute(sql, (tag,), prepare=True)
            expected = sum(1 for r in TABLE if r["tag"] == tag and in_box(r))
            assert cur.fetchall() == [(expected,)]


@pytest.mark.parametrize("index", CALLS)
@pytest.mark.parametrize(
    "where,params,test",
    [
        ("tag = %s AND " + BOX, (3,), lambda r: r["tag"] == 3 and in_box(r)),
        ("tag = %s AND x BETWEEN 10 AND 30 AND y BETWEEN 10 AND 30", (3,),
         lambda r: r["tag"] == 3 and in_box(r)),
        ("x BETWEEN %s AND 30 AND y BETWEEN 10 AND 30", (10,), lambda r: in_box(r)),
    ],
)
def test_nearest_rows_in_a_box(conn, schema, index, where, params, test):
    sql = (
        f"SELECT pk FROM {schema}.{index} WHERE {where.format(call=CALLS[index])} "
        "ORDER BY emb <-> [0, 0]::FLOAT[2] LIMIT %s"
    )
    expected = [r["pk"] for r in TABLE if test(r)][:3]
    with conn.cursor() as cur:
        for _ in range(3):
            cur.execute(sql, (*params, 3), prepare=True)
            assert [r[0] for r in cur.fetchall()] == expected


def test_new_rows_reach_a_prepared_statement(conn, schema):
    sql = (
        f"SELECT count(*), sum(pk) FROM {schema}.cpn_idx WHERE tag = %s "
        "AND curve(ROW(x, y)) @@ ts_between(ROW(0, 0), ROW(4, 4), true, true)"
    )
    pks = []
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {schema}.cpn (pk INTEGER PRIMARY KEY, x BIGINT, y BIGINT, tag INTEGER)"
        )
        cur.execute(
            f"CREATE INDEX cpn_idx ON {schema}.cpn USING inverted(pk, tag, (curve(ROW(x, y))))"
        )
        for step in range(4):
            cur.execute(
                f"INSERT INTO {schema}.cpn SELECT {step} * 100 + i, i % 10, i % 10, i % 2 "
                "FROM range(100) t(i)"
            )
            cur.execute(f"VACUUM (REFRESH_TABLE) {schema}.cpn")
            pks += [step * 100 + i for i in range(100) if i % 10 <= 4 and i % 2 == 1]
            for _ in range(2):
                cur.execute(sql, (1,), prepare=True)
                assert cur.fetchall() == [(len(pks), sum(pks))]
