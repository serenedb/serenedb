from __future__ import annotations

import os

import psycopg
from spec_loader import conn_kwargs


def test_dml_cte_results():
    prefix = f"dmlcte_{os.getpid()}"
    with psycopg.connect(**conn_kwargs(), autocommit=True) as c, c.cursor() as cur:
        cur.execute(f"CREATE TABLE {prefix}_a (x INT)")
        cur.execute(f"CREATE TABLE {prefix}_b (y INT)")
        try:
            for statement, tag in (
                (f"WITH c AS (INSERT INTO {prefix}_a VALUES (7) RETURNING x) INSERT INTO {prefix}_b VALUES (5)",
                 "INSERT 0 1"),
                (f"WITH c AS (UPDATE {prefix}_b SET y = 2 RETURNING y) UPDATE {prefix}_a SET x = 3", "UPDATE 1"),
                (f"WITH c AS (DELETE FROM {prefix}_a RETURNING x) DELETE FROM {prefix}_b", "DELETE 1"),
            ):
                cur.execute(statement)
                assert cur.statusmessage == tag
                assert cur.description is None
            cur.execute(f"WITH c AS (INSERT INTO {prefix}_a VALUES (9) RETURNING x) "
                        f"INSERT INTO {prefix}_b VALUES (6) RETURNING y")
            assert cur.statusmessage == "INSERT 0 1"
            assert cur.fetchall() == [(6,)]
            for statement in (
                f"WITH c AS (INSERT INTO {prefix}_a VALUES (1)) SELECT 42 AS v",
                f"WITH c AS (INSERT INTO {prefix}_a VALUES (1) RETURNING x) SELECT 42 AS v",
            ):
                cur.execute(statement)
                assert cur.statusmessage == "SELECT 1"
                assert cur.fetchall() == [(42,)]
        finally:
            cur.execute(f"DROP TABLE {prefix}_a")
            cur.execute(f"DROP TABLE {prefix}_b")
