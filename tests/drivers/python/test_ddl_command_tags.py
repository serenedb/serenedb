"""Command tags of CREATE DATABASE, triggers and dictionaries match PostgreSQL."""

from __future__ import annotations

import os

import psycopg
from spec_loader import conn_kwargs


def test_ddl_command_tags():
    suffix = os.getpid()
    database = f"cmdtag_db_{suffix}"
    prefix = f"cmdtag_{suffix}"
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        with c.cursor() as cur:
            cur.execute(f"CREATE TEXT SEARCH DICTIONARY {prefix}_dict AS [keyword()]")
            for statement, tag in (
                (f"CREATE DATABASE {database}", "CREATE DATABASE"),
                (f"CREATE TABLE {prefix}_t (id INT)", "CREATE TABLE"),
                (f"CREATE TABLE {prefix}_log (id INT)", "CREATE TABLE"),
                (f"CREATE TRIGGER {prefix}_trg AFTER INSERT ON {prefix}_t FOR EACH ROW "
                 f"INSERT INTO {prefix}_log VALUES (NEW.id)", "CREATE TRIGGER"),
                (f"DROP TRIGGER {prefix}_trg ON {prefix}_t", "DROP TRIGGER"),
                (f"DROP TEXT SEARCH DICTIONARY {prefix}_dict", "DROP TEXT SEARCH DICTIONARY"),
                (f"DROP TABLE {prefix}_t", "DROP TABLE"),
                (f"DROP TABLE {prefix}_log", "DROP TABLE"),
            ):
                cur.execute(statement)
                assert cur.statusmessage == tag
                assert cur.description is None
            cur.execute(f"DROP DATABASE {database}")
    finally:
        c.close()
