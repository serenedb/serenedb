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
            cur.execute(f"CREATE ROLE {prefix}_r")
            cur.execute(f"CREATE ROLE {prefix}_u")
            for statement, tag in (
                (f"CREATE DATABASE {database}", "CREATE DATABASE"),
                (f"CREATE TABLE {prefix}_t (id INT)", "CREATE TABLE"),
                (f"CREATE TABLE {prefix}_log (id INT)", "CREATE TABLE"),
                (f"CREATE TRIGGER {prefix}_trg AFTER INSERT ON {prefix}_t FOR EACH ROW "
                 f"INSERT INTO {prefix}_log VALUES (NEW.id)", "CREATE TRIGGER"),
                (f"DROP TRIGGER {prefix}_trg ON {prefix}_t", "DROP TRIGGER"),
                (f"DROP TEXT SEARCH DICTIONARY {prefix}_dict", "DROP TEXT SEARCH DICTIONARY"),
                (f"CREATE FUNCTION {prefix}_f(x INT) RETURNS INT AS $$ SELECT x $$ LANGUAGE sql", "CREATE FUNCTION"),
                (f"CREATE PROCEDURE {prefix}_p(x INT) AS $$ INSERT INTO {prefix}_log VALUES (x) $$ LANGUAGE sql",
                 "CREATE PROCEDURE"),
                (f"CREATE OR REPLACE PROCEDURE {prefix}_p(x INT) AS $$ INSERT INTO {prefix}_log VALUES (x + 1) $$ "
                 "LANGUAGE sql", "CREATE PROCEDURE"),
                (f"CALL {prefix}_p(1)", "CALL"),
                (f"DROP PROCEDURE {prefix}_p", "DROP PROCEDURE"),
                (f"DROP FUNCTION {prefix}_f", "DROP FUNCTION"),
                (f"GRANT SELECT ON {prefix}_t TO {prefix}_r", "GRANT"),
                (f"REVOKE SELECT ON {prefix}_t FROM {prefix}_r", "REVOKE"),
                (f"GRANT {prefix}_r TO {prefix}_u", "GRANT ROLE"),
                (f"REVOKE {prefix}_r FROM {prefix}_u", "REVOKE ROLE"),
                (f"ALTER ROLE {prefix}_r WITH LOGIN", "ALTER ROLE"),
                (f"ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {prefix}_r", "ALTER DEFAULT PRIVILEGES"),
                (f"ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM {prefix}_r", "ALTER DEFAULT PRIVILEGES"),
                (f"ANALYZE {prefix}_t", "ANALYZE"),
                (f"VACUUM ANALYZE {prefix}_t", "VACUUM"),
                (f"DROP TABLE {prefix}_t", "DROP TABLE"),
                (f"DROP TABLE {prefix}_log", "DROP TABLE"),
            ):
                cur.execute(statement)
                assert cur.statusmessage == tag
                assert cur.description is None
            cur.execute(f"DROP ROLE {prefix}_u")
            cur.execute(f"DROP ROLE {prefix}_r")
            cur.execute(f"DROP DATABASE {database}")
    finally:
        c.close()


def test_permission_command_tags():
    prefix = f"cmdtag_priv_{os.getpid()}"
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        with c.cursor() as cur:
            for statement, tag in (
                (f"CREATE ROLE {prefix}_r", "CREATE ROLE"),
                (f"CREATE ROLE {prefix}_g", "CREATE ROLE"),
                (f"CREATE TABLE {prefix}_t (a INT)", "CREATE TABLE"),
                (f"CREATE SEQUENCE {prefix}_s", "CREATE SEQUENCE"),
                (f"CREATE SCHEMA {prefix}_sch", "CREATE SCHEMA"),
                (f"GRANT SELECT ON {prefix}_t TO {prefix}_r", "GRANT"),
                (f"REVOKE SELECT ON {prefix}_t FROM {prefix}_r", "REVOKE"),
                (f"GRANT USAGE ON SCHEMA {prefix}_sch TO {prefix}_r", "GRANT"),
                (f"REVOKE USAGE ON SCHEMA {prefix}_sch FROM {prefix}_r", "REVOKE"),
                (f"GRANT {prefix}_g TO {prefix}_r", "GRANT ROLE"),
                (f"REVOKE {prefix}_g FROM {prefix}_r", "REVOKE ROLE"),
                (f"ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {prefix}_r",
                 "ALTER DEFAULT PRIVILEGES"),
                (f"ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM {prefix}_r",
                 "ALTER DEFAULT PRIVILEGES"),
                (f"ALTER ROLE {prefix}_r CREATEDB", "ALTER ROLE"),
                (f"ALTER TABLE {prefix}_t OWNER TO {prefix}_r", "ALTER TABLE"),
                (f"ALTER SEQUENCE {prefix}_s OWNER TO {prefix}_r", "ALTER SEQUENCE"),
                (f"ALTER SCHEMA {prefix}_sch OWNER TO {prefix}_r", "ALTER SCHEMA"),
                (f"DROP SEQUENCE {prefix}_s", "DROP SEQUENCE"),
                (f"DROP TABLE {prefix}_t", "DROP TABLE"),
                (f"DROP SCHEMA {prefix}_sch", "DROP SCHEMA"),
                (f"DROP ROLE {prefix}_r", "DROP ROLE"),
                (f"DROP ROLE {prefix}_g", "DROP ROLE"),
            ):
                cur.execute(statement)
                assert cur.statusmessage == tag, (statement, cur.statusmessage)
    finally:
        c.close()
