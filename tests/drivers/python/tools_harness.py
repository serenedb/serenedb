from __future__ import annotations

import os
from pathlib import Path

import psycopg
from spec_loader import conn_kwargs

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "tools"


def database_name(suite: str) -> str:
    return f"py_{suite}_{os.environ.get('SDB_DRV_RUN_ID', '0')}"


def connect(dbname: str) -> psycopg.Connection:
    return psycopg.connect(**(conn_kwargs() | {"dbname": dbname}), autocommit=True)


def statements(path: Path) -> list[str]:
    return [s.strip() for s in path.read_text().split(";\n") if s.strip()]


def create_empty_database(name: str) -> None:
    drop_database(name)
    with connect(conn_kwargs()["dbname"]) as conn:
        conn.execute(f"CREATE DATABASE {name}")


def create_database(name: str) -> None:
    create_empty_database(name)
    with connect(name) as conn:
        for statement in statements(FIXTURES / "schema.sql"):
            conn.execute(statement)


def drop_database(name: str) -> None:
    with connect(conn_kwargs()["dbname"]) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {name}")


def url(dbname: str, driver: str) -> str:
    kw = conn_kwargs()
    return f"postgresql+{driver}://{kw['user']}@{kw['host']}:{kw['port']}/{dbname}"
