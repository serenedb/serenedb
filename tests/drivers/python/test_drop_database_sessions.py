"""DROP DATABASE waits for the database's other sessions, as PostgreSQL does.

A session that ends while the drop waits does not fail it; one that stays fails
it once the wait, five seconds as in PostgreSQL, runs out.
"""

from __future__ import annotations

import os
import threading
import time

import psycopg
import pytest
from spec_loader import conn_kwargs


def _connect(dbname: str | None = None) -> psycopg.Connection:
    kwargs = conn_kwargs()
    if dbname is not None:
        kwargs["dbname"] = dbname
    return psycopg.connect(**kwargs, autocommit=True)


def test_drop_database_waits_for_a_closing_session():
    database = f"dds_closing_{os.getpid()}"
    admin = _connect()
    try:
        admin.execute(f"CREATE DATABASE {database}")
        other = _connect(database)
        other.execute("SELECT 1")
        closer = threading.Timer(1.0, other.close)
        closer.start()
        started = time.monotonic()
        admin.execute(f"DROP DATABASE {database}")
        assert time.monotonic() - started < 5.0
        closer.join()
    finally:
        admin.execute(f"DROP DATABASE IF EXISTS {database}")
        admin.close()


def test_drop_database_refuses_a_session_that_stays():
    database = f"dds_staying_{os.getpid()}"
    admin = _connect()
    try:
        admin.execute(f"CREATE DATABASE {database}")
        other = _connect(database)
        try:
            other.execute("SELECT 1")
            started = time.monotonic()
            with pytest.raises(psycopg.errors.ObjectInUse,
                               match="is being accessed by other users"):
                admin.execute(f"DROP DATABASE {database}")
            assert time.monotonic() - started >= 4.5
        finally:
            other.close()
        admin.execute(f"DROP DATABASE {database}")
    finally:
        admin.execute(f"DROP DATABASE IF EXISTS {database}")
        admin.close()
