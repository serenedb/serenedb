"""COMMIT of a transaction block that hit an error reports ROLLBACK, as in PostgreSQL."""

from __future__ import annotations

import psycopg
import pytest
from spec_loader import conn_kwargs


@pytest.mark.parametrize("cursor_factory", [psycopg.Cursor, psycopg.ClientCursor])
def test_commit_of_aborted_block_reports_rollback(cursor_factory):
    c = psycopg.connect(**conn_kwargs(), autocommit=True, cursor_factory=cursor_factory)
    try:
        with c.cursor() as cur:
            cur.execute("BEGIN")
            with pytest.raises(psycopg.Error):
                cur.execute("SELECT * FROM papich_missing_table")
            cur.execute("COMMIT")
            assert cur.statusmessage == "ROLLBACK"

            cur.execute("BEGIN")
            cur.execute("SELECT 1")
            cur.execute("COMMIT")
            assert cur.statusmessage == "COMMIT"
    finally:
        c.close()
