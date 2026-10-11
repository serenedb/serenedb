from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import tools_harness as harness
from spec_loader import conn_kwargs
from target import check_golden, is_serenedb

DB = harness.database_name("dump")
RESTORED = harness.database_name("restored")
SERENEDB = is_serenedb()
GOLDENS = Path(__file__).resolve().parent / "fixtures" / "pg_dump"
DATA = [
    "INSERT INTO customers (name, email, mood, born, score, created) VALUES "
    "('Pudge', 'pudge@dota', 'happy', '2004-02-01', 12.5, '2024-01-01 00:00:00+00'), "
    "('Lina', NULL, 'ok', NULL, 7.25, '2024-01-02 00:00:00+00')",
    "INSERT INTO items (sku, title, qty, price, tags, meta, added, uid, blob) VALUES "
    "('HOOK0001', 'Meat Hook', 3, 9.5, '{hook,meat}', '{\"range\": 1300}', "
    "'2024-01-02 03:04:05', '00000000-0000-0000-0000-000000000007', '\\x00ff')",
    "INSERT INTO orders (customer_id, sku, placed, note) VALUES "
    "(1, 'HOOK0001', '2024-05-06', 'rush'), (2, NULL, '2024-05-07', NULL)",
    "INSERT INTO order_lines (order_id, line, qty) VALUES (1, 1, 4), (1, 2, 1)",
    "INSERT INTO inventory.stock (sku, warehouse, amount) VALUES ('HOOK0001', 'north', 3)",
    "SELECT setval('order_no', 150)",
]
DUMP_OF_SERENEDB = ("pg_dump stops at its second pg_catalog.set_config() call: with "
                    "search_path = '' SereneDB cannot resolve pg_catalog functions")
RESTORE_ON_SERENEDB = ("a dump sets search_path = '', which moves the session into SereneDB's "
                       "server-wide memory catalog where public does not exist (#1446)")


class ToolFailed(Exception):
    pass


def tool(name: str, *args: str) -> str:
    kw = conn_kwargs()
    result = subprocess.run([shutil.which(name), "-h", kw["host"], "-p", str(kw["port"]),
                             "-U", kw["user"], *args],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise ToolFailed(result.stderr)
    return result.stdout + result.stderr


def normalize(text: str) -> str:
    text = re.sub(r"^-- Dumped (from database|by pg_dump) version .*\n", "", text,
                  flags=re.MULTILINE)
    return text.replace(DB, "<db>").replace(RESTORED, "<db>")


@pytest.fixture(scope="module")
def database():
    harness.create_database(DB)
    with harness.connect(DB) as conn:
        for statement in DATA:
            conn.execute(statement)
    yield DB
    harness.drop_database(DB)
    harness.drop_database(RESTORED)


def dump(*args: str) -> str:
    return normalize(tool("pg_dump", "--restrict-key=drivers", *args))


@pytest.mark.xfail(SERENEDB, strict=True, raises=ToolFailed, reason=DUMP_OF_SERENEDB)
def test_schema_dump_matches_postgresql(database):
    check_golden(GOLDENS / "schema.sql", dump("--schema-only", database))


@pytest.mark.xfail(SERENEDB, strict=True, raises=ToolFailed, reason=DUMP_OF_SERENEDB)
def test_full_dump_matches_postgresql(database):
    check_golden(GOLDENS / "full.sql", dump(database))


@pytest.mark.xfail(SERENEDB, strict=True, raises=ToolFailed, reason=DUMP_OF_SERENEDB)
def test_custom_format_round_trip(database, tmp_path):
    archive = tmp_path / "dump.custom"
    tool("pg_dump", "-Fc", "-f", str(archive), database)
    toc = tool("pg_restore", "--list", str(archive))
    toc = re.sub(r"^;.*\n", "", toc, flags=re.MULTILINE)
    toc = re.sub(r"^\d+; \d+ \d+ ", "", toc, flags=re.MULTILINE)
    check_golden(GOLDENS / "toc.txt", normalize(toc))
    harness.create_empty_database(RESTORED)
    tool("pg_restore", "--exit-on-error", "-d", RESTORED, str(archive))
    assert dump("--schema-only", RESTORED) == dump("--schema-only", database)


def restore_errors(script: str, *flags: str) -> str:
    harness.create_empty_database(RESTORED)
    kw = conn_kwargs()
    result = subprocess.run([shutil.which("psql"), "-X", "-q", "-v", "VERBOSITY=terse", *flags,
                             "-h", kw["host"], "-p", str(kw["port"]), "-U", kw["user"],
                             "-d", RESTORED, "-f", "-"],
                            input=script, capture_output=True, text=True)
    harness.drop_database(RESTORED)
    return "".join(re.sub(r"^psql:<stdin>:", "line ", line) + "\n"
                   for line in result.stderr.splitlines() if "ERROR" in line)


@pytest.mark.xfail(SERENEDB, strict=True, raises=AssertionError, reason=RESTORE_ON_SERENEDB)
def test_restore_postgresql_dump():
    assert restore_errors((GOLDENS / "full.sql").read_text(), "--single-transaction") == ""


def test_restore_postgresql_dump_with_search_path():
    script = (GOLDENS / "full.sql").read_text().replace(
        "SELECT pg_catalog.set_config('search_path', '', false);",
        "SET search_path = public, pg_catalog;")
    check_golden(GOLDENS / "restore_with_search_path_errors.txt", restore_errors(script))
