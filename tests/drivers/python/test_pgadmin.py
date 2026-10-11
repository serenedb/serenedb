from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest
import tools_harness as harness
from spec_loader import conn_kwargs
from target import check_golden, is_serenedb

DB = harness.database_name("pgadmin")
SERENEDB = is_serenedb()
HERE = Path(__file__).resolve().parent
GOLDENS = HERE / "fixtures" / "pgadmin"
PGADMIN_PYTHON = Path(os.environ.get(
    "SDB_DRV_PGADMIN_PYTHON",
    Path(os.environ.get("SDB_DRIVERS_DEPS", "/opt/sdb-drivers")) / "pgadmin/bin/python"))


@pytest.fixture(scope="module")
def browser_text(tmp_path_factory):
    assert PGADMIN_PYTHON.exists(), (
        f"pgAdmin venv missing at {PGADMIN_PYTHON}: install "
        f"{HERE / 'pgadmin-requirements.txt'} into it or set SDB_DRV_PGADMIN_PYTHON")
    harness.create_database(DB)
    output = tmp_path_factory.mktemp("pgadmin") / "browser.json"
    kw = conn_kwargs()
    subprocess.run([str(PGADMIN_PYTHON), str(HERE / "pgadmin_driver.py"), kw["host"],
                    str(kw["port"]), kw["user"], DB, str(output)],
                   check=True, capture_output=True, text=True)
    yield output.read_text()
    harness.drop_database(DB)


@pytest.fixture(scope="module")
def browser(browser_text):
    return json.loads(browser_text)


def schema_objects(browser, schema: str, collection: str) -> dict:
    database = browser["database"]["database <db>"]
    return database["coll schema"][f"schema {schema}"][f"coll {collection}"]


def failures(objects: dict) -> list:
    return [(name, endpoint, entry[endpoint]["error"])
            for name, entry in objects.items()
            for endpoint in ("obj", "sql")
            if isinstance(entry.get(endpoint), dict) and "error" in entry[endpoint]]


def test_browser_tree_matches_postgresql(browser_text):
    check_golden(GOLDENS / "browser.json", browser_text)


def test_schemas_types_and_tables_are_listed(browser):
    assert sorted(schema_objects(browser, "public", "table")) == [
        "table customers", "table items", "table order_lines", "table orders"]
    assert list(schema_objects(browser, "inventory", "view")) == ["view low_stock"]
    assert failures(schema_objects(browser, "public", "type")) == []


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB rejects 'x' = ANY(ARRAY(SELECT ...)), pgAdmin reads "
                          "the connected role's privileges that way on connect")
def test_connect_reads_role_privileges(browser):
    assert "error" not in browser["connect"]


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB parses substring(text FROM pattern) as a start "
                          "position, pgAdmin reads fillfactor from reloptions that way")
def test_table_properties_and_sql(browser):
    assert failures(schema_objects(browser, "public", "table")) == []
    assert "CREATE TABLE IF NOT EXISTS public.customers" in schema_objects(
        browser, "public", "table")["table customers"]["sql"]


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB has no bpchar(x) function-style cast, pgAdmin's view "
                          "query filters pg_rewrite with it")
def test_view_properties_and_sql(browser):
    assert failures(schema_objects(browser, "public", "view")) == []


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="catalog bug: pg_proc has no xmin system column")
def test_function_properties_and_sql(browser):
    assert failures(schema_objects(browser, "public", "function")) == []


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB cannot SELECT from a sequence relation")
def test_sequence_properties_and_sql(browser):
    assert failures(schema_objects(browser, "public", "sequence")) == []


def test_query_tool_runs_queries(browser):
    queries = browser["query_tool"]
    assert queries["initialize"] is None
    assert queries["SELECT add_one(41) AS answer, current_schema() AS schema"][
        "result"] == [[42, "public"]]
    assert queries["SELEC 1"]["status"] == 500


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB's RowDescription carries no table OID and column "
                          "number, so pgAdmin cannot map result columns to customers.id "
                          "and make the result editable")
def test_query_tool_result_is_editable(browser):
    result = browser["query_tool"]["SELECT id, name, email, mood, score, active FROM customers"]
    assert result["primary_keys"] == {"id": "int4"}
    assert all(column["is_editable"] for column in result["colinfo"])
