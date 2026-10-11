from __future__ import annotations

import datetime
import decimal
import json
import uuid
from pathlib import Path

import pyodbc
import pytest
import tools_harness as harness
from spec_loader import conn_kwargs
from target import check_golden, is_serenedb

DB = harness.database_name("odbc")
SERENEDB = is_serenedb()
GOLDENS = Path(__file__).resolve().parent / "fixtures" / "odbc"
OID_COLUMNS = {"table oid", "field_type", "base typeid"}
FIRST_NORMAL_OID = 16384
pyodbc.pooling = False
TABLES = [("public", "customers"), ("public", "items"), ("public", "orders"),
          ("public", "order_lines"), ("inventory", "stock")]


def odbc_connect(options: str) -> pyodbc.Connection:
    kw = conn_kwargs()
    return pyodbc.connect(f"DRIVER={{PostgreSQL Unicode}};SERVER={kw['host']};"
                          f"PORT={kw['port']};DATABASE={DB};UID={kw['user']};{options}",
                          autocommit=True)


@pytest.fixture(scope="module")
def database():
    harness.create_database(DB)
    yield
    harness.drop_database(DB)


@pytest.fixture(scope="module")
def connection(database):
    conn = odbc_connect("")
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def client_side_connection(database):
    conn = odbc_connect("UseServerSidePrepare=0;")
    yield conn
    conn.close()


def value_text(column: str, value) -> str | None:
    if value is None:
        return None
    if column in OID_COLUMNS and int(value) >= FIRST_NORMAL_OID:
        return "<oid>"
    return str(value).replace(DB, "<db>")


def describe(result) -> dict:
    columns = [column[0] for column in result.description]
    return {"columns": columns,
            "rows": [[value_text(column, value) for column, value in zip(columns, row)]
                     for row in result]}


def outcome(load):
    try:
        return load()
    except pyodbc.Error as error:
        return {"error": str(error.args[-1]).splitlines()[0]}


def test_catalog_functions_match_postgresql(connection):
    cursor = connection.cursor()
    result = {
        "tables": outcome(lambda: describe(cursor.tables(schema="public"))),
        "views": outcome(lambda: describe(cursor.tables(schema="%", tableType="VIEW"))),
        "inventory": outcome(lambda: describe(cursor.tables(schema="inventory"))),
        "table_types": outcome(lambda: describe(cursor.tables(tableType="%"))),
        "procedures": outcome(lambda: describe(cursor.procedures(schema="public"))),
        "procedure_columns": outcome(
            lambda: describe(cursor.procedureColumns(procedure="add_one",
                                                     schema="public"))),
    }
    for schema, table in TABLES:
        result[f"{schema}.{table}"] = {
            "columns": outcome(lambda: describe(cursor.columns(table=table,
                                                               schema=schema))),
            "statistics": outcome(lambda: describe(cursor.statistics(table,
                                                                     schema=schema))),
            "primary_keys": outcome(lambda: describe(cursor.primaryKeys(table,
                                                                        schema=schema))),
            "imported_keys": outcome(lambda: describe(cursor.foreignKeys(
                foreignTable=table, foreignSchema=schema))),
            "exported_keys": outcome(lambda: describe(cursor.foreignKeys(
                table=table, schema=schema))),
            "row_id_columns": outcome(lambda: describe(cursor.rowIdColumns(
                table, schema=schema))),
            "row_version_columns": outcome(lambda: describe(cursor.rowVerColumns(
                table, schema=schema))),
        }
    check_golden(GOLDENS / "catalog.json",
                 json.dumps(result, indent=1, sort_keys=True) + "\n")


def test_type_info_matches_postgresql(connection):
    check_golden(GOLDENS / "type_info.json",
                 json.dumps(describe(connection.cursor().getTypeInfo()), indent=1) + "\n")


def test_driver_metadata(connection):
    assert connection.getinfo(pyodbc.SQL_DATABASE_NAME) == DB
    assert connection.getinfo(pyodbc.SQL_DBMS_NAME) == "PostgreSQL"
    assert connection.getinfo(pyodbc.SQL_DBMS_VER).startswith("18.")
    assert connection.getinfo(pyodbc.SQL_IDENTIFIER_QUOTE_CHAR) == '"'
    assert connection.getinfo(pyodbc.SQL_USER_NAME) == conn_kwargs()["user"]


@pytest.mark.parametrize("fixture", [
    pytest.param("connection", marks=pytest.mark.xfail(
        SERENEDB, strict=True,
        reason="SereneDB's DEALLOCATE does not see statements prepared with the "
               "protocol's Parse message, psqlODBC drops its _PLAN statements that "
               "way before reusing a statement handle")),
    "client_side_connection",
])
def test_round_trip_through_odbc(request, fixture):
    with harness.connect(DB) as conn:
        for table in ("orders", "items", "customers"):
            conn.execute(f"DELETE FROM {table}")
    cursor = request.getfixturevalue(fixture).cursor()
    cursor.execute("INSERT INTO customers (name, email, mood, born, score) "
                   "VALUES (?, ?, ?, ?, ?)",
                   "Pudge", "pudge@dota", "happy", datetime.date(2004, 2, 1),
                   decimal.Decimal("12.50"))
    cursor.execute("INSERT INTO items (sku, title, qty, price, tags, meta, weight, "
                   "added, ready, uid, blob, small, big) "
                   "VALUES (?, ?, ?, ?, '{hook,meat}', '{\"range\": 1300}', ?, ?, ?, "
                   "?, ?, ?, ?)",
                   "HOOK0001", "Meat Hook", 3, 9.5, 1.5,
                   datetime.datetime(2024, 1, 2, 3, 4, 5), datetime.time(6, 7, 8),
                   str(uuid.UUID(int=7)), b"\x00\xff", 7, 2**40)
    customer = cursor.execute("SELECT id, name, email, mood, born, score, active "
                              "FROM customers").fetchone()
    assert list(customer)[1:] == ["Pudge", "pudge@dota", "happy",
                                  datetime.date(2004, 2, 1), decimal.Decimal("12.50"),
                                  "1"]
    item = cursor.execute("SELECT title, qty, price, tags, weight, added, ready, uid, "
                          "blob, small, big FROM items").fetchone()
    assert item.title == "Meat Hook" and item.qty == 3 and item.price == 9.5
    assert item.added == datetime.datetime(2024, 1, 2, 3, 4, 5)
    assert item.ready == datetime.time(6, 7, 8)
    assert str(item.uid).lower() == str(uuid.UUID(int=7))
    assert bytes(item.blob) == b"\x00\xff" and item.small == 7 and item.big == 2**40
    cursor.execute("INSERT INTO orders (customer_id, sku, placed) VALUES (?, ?, ?)",
                   customer.id, "HOOK0001", datetime.date(2024, 5, 6))
    assert cursor.execute("SELECT n FROM customer_orders").fetchval() == 1
    assert cursor.execute("SELECT add_one(?)", 41).fetchval() == 42
    cursor.execute("DELETE FROM orders")
    cursor.execute("DELETE FROM items")
    cursor.execute("DELETE FROM customers")


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="catalog bug: OPERATOR(pg_catalog.=) between a boolean and "
                          "an untyped literal hits a DuckDB assertion, psqlODBC's "
                          "SQLPrimaryKeys and SQLSpecialColumns compare indisprimary "
                          "that way")
def test_primary_key_and_row_id_columns(connection):
    cursor = connection.cursor()
    keys = cursor.primaryKeys("order_lines", schema="public").fetchall()
    assert [(key.column_name, key.key_seq, key.pk_name) for key in keys] == [
        ("order_id", 1, "order_lines_pkey"), ("line", 2, "order_lines_pkey")]
    assert [column.column_name for column in
            cursor.rowIdColumns("customers", schema="public").fetchall()] == ["id"]


def test_prepared_statement_metadata(database):
    conn = odbc_connect("")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, score, created FROM customers WHERE id = ?", 0)
        description = [[str(value) for value in column] for column in cursor.description]
    finally:
        conn.close()
    check_golden(GOLDENS / "description.json", json.dumps(description, indent=1) + "\n")
