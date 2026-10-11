from __future__ import annotations

import json
from pathlib import Path

import pytest
import tools_harness as harness
from pgcli.pgexecute import PGExecute
from pgspecial.main import PGSpecial
from spec_loader import conn_kwargs
from target import check_golden, is_serenedb

DB = harness.database_name("pgcli")
SERENEDB = is_serenedb()
GOLDENS = Path(__file__).resolve().parent / "fixtures" / "pgcli"
USER_SCHEMAS = ("public", "inventory")
BACKSLASH_COMMANDS = [
    "\\dt", "\\dt inventory.*", "\\dv", "\\dm", "\\ds", "\\di", "\\df", "\\df+ add_one",
    "\\dT", "\\dD", "\\dn", "\\dn+", "\\du postgres", f"\\l {DB}", "\\dx", "\\dp",
    "\\ddp", "\\db", "\\dF", "\\dE", "\\d", "\\d customers", "\\d+ customers",
    "\\d orders", "\\d order_lines", "\\d inventory.stock", "\\d customer_orders",
    "\\d+ customer_orders", "\\d customers_name_idx", "\\d order_no",
    "\\sf add_one", "describe items",
]


@pytest.fixture(scope="module")
def executor():
    harness.create_database(DB)
    kw = conn_kwargs()
    executor = PGExecute(database=DB, user=kw["user"], host=kw["host"], port=kw["port"])
    yield executor
    executor.conn.close()
    harness.drop_database(DB)


def user_rows(rows) -> list:
    return [list(row) for row in rows if row[0] in USER_SCHEMAS]


def function_rows(functions) -> list:
    return user_rows([f.schema_name, f.func_name, f.arg_names, f.arg_types, f.arg_modes,
                      f.return_type, f.is_aggregate, f.is_window, f.is_set_returning,
                      f.is_extension, f.arg_defaults] for f in functions)


def outcome(load):
    try:
        return load()
    except Exception as error:
        return {"error": str(error).splitlines()[0]}


def test_completion_metadata_matches_postgresql(executor):
    loaders = {
        "search_path": executor.search_path,
        "schemata": executor.schemata,
        "tables": lambda: user_rows(executor.tables()),
        "views": lambda: user_rows(executor.views()),
        "table_columns": lambda: user_rows(executor.table_columns()),
        "view_columns": lambda: user_rows(executor.view_columns()),
        "foreignkeys": lambda: sorted(list(fk) for fk in executor.foreignkeys()),
        "functions": lambda: function_rows(executor.functions()),
        "datatypes": lambda: user_rows(executor.datatypes()),
        "casing": lambda: sorted(executor.casing()),
        "view_definition": lambda: executor.view_definition("customer_orders"),
        "function_definition": lambda: executor.function_definition("add_one"),
        "timezone": executor.get_timezone,
    }
    result = {name: outcome(load) for name, load in loaders.items()}
    check_golden(GOLDENS / "metadata.json",
                 json.dumps(result, indent=1, sort_keys=True, default=str) + "\n")


def test_completion_sees_databases(executor):
    assert DB in executor.databases()
    rows, headers, _ = executor.full_databases()
    assert headers[0] == "Name" and DB in [row[0] for row in rows]
    types = {(schema, name) for schema, name in executor.datatypes()}
    assert ("public", "mood") in types


def test_completion_sees_system_functions(executor):
    functions = {(f.schema_name, f.func_name) for f in executor.functions()}
    for name in ("now", "lower", "count", "sum", "length", "generate_series",
                 "date_trunc", "string_agg", "array_agg", "current_setting"):
        assert ("pg_catalog", name) in functions, name


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB infers an untyped parameter of = ANY($1) as text, "
                          "not text[], so pgcli's relation and column queries fail")
def test_completion_sees_relations(executor):
    assert ["public", "customers"] in user_rows(executor.tables())
    assert ["public", "customer_orders"] in user_rows(executor.views())
    assert ["public", "customers", "email", "text", False, None] in user_rows(
        executor.table_columns())


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB rejects an object name bound as a text parameter "
                          "of a regclass/regproc cast")
def test_view_and_function_definitions(executor):
    assert "customer_orders" in executor.view_definition("customer_orders")
    assert "add_one" in executor.function_definition("add_one")


def render(executor, special, command: str) -> str:
    lines = [f"> {command}"]
    for title, cur, headers, status, *_ in executor.run(
            command, pgspecial=special, on_error_resume=True,
            exception_formatter=lambda error: f"ERROR: {str(error).splitlines()[0]}"):
        if title:
            lines.append(str(title))
        if headers:
            lines.append(" | ".join(headers))
        if cur:
            lines.extend(" | ".join(str(value) for value in row) for row in cur)
        if status:
            lines.append(str(status))
    return "\n".join(lines).replace(DB, "<db>") + "\n"


def test_backslash_commands_match_postgresql(executor):
    special = PGSpecial()
    check_golden(GOLDENS / "backslash.txt",
                 "\n".join(render(executor, special, command)
                           for command in BACKSLASH_COMMANDS))


def test_queries_with_special_commands_interleaved(executor):
    special = PGSpecial()
    results = list(executor.run("SELECT 1 AS one; \\dn; SELECT add_one(1)",
                                pgspecial=special))
    assert [row for row in results[0][1]] == [(1,)]
    assert any(row[0] == "inventory" for row in results[1][1])
    assert [row for row in results[2][1]] == [(2,)]


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB rejects pgcli's EXPLAIN (ANALYZE, COSTS, VERBOSE, "
                          "BUFFERS, FORMAT JSON)")
def test_explain_mode(executor):
    (result,) = executor.run("SELECT * FROM customers", explain_mode=True)
    (plan,) = list(result[1])[0]
    assert "Plan" in json.loads(plan)[0]
