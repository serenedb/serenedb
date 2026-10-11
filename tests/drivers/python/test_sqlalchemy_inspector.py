from __future__ import annotations

import datetime
import decimal
import json
import uuid
from pathlib import Path

import pytest
import tools_harness as harness
from sqlalchemy import MetaData, Table, create_engine, inspect, select, text
from sqlalchemy.engine.reflection import ObjectKind
from sqlalchemy.schema import CreateIndex, CreateTable
from target import check_golden, is_serenedb

DB = harness.database_name("sqla")
SERENEDB = is_serenedb()
GOLDENS = Path(__file__).resolve().parent / "fixtures" / "sqlalchemy"
SCHEMAS = [None, "inventory"]


@pytest.fixture(scope="module")
def engine():
    harness.create_database(DB)
    engine = create_engine(harness.url(DB, "psycopg2"))
    yield engine
    engine.dispose()
    harness.drop_database(DB)


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB has no SAVEPOINT (#722), SQLAlchemy's psycopg "
                          "dialect fetches the hstore type inside one on connect")
def test_psycopg_dialect_connects(engine):
    psycopg_engine = create_engine(harness.url(DB, "psycopg"))
    try:
        assert "customers" in inspect(psycopg_engine).get_table_names()
    finally:
        psycopg_engine.dispose()


def test_asyncpg_dialect_connects(engine):
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    async def table_names():
        async_engine = create_async_engine(harness.url(DB, "asyncpg"))
        try:
            async with async_engine.connect() as conn:
                return await conn.run_sync(
                    lambda sync_conn: inspect(sync_conn).get_table_names())
        finally:
            await async_engine.dispose()

    assert "customers" in asyncio.run(table_names())


def dump(value) -> str:
    return json.dumps(value, indent=1, sort_keys=True, default=repr) + "\n"


def keyed(multi: dict) -> dict:
    return {f"{schema}.{name}": value for (schema, name), value in multi.items()}


def describe(inspector, name: str, schema: str | None) -> dict:
    return {
        "columns": inspector.get_columns(name, schema),
        "primary_key": inspector.get_pk_constraint(name, schema),
        "foreign_keys": inspector.get_foreign_keys(name, schema),
        "indexes": inspector.get_indexes(name, schema),
        "unique_constraints": inspector.get_unique_constraints(name, schema),
        "check_constraints": inspector.get_check_constraints(name, schema),
        "comment": inspector.get_table_comment(name, schema),
        "options": inspector.get_table_options(name, schema),
    }


def test_inspector_matches_postgresql(engine):
    inspector = inspect(engine)
    result = {
        "schemas": inspector.get_schema_names(),
        "default_schema": inspector.default_schema_name,
    }
    for schema in SCHEMAS:
        tables = sorted(inspector.get_table_names(schema))
        views = sorted(inspector.get_view_names(schema))
        result[schema or "public"] = {
            "tables": tables,
            "views": views,
            "materialized_views": sorted(inspector.get_materialized_view_names(schema)),
            "sequences": sorted(inspector.get_sequence_names(schema)),
            "foreign_tables": sorted(inspector.get_foreign_table_names(schema)),
            "enums": inspector.get_enums(schema),
            "domains": inspector.get_domains(schema),
            "sorted_tables": [[table, sorted(constraints)] for table, constraints
                              in inspector.get_sorted_table_and_fkc_names(schema)],
            "view_definitions": {view: inspector.get_view_definition(view, schema)
                                 for view in views},
            "relations": {name: describe(inspector, name, schema)
                          for name in tables + views},
        }
    check_golden(GOLDENS / "inspector.json", dump(result))


def test_multi_reflection_matches_single_table_reflection(engine):
    inspector = inspect(engine)
    single = {
        "columns": inspector.get_columns,
        "primary_key": inspector.get_pk_constraint,
        "foreign_keys": inspector.get_foreign_keys,
        "indexes": inspector.get_indexes,
        "unique_constraints": inspector.get_unique_constraints,
        "check_constraints": inspector.get_check_constraints,
        "comment": inspector.get_table_comment,
    }
    multi = {
        "columns": inspector.get_multi_columns,
        "primary_key": inspector.get_multi_pk_constraint,
        "foreign_keys": inspector.get_multi_foreign_keys,
        "indexes": inspector.get_multi_indexes,
        "unique_constraints": inspector.get_multi_unique_constraints,
        "check_constraints": inspector.get_multi_check_constraints,
        "comment": inspector.get_multi_table_comment,
    }
    for schema in SCHEMAS:
        names = inspector.get_table_names(schema) + inspector.get_view_names(schema)
        for key, method in multi.items():
            batch = method(schema=schema, kind=ObjectKind.ANY)
            assert sorted(name for _, name in batch) == sorted(names)
            for name in names:
                assert dump(batch[(schema, name)]) == dump(single[key](name, schema))


def test_existence_checks(engine):
    inspector = inspect(engine)
    assert inspector.has_schema("inventory") and not inspector.has_schema("nope")
    assert inspector.has_table("customers") and inspector.has_table("customer_orders")
    assert inspector.has_table("stock", schema="inventory")
    assert not inspector.has_table("stock")
    assert inspector.has_index("orders", "orders_partial_idx")
    assert not inspector.has_index("orders", "customers_name_idx")
    assert inspector.has_sequence("order_no")
    assert inspector.has_type("mood") and not inspector.has_type("nope")
    assert inspector.get_table_oid("customers") > 0
    assert inspector.get_table_oid("stock", schema="inventory") > 0
    assert inspector.get_table_comment("customers")["text"] == "people who buy"


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="catalog bug: temporary tables and views are missing "
                          "from pg_class and pg_namespace")
def test_temporary_tables(engine):
    with engine.connect() as conn:
        conn.execute(text("CREATE TEMP TABLE scratch (id integer PRIMARY KEY, note text)"))
        conn.execute(text("CREATE TEMP VIEW scratch_view AS SELECT id FROM scratch"))
        inspector = inspect(conn)
        assert inspector.get_temp_table_names() == ["scratch"]
        assert inspector.get_temp_view_names() == ["scratch_view"]
        assert [c["name"] for c in inspector.get_columns("scratch")] == ["id", "note"]
        conn.rollback()


def test_reflected_metadata_renders_postgresql_ddl(engine):
    metadata = MetaData()
    for schema in SCHEMAS:
        metadata.reflect(engine, schema=schema, views=True)
    ddl = []
    for table in metadata.sorted_tables:
        ddl.append(str(CreateTable(table).compile(engine)).strip() + ";")
        for index in sorted(table.indexes, key=lambda index: index.name):
            ddl.append(str(CreateIndex(index).compile(engine)) + ";")
    check_golden(GOLDENS / "reflected.sql", "\n\n".join(ddl) + "\n")


def test_autoload_follows_foreign_keys(engine):
    metadata = MetaData()
    lines = Table("order_lines", metadata, autoload_with=engine)
    assert sorted(metadata.tables) == ["customers", "items", "order_lines", "orders"]
    assert [c.name for c in lines.primary_key] == ["order_id", "line"]
    assert lines.c.total.computed is not None
    assert lines.c.qty.server_default is not None
    orders = metadata.tables["orders"]
    assert {fk.target_fullname for fk in orders.foreign_keys} == {"customers.id",
                                                                  "items.sku"}
    stock = Table("stock", MetaData(), schema="inventory", autoload_with=engine)
    assert stock.c.amount.server_default.arg.text == "0"


def test_reflected_tables_round_trip_rows(engine):
    metadata = MetaData()
    metadata.reflect(engine)
    customers, items = metadata.tables["customers"], metadata.tables["items"]
    orders, lines = metadata.tables["orders"], metadata.tables["order_lines"]
    with engine.begin() as conn:
        customer = conn.execute(customers.insert().returning(customers.c.id), {
            "name": "Pudge", "email": "pudge@dota", "mood": "happy",
            "born": datetime.date(2004, 2, 1)}).scalar_one()
        conn.execute(items.insert(), {
            "sku": "HOOK0001", "title": "Meat Hook", "qty": 3, "price": 9.5,
            "tags": ["hook", "meat"], "meta": {"range": 1300}, "weight": 1.5,
            "added": datetime.datetime(2024, 1, 2, 3, 4, 5),
            "ready": datetime.time(6, 7, 8),
            "shelf_life": datetime.timedelta(days=3, hours=2),
            "uid": uuid.UUID(int=7), "blob": b"\x00\xff", "small": 7, "big": 2**40})
        order = conn.execute(orders.insert().returning(orders.c.id), {
            "customer_id": customer, "sku": "HOOK0001",
            "placed": datetime.date(2024, 5, 6), "note": "rush"}).scalar_one()
        conn.execute(lines.insert(), {"order_id": order, "line": 1, "qty": 4})
        conn.execute(lines.insert(), {"order_id": order, "line": 2})
    with engine.connect() as conn:
        row = conn.execute(select(customers).where(customers.c.id == customer)).one()
        assert row.mood == "happy" and row.score == decimal.Decimal("0.00")
        assert row.active is True and row.created is not None
        item = conn.execute(select(items)).one()
        assert item.tags == ["hook", "meat"] and item.meta == {"range": 1300}
        assert item.shelf_life == datetime.timedelta(days=3, hours=2)
        assert item.uid == uuid.UUID(int=7) and bytes(item.blob) == b"\x00\xff"
        totals = conn.execute(select(lines.c.line, lines.c.total)
                              .order_by(lines.c.line)).all()
        assert totals == [(1, decimal.Decimal("10.00")), (2, decimal.Decimal("2.50"))]
        assert conn.execute(text("SELECT n FROM customer_orders")).scalar_one() == 1
        assert conn.execute(text("SELECT add_one(41)")).scalar_one() == 42
    for table in (lines, orders, items, customers):
        with engine.begin() as conn:
            conn.execute(table.delete())


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB rejects deleting a parent row after its "
                          "children in one transaction (#1433)")
def test_delete_children_then_parent_in_one_transaction(engine):
    metadata = MetaData()
    metadata.reflect(engine)
    customers, orders = metadata.tables["customers"], metadata.tables["orders"]
    with engine.begin() as conn:
        customer = conn.execute(customers.insert().returning(customers.c.id),
                                {"name": "Lion"}).scalar_one()
        conn.execute(orders.insert(), {"customer_id": customer,
                                       "placed": datetime.date(2024, 1, 1)})
    with engine.begin() as conn:
        conn.execute(orders.delete().where(orders.c.customer_id == customer))
        conn.execute(customers.delete().where(customers.c.id == customer))
    with engine.connect() as conn:
        assert conn.execute(select(customers).where(customers.c.id == customer)).first() is None


def reflect_new_table(engine, ddl: list[str], name: str, schema: str | None = None):
    with engine.begin() as conn:
        for statement in ddl:
            conn.execute(text(statement))
    return describe(inspect(engine), name, schema)


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB has no identity columns (#1426)")
def test_identity_columns(engine):
    table = reflect_new_table(engine, [
        "CREATE TABLE ident (id bigint GENERATED BY DEFAULT AS IDENTITY "
        "(START WITH 10 INCREMENT BY 5) PRIMARY KEY, "
        "seq integer GENERATED ALWAYS AS IDENTITY)"], "ident")
    identity = {c["name"]: c.get("identity") for c in table["columns"]}
    assert identity["id"] == {"always": False, "start": 10, "increment": 5,
                              "minvalue": 1, "maxvalue": 9223372036854775807,
                              "cycle": False, "cache": 1}
    assert identity["seq"]["always"] is True


@pytest.mark.xfail(SERENEDB, strict=True, reason="SereneDB has no CREATE DOMAIN")
def test_domains(engine):
    reflect_new_table(engine, [
        "CREATE DOMAIN positive_int AS integer NOT NULL CHECK (VALUE > 0)",
        "CREATE TABLE domained (id positive_int)"], "domained")
    (domain,) = inspect(engine).get_domains()
    assert domain["name"] == "positive_int" and domain["type"] == "integer"
    assert domain["nullable"] is False
    assert domain["constraints"] == [{"name": "positive_int_check",
                                      "check": "VALUE > 0"}]


@pytest.mark.xfail(SERENEDB, strict=True, reason="SereneDB has no materialized views")
def test_materialized_views(engine):
    reflect_new_table(engine, [
        "CREATE MATERIALIZED VIEW order_counts AS "
        "SELECT customer_id, count(*) AS n FROM orders GROUP BY customer_id"],
        "order_counts")
    inspector = inspect(engine)
    assert inspector.get_materialized_view_names() == ["order_counts"]
    assert "count(*)" in inspector.get_view_definition("order_counts")


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB foreign keys have no ON DELETE/ON UPDATE actions")
def test_foreign_key_actions(engine):
    table = reflect_new_table(engine, [
        "CREATE TABLE fk_actions (customer_id integer REFERENCES customers (id) "
        "ON DELETE CASCADE ON UPDATE SET NULL)"], "fk_actions")
    (fk,) = table["foreign_keys"]
    assert fk["options"] == {"ondelete": "CASCADE", "onupdate": "SET NULL"}


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB cannot reference a table in another schema")
def test_cross_schema_foreign_key(engine):
    table = reflect_new_table(engine, [
        "CREATE TABLE inventory.reservations (customer_id integer "
        "REFERENCES public.customers (id))"], "reservations", "inventory")
    (fk,) = table["foreign_keys"]
    assert fk["referred_schema"] is None and fk["referred_table"] == "customers"


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB rejects USING btree/hash/gin, NULLS FIRST, "
                          "NULLS NOT DISTINCT, index COLLATE and WITH (fillfactor)")
def test_index_methods_and_options(engine):
    table = reflect_new_table(engine, [
        "CREATE TABLE indexed (a text, b integer[], c text, d text, e date)",
        "CREATE INDEX indexed_a_btree ON indexed USING btree (a)",
        "CREATE INDEX indexed_a_hash ON indexed USING hash (a)",
        "CREATE INDEX indexed_b_gin ON indexed USING gin (b)",
        "CREATE INDEX indexed_c_nulls ON indexed (c NULLS FIRST)",
        "CREATE UNIQUE INDEX indexed_d_nnd ON indexed (d) NULLS NOT DISTINCT",
        "CREATE INDEX indexed_c_collate ON indexed (c COLLATE \"C\")",
        "CREATE INDEX indexed_e_fill ON indexed (e) WITH (fillfactor = 70)"],
        "indexed")
    indexes = {index["name"]: index for index in table["indexes"]}
    assert indexes["indexed_a_hash"]["dialect_options"]["postgresql_using"] == "hash"
    assert indexes["indexed_b_gin"]["dialect_options"]["postgresql_using"] == "gin"
    assert indexes["indexed_c_nulls"]["column_sorting"] == {"c": ("nulls_first",)}
    assert indexes["indexed_d_nnd"]["dialect_options"][
        "postgresql_nulls_not_distinct"] is True
    assert indexes["indexed_e_fill"]["dialect_options"]["postgresql_with"] == {
        "fillfactor": "70"}


@pytest.mark.xfail(SERENEDB, strict=True, reason="SereneDB has no partitioned tables")
def test_partitioned_tables(engine):
    reflect_new_table(engine, [
        "CREATE TABLE measurements (taken date NOT NULL, value integer) "
        "PARTITION BY RANGE (taken)",
        "CREATE TABLE measurements_2024 PARTITION OF measurements "
        "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"], "measurements")
    names = inspect(engine).get_table_names()
    assert "measurements" in names and "measurements_2024" in names
