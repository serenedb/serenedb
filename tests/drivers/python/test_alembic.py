from __future__ import annotations

import datetime
import io
import runpy
import subprocess
from pathlib import Path

import pytest
import sqlalchemy as sa
import tools_harness as harness
from alembic import command
from alembic.autogenerate import compare_metadata, produce_migrations, render_python_code
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.dialects import postgresql
from spec_loader import conn_kwargs
from target import check_golden, is_serenedb

ROOT = Path(__file__).resolve().parent / "fixtures" / "alembic"
MODELS = runpy.run_path(str(ROOT / "models.py"))
METADATA = MODELS["metadata"]
TYPED_METADATA = MODELS["typed_metadata"]
DB = harness.database_name("alembic")
SERENEDB = is_serenedb()


@pytest.fixture()
def engine():
    harness.create_empty_database(DB)
    engine = sa.create_engine(harness.url(DB, "psycopg2"))
    yield engine
    engine.dispose()
    harness.drop_database(DB)


@pytest.fixture()
def populated(engine):
    METADATA.create_all(engine)
    with engine.begin() as conn:
        conn.execute(sa.text("INSERT INTO authors (id, name, email) "
                             "VALUES (1, 'Pudge', 'pudge@dota'), (2, 'Lina', NULL)"))
        conn.execute(sa.text("INSERT INTO books (id, author_id, title, rating, published) "
                             "VALUES (1, 1, 'Fresh Meat', 4.5, '2024-01-02'), "
                             "(2, 2, 'Dragon Slave', 5, '2024-03-04')"))
    return engine


def diffs(engine, target: sa.MetaData) -> str:
    with engine.connect() as conn:
        found = compare_metadata(MigrationContext.configure(conn), target)
    return "".join(f"{diff!r}\n" for diff in found)


def run_code(engine, code: str) -> None:
    with engine.begin() as conn:
        operations = Operations(MigrationContext.configure(conn))
        body = "".join(f"    {line}\n" for line in code.splitlines())
        exec(f"if True:\n{body}",
             vars(sa) | {"op": operations, "sa": sa, "postgresql": postgresql})


def apply(engine, *calls) -> None:
    with engine.begin() as conn:
        operations = Operations(MigrationContext.configure(conn))
        for call in calls:
            call(operations)


def columns(engine, table: str) -> dict:
    return {column["name"]: column for column in sa.inspect(engine).get_columns(table)}


def alembic_config(engine, out: io.StringIO) -> Config:
    config = Config(stdout=out, output_buffer=out)
    config.set_main_option("script_location", str(ROOT))
    config.set_main_option("sqlalchemy.url",
                           engine.url.render_as_string(hide_password=False))
    return config


def test_autogenerate_from_empty_database(engine):
    with engine.connect() as conn:
        script = produce_migrations(MigrationContext.configure(conn), METADATA)
    upgrade = render_python_code(script.upgrade_ops)
    downgrade = render_python_code(script.downgrade_ops)
    check_golden(ROOT / "initial_upgrade.txt", upgrade)
    check_golden(ROOT / "initial_downgrade.txt", downgrade)
    run_code(engine, upgrade)
    assert diffs(engine, METADATA) == ""
    run_code(engine, downgrade)
    assert sa.inspect(engine).get_table_names() == []


def test_create_all_matches_metadata(engine):
    METADATA.create_all(engine)
    check_golden(ROOT / "create_all_diffs.txt", diffs(engine, METADATA))


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB drops type modifiers (#1434), autogenerate "
                          "proposes to retype varchar(n), char(n) and timestamp(p)")
def test_type_modifiers_produce_no_diffs(engine):
    TYPED_METADATA.create_all(engine)
    assert diffs(engine, TYPED_METADATA) == ""


def test_command_workflow(engine):
    out = io.StringIO()
    config = alembic_config(engine, out)
    command.upgrade(config, "head")
    command.check(config)
    command.current(config)
    assert "0003 (head)" in out.getvalue()
    with engine.connect() as conn:
        assert conn.execute(sa.text("SELECT version_num FROM alembic_version")).scalar_one() \
            == "0003"
        assert conn.execute(sa.text("SELECT count(*) FROM authors")).scalar_one() == 2
    command.history(config, verbose=False)
    assert "0002 -> 0003 (head)" in out.getvalue()
    command.downgrade(config, "0001")
    assert sorted(sa.inspect(engine).get_table_names()) == ["alembic_version", "authors"]
    command.downgrade(config, "base")
    assert sa.inspect(engine).get_table_names() == ["alembic_version"]
    command.stamp(config, "head")
    with engine.connect() as conn:
        assert conn.execute(sa.text("SELECT version_num FROM alembic_version")).scalar_one() \
            == "0003"


def test_offline_sql_applies_with_psql(engine):
    out = io.StringIO()
    config = alembic_config(engine, out)
    command.upgrade(config, "head", sql=True)
    check_golden(ROOT / "offline_upgrade.sql", out.getvalue())
    kw = conn_kwargs()
    subprocess.run(["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-h", kw["host"],
                    "-p", str(kw["port"]), "-U", kw["user"], "-d", DB],
                   input=out.getvalue(), text=True, check=True, capture_output=True)
    command.check(config)


def test_add_and_drop_columns(populated):
    apply(populated,
          lambda op: op.add_column("books", sa.Column("preface", sa.Text)),
          lambda op: op.add_column("books", sa.Column("copies", sa.Integer,
                                                      nullable=False,
                                                      server_default="1")),
          lambda op: op.add_column("authors", sa.Column("motto", sa.Text)))
    found = columns(populated, "books")
    assert found["preface"]["nullable"] and found["copies"]["default"] is not None
    assert "motto" in columns(populated, "authors")
    with populated.connect() as conn:
        assert conn.execute(sa.text("SELECT sum(copies) FROM books")).scalar_one() == 2
    apply(populated, lambda op: op.drop_column("books", "preface"))
    assert "preface" not in columns(populated, "books")


def test_alter_column_nullability_and_default(populated):
    apply(populated,
          lambda op: op.alter_column("authors", "born", nullable=True,
                                     server_default=sa.text("'2000-01-01'::date")),
          lambda op: op.alter_column("books", "rating", nullable=False))
    assert columns(populated, "authors")["born"]["default"] is not None
    assert not columns(populated, "books")["rating"]["nullable"]
    apply(populated,
          lambda op: op.alter_column("authors", "born", server_default=None),
          lambda op: op.alter_column("books", "rating", nullable=True))
    assert columns(populated, "authors")["born"]["default"] is None
    assert columns(populated, "books")["rating"]["nullable"]


def test_alter_column_type(populated):
    apply(populated, lambda op: op.alter_column("books", "pages", type_=sa.BigInteger,
                                                existing_type=sa.Integer))
    assert repr(columns(populated, "books")["pages"]["type"]) == "BIGINT()"


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB cannot change the type of a column with a "
                          "CHECK constraint")
def test_alter_type_of_checked_column(populated):
    apply(populated, lambda op: op.alter_column("books", "rating",
                                                type_=sa.Numeric(6, 2),
                                                existing_type=sa.Double))
    assert repr(columns(populated, "books")["rating"]["type"]) == \
        "NUMERIC(precision=6, scale=2)"


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB maps float without a precision to real, "
                          "PostgreSQL to double precision")
def test_float_is_double_precision(engine):
    floats = sa.MetaData()
    sa.Table("floats", floats, sa.Column("value", sa.Float))
    floats.create_all(engine)
    assert repr(columns(engine, "floats")["value"]["type"]) == \
        "DOUBLE_PRECISION(precision=53)"
    assert diffs(engine, floats) == ""


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="catalog bug: pg_get_expr renders column defaults as "
                          "DuckDB SQL, not coerced and deparsed like PostgreSQL")
def test_server_default_text(engine):
    defaults = sa.MetaData()
    sa.Table("defaults", defaults,
             sa.Column("count", sa.Integer, server_default="1"),
             sa.Column("born", sa.Date, server_default=sa.text("'2000-01-01'::date")),
             sa.Column("label", sa.Text, server_default="x"),
             sa.Column("price", sa.Numeric(6, 2), server_default=sa.text("1.5")),
             sa.Column("stamp", sa.DateTime, server_default=sa.func.now()))
    defaults.create_all(engine)
    found = {name: column["default"] for name, column in columns(engine, "defaults").items()}
    assert found == {"count": "1", "born": "'2000-01-01'::date",
                     "label": "'x'::text", "price": "1.5", "stamp": "now()"}


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB cannot drop, rename or retype a column of, or "
                          "rename, a table that a foreign key references")
@pytest.mark.parametrize("operation", [
    lambda op: op.alter_column("authors", "score", type_=sa.Numeric(12, 3),
                               existing_type=sa.Numeric(10, 2)),
    lambda op: op.alter_column("authors", "born", new_column_name="birthday"),
    lambda op: op.drop_column("authors", "born"),
    lambda op: op.rename_table("authors", "writers"),
], ids=["alter_type", "rename_column", "drop_column", "rename_table"])
def test_alter_referenced_table(populated, operation):
    apply(populated, operation)


def test_rename_column_and_table(populated):
    apply(populated,
          lambda op: op.alter_column("books", "cover", new_column_name="jacket"),
          lambda op: op.rename_table("books", "volumes"))
    assert "jacket" in columns(populated, "volumes")
    assert "books" not in sa.inspect(populated).get_table_names()


def test_indexes(populated):
    apply(populated,
          lambda op: op.create_index("books_title_idx", "books", ["title"]),
          lambda op: op.create_index("books_rating_uidx", "books", ["id", "rating"],
                                     unique=True),
          lambda op: op.create_index("books_rated_idx", "books", ["rating"],
                                     postgresql_where=sa.text("rating IS NOT NULL")))
    indexes = {index["name"]: index for index in sa.inspect(populated).get_indexes("books")}
    assert indexes["books_rating_uidx"]["unique"]
    assert indexes["books_rated_idx"]["dialect_options"]["postgresql_where"] == \
        "(rating IS NOT NULL)"
    apply(populated,
          lambda op: op.drop_index("books_title_idx", table_name="books"),
          lambda op: op.drop_index("books_rating_uidx", table_name="books"),
          lambda op: op.drop_index("books_rated_idx", table_name="books"))
    names = {index["name"] for index in sa.inspect(populated).get_indexes("books")}
    assert names == {"books_published_idx", "books_blurb_idx", "books_author_title_key"}


def test_unique_and_check_constraints(populated):
    apply(populated,
          lambda op: op.create_unique_constraint("books_title_key", "books", ["title"]),
          lambda op: op.create_check_constraint("books_title_len", "books",
                                                "length(title) < 100"))
    inspector = sa.inspect(populated)
    assert {c["name"] for c in inspector.get_unique_constraints("books")} == {
        "books_author_title_key", "books_title_key"}
    assert {c["name"] for c in inspector.get_check_constraints("books")} == {
        "books_rating_check", "books_title_len"}
    apply(populated,
          lambda op: op.drop_constraint("books_title_key", "books", type_="unique"),
          lambda op: op.drop_constraint("books_title_len", "books", type_="check"))
    inspector = sa.inspect(populated)
    assert {c["name"] for c in inspector.get_unique_constraints("books")} == {
        "books_author_title_key"}
    assert {c["name"] for c in inspector.get_check_constraints("books")} == {
        "books_rating_check"}


def test_comments(populated):
    apply(populated,
          lambda op: op.create_table_comment("books", "printed works"),
          lambda op: op.alter_column("books", "title", comment="what the cover says",
                                     existing_type=sa.Text, existing_nullable=False))
    inspector = sa.inspect(populated)
    assert inspector.get_table_comment("books")["text"] == "printed works"
    assert columns(populated, "books")["title"]["comment"] == "what the cover says"
    apply(populated, lambda op: op.drop_table_comment("books"))
    assert sa.inspect(populated).get_table_comment("books")["text"] is None


def test_execute_and_bulk_insert(populated):
    books = sa.table("books", sa.column("id", sa.BigInteger), sa.column("author_id"),
                     sa.column("title"), sa.column("published", sa.Date))
    apply(populated,
          lambda op: op.bulk_insert(books, [
              {"id": 10 + i, "author_id": 1, "title": f"Rot {i}",
               "published": datetime.date(2024, 5, i + 1)} for i in range(3)]),
          lambda op: op.execute("UPDATE books SET rating = 1 WHERE title LIKE 'Rot%'"))
    with populated.connect() as conn:
        assert conn.execute(sa.text("SELECT sum(rating) FROM books "
                                    "WHERE title LIKE 'Rot%'")).scalar_one() == 3


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB rejects UNIQUE NULLS DISTINCT, which batch mode "
                          "renders from the reflected unique constraint")
def test_batch_recreate_table(populated):
    def recreate(op):
        with op.batch_alter_table("books", recreate="always") as batch:
            batch.drop_column("cover")
            batch.alter_column("title", new_column_name="name")

    apply(populated, recreate)
    found = columns(populated, "books")
    assert "cover" not in found and "name" in found
    with populated.connect() as conn:
        assert conn.execute(sa.text("SELECT count(*) FROM books")).scalar_one() == 2


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB cannot add a FOREIGN KEY with ALTER TABLE (#1427)")
def test_create_foreign_key(populated):
    apply(populated,
          lambda op: op.add_column("books", sa.Column("editor_id", sa.Integer)),
          lambda op: op.create_foreign_key("books_editor_fk", "books", "authors",
                                           ["editor_id"], ["id"]))
    fks = {fk["name"] for fk in sa.inspect(populated).get_foreign_keys("books")}
    assert "books_editor_fk" in fks


def test_drop_foreign_key(populated):
    fk = sa.inspect(populated).get_foreign_keys("books")[0]["name"]
    apply(populated, lambda op: op.drop_constraint(fk, "books", type_="foreignkey"))
    assert sa.inspect(populated).get_foreign_keys("books") == []
