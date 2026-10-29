import sys
from pathlib import Path

from sqllogic_gen import HERE, Test

sys.path.insert(0, str(Path(HERE).parents[1] / "scripts"))
from pg_catalog import claims  # noqa: E402

# Green relations whose content cannot be produced by a self-contained fixture.
EXEMPT = {
    # A subscription lists its tables only once it has reached its publisher.
    "pg_subscription_rel",
}

FIXTURE = [
    "DROP SERVER IF EXISTS stc_srv",
    "DROP SUBSCRIPTION IF EXISTS stc_sub",
    "DROP TEXT SEARCH DICTIONARY IF EXISTS stc_dict",
    "DROP SCHEMA IF EXISTS stc_s CASCADE",
    "DROP VIEW IF EXISTS stc_v",
    "DROP TABLE IF EXISTS stc_child",
    "DROP TABLE IF EXISTS stc_parent",
    "DROP TABLE IF EXISTS stc_audit",
    "DROP TYPE IF EXISTS stc_mood",
    "DROP SEQUENCE IF EXISTS stc_seq",
    "DROP ROLE IF EXISTS stc_member",
    "DROP ROLE IF EXISTS stc_role",
    "CREATE SCHEMA stc_s",
    "CREATE TYPE stc_mood AS ENUM ('ok', 'bad')",
    "CREATE TABLE stc_parent (id INT PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    "CREATE TABLE stc_child (\n  id INT PRIMARY KEY,\n  pid INT REFERENCES stc_parent(id),\n"
    "  amt NUMERIC CHECK (amt >= 0),\n  note TEXT DEFAULT 'x',\n  mood stc_mood)",
    "CREATE INDEX stc_idx ON stc_child (amt)",
    "CREATE VIEW stc_v AS SELECT id, amt FROM stc_child",
    "CREATE SEQUENCE stc_seq START 5",
    "CREATE TABLE stc_audit (k TEXT, v INT)",
    "CREATE TRIGGER stc_trg AFTER DELETE ON stc_child FOR EACH ROW\n  INSERT INTO stc_audit VALUES ('d', OLD.id)",
    "CREATE MACRO stc_macro(a) AS a + 1",
    "COMMENT ON TABLE stc_parent IS 'fixture parent'",
    "COMMENT ON COLUMN stc_parent.name IS 'fixture column'",
    "CREATE ROLE stc_role",
    "CREATE ROLE stc_member",
    "GRANT stc_role TO stc_member",
    "GRANT SELECT ON stc_parent TO stc_role",
    "ALTER ROLE stc_role SET statement_timeout = 5000",
    "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO stc_role",
    "CREATE TEXT SEARCH DICTIONARY stc_dict AS keyword() WITH (frequency)",
    "PREPARE stc_prep AS SELECT 1",
    "INSERT INTO stc_parent VALUES (1, 'a')",
    "INSERT INTO stc_child VALUES (1, 1, 5, 'n', 'ok')",
    "CREATE SUBSCRIPTION stc_sub CONNECTION 'host=localhost dbname=stc' PUBLICATION stc_pub "
    "WITH (connect = false, slot_name = NONE)",
    "CREATE SERVER stc_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (path ':memory:')",
]

TEARDOWN = [
    "DROP SERVER IF EXISTS stc_srv",
    "DROP SUBSCRIPTION IF EXISTS stc_sub",
    "DEALLOCATE stc_prep",
    "DROP TEXT SEARCH DICTIONARY IF EXISTS stc_dict",
    "ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM stc_role",
    "DROP SCHEMA IF EXISTS stc_s CASCADE",
    "DROP VIEW IF EXISTS stc_v",
    "DROP TABLE IF EXISTS stc_child",
    "DROP TABLE IF EXISTS stc_parent",
    "DROP TABLE IF EXISTS stc_audit",
    "DROP TYPE IF EXISTS stc_mood",
    "DROP SEQUENCE IF EXISTS stc_seq",
    "DROP ROLE IF EXISTS stc_member",
    "DROP ROLE IF EXISTS stc_role",
]

t = Test("Claims made by docs/compatibility/system-table-compatibility.md. A relation "
         "documented as supported returns rows against the fixture below; one documented "
         "as a compatibility stub only has to be queryable, since another file's DDL may "
         "legitimately put rows in a stub.")
for sql in FIXTURE:
    t.statement(sql)
for _, schema, name, mark in claims.parse(claims.DOCS):
    t.statement(f'SELECT * FROM {schema}."{name}" LIMIT 0')
    if mark == "🟢" and name not in EXEMPT:
        t.query(f'SELECT count(*) > 0 AS populated FROM {schema}."{name}"', ["populated"], [["t"]])
for sql in TEARDOWN:
    t.statement(sql)
t.write()
