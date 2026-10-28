import re
from pathlib import Path

from sqllogic_gen import HERE, Test

CATALOG = Path(HERE).parents[1] / "server" / "pg" / "catalog"
COLUMNS = re.compile(r"SystemSqlColumn k(\w+)Columns\[\] = \{(.*?)\n\};", re.S)
COLUMN = re.compile(r'\{"(\w+)", (k\w+), (\w+), \w+, (true|false), [^,]+, \d+, SystemKey::(\w+)\}')
RELATION = re.compile(r'SystemSql k(\w+)Sql\{(?:\.oid = )?k\w+?(Table|Function),\s*'
                      r'(?:\.schema = )?"(\w+)",\s*(?:\.name = )?"(\w+)"')
TEXT = {"kName", "kText", "kChar", "kPgNodeTree", "kCharacterData", "kSqlIdentifier",
        "kYesOrNo"}
NUMBER = {"kInt2", "kInt4", "kInt8", "kOid", "kRegproc", "kXid", "kCardinalNumber"}
GLOBAL = {"pg_authid", "pg_auth_members", "pg_database", "pg_db_role_setting",
          "pg_default_acl", "pg_shdepend", "pg_foreign_server"}
LIVE = {"sdb_progress"}
STABLE = {"sdb_metrics": "metric, description, relation_id"}

FIXTURE = [
    "CREATE SCHEMA spd_s",
    "CREATE TYPE spd_s.spd_mood AS ENUM ('ok', 'bad')",
    "CREATE TYPE spd_pair AS (a INT, b TEXT)",
    "CREATE TYPE spd_code AS VARCHAR",
    "CREATE TABLE spd_parent (id INT PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    "CREATE TABLE spd_child (\n  id INT PRIMARY KEY,\n  pid INT REFERENCES spd_parent(id),\n"
    "  amt NUMERIC CHECK (amt >= 0),\n  note TEXT DEFAULT 'x',\n  mood spd_s.spd_mood,\n"
    "  pair spd_pair,\n  code spd_code)",
    "CREATE TABLE spd_s.spd_other (id INT PRIMARY KEY, mood spd_s.spd_mood)",
    "CREATE INDEX spd_idx ON spd_child (amt)",
    "CREATE INDEX spd_expr_idx ON spd_child ((amt + 1))",
    "CREATE VIEW spd_v AS SELECT id, amt FROM spd_child",
    "CREATE SEQUENCE spd_seq START 5",
    "CREATE TABLE spd_serial (id SERIAL PRIMARY KEY)",
    "CREATE TABLE spd_audit (k TEXT, v INT)",
    "CREATE TRIGGER spd_trg AFTER DELETE ON spd_child FOR EACH ROW\n"
    "  INSERT INTO spd_audit VALUES ('d', OLD.id)",
    "CREATE MACRO spd_macro(a) AS a + 1",
    "CREATE MACRO spd_rows(n) AS TABLE SELECT * FROM range(n)",
    "CREATE FUNCTION spd_fn(a INT) RETURNS INT LANGUAGE SQL AS 'SELECT a + 1'",
    "CREATE TEXT SEARCH DICTIONARY spd_dict AS keyword() WITH (frequency)",
    "CREATE TABLE spd_docs (id INT PRIMARY KEY, body TEXT) WITH (storage = 'search')",
    "CREATE INDEX spd_docs_idx ON spd_docs USING inverted (id, body spd_dict)",
    "COMMENT ON TABLE spd_parent IS 'table'",
    "COMMENT ON COLUMN spd_parent.name IS 'column'",
    "COMMENT ON VIEW spd_v IS 'view'",
    "COMMENT ON INDEX spd_idx IS 'index'",
    "COMMENT ON SEQUENCE spd_seq IS 'sequence'",
    "COMMENT ON TYPE spd_pair IS 'type'",
    "COMMENT ON MACRO spd_macro IS 'macro'",
    "INSERT INTO spd_parent VALUES (1, 'a')",
    "INSERT INTO spd_child VALUES (1, 1, 5, 'n', 'ok', (1, 'x'), 'c')",
    "PREPARE spd_prep AS SELECT 1",
    "CREATE SUBSCRIPTION spd_sub CONNECTION 'host=localhost dbname=spd' PUBLICATION spd_pub "
    "WITH (connect = false, slot_name = NONE)",
]

TEARDOWN = [
    "DROP SUBSCRIPTION spd_sub",
    "DEALLOCATE spd_prep",
]

GLOBAL_FIXTURE = [
    "CREATE ROLE spd_role",
    "CREATE ROLE spd_member",
    "GRANT spd_role TO spd_member",
    "GRANT SELECT ON spd_parent TO spd_role",
    "ALTER ROLE spd_role SET statement_timeout = 5000",
    "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO spd_role",
    "CREATE SERVER spd_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (path ':memory:')",
]

GLOBAL_TEARDOWN = [
    "DROP SERVER spd_srv",
    "ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM spd_role",
    "REVOKE SELECT ON spd_parent FROM spd_role",
    "DROP ROLE spd_member",
    "DROP ROLE spd_role",
]

OIDS = ["'spd_parent'::regclass", "'spd_child'::regclass", "'spd_s.spd_other'::regclass",
        "'spd_v'::regclass", "'spd_idx'::regclass", "'spd_seq'::regclass",
        "'spd_docs'::regclass", "'spd_docs_idx'::regclass", "'spd_pair'::regtype",
        "'_spd_pair'::regtype", "'spd_s.spd_mood'::regtype", "'spd_code'::regtype",
        "'spd_s'::regnamespace", "'spd_macro'::regproc", "'spd_rows'::regproc",
        "'spd_fn'::regproc", "'spd_dict'::regdictionary"]
NAMES = ["spd_parent", "spd_child", "spd_other", "spd_v", "spd_idx", "spd_seq", "spd_docs",
         "spd_pair", "_spd_pair", "spd_mood", "_spd_mood", "spd_code", "spd_s", "spd_macro",
         "spd_rows", "spd_fn", "spd_dict", "spd_trg", "spd_parent_pkey", "spd_sub"]
GLOBAL_OIDS = ["'spd_role'::regrole", "'spd_member'::regrole"]
GLOBAL_NAMES = ["spd_role", "spd_member", "spd_srv"]


def relations():
    texts = [(CATALOG / "generated" / "tables.gen.inc").read_text()]
    texts += [path.read_text() for path in sorted((CATALOG / "tables").rglob("*.cpp"))]
    columns, names = {}, {}
    for text in texts:
        for name, body in COLUMNS.findall(text):
            columns[name] = COLUMN.findall(body)
        for name, kind, schema, relation in RELATION.findall(text):
            names[name] = (schema, relation, kind == "Function")
    registry = (CATALOG / "generated" / "registry.gen.inc").read_text()
    for name in re.findall(r"&g(\w+),", registry):
        schema, relation, function = names[name]
        if relation not in LIVE:
            yield relation, f'{schema}."{relation}"' + ("()" if function else ""), columns[name]


def predicates(column, kind, element, not_null, key, oids, names):
    name = f'"{column}"'
    if element != "0":
        return []
    keyed = key != "None"
    if kind in TEXT:
        found = [f"{name} <> 'spd'"] + [f"{name} = '{n}'" for n in names if keyed]
    elif kind in NUMBER:
        found = [f"{name} <> 12345"] + [f"{name} = {oid}" for oid in oids
                                         if keyed and kind == "kOid"]
    elif kind == "kBool":
        found = [f"{name} IN (true, false)"]
    else:
        return []
    return found + ([f"{name} IS NULL"] if not_null == "false" else [])


def check(t, name, source, columns, oids, names):
    t.section(source)
    shown = STABLE.get(name, "*")
    for column in columns:
        for predicate in predicates(*column, oids, names):
            pushed = f"SELECT {shown} FROM {source} WHERE {predicate}"
            scanned = (f"SELECT {shown} FROM (SELECT * FROM {source} LIMIT 1000000000) "
                       f"WHERE {predicate}")
            t.query(f"SELECT count(*) AS mismatches FROM (({pushed} EXCEPT ALL {scanned}) "
                    f"UNION ALL ({scanned} EXCEPT ALL {pushed}))", ["mismatches"], [[0]])


t = Test("A filter a system relation evaluates while it scans returns the rows the same "
         "filter keeps from the unfiltered scan, for every column a filter can be pushed "
         "into, against a fixture with an object of every kind, and every key column looks "
         "up each of those objects by its oid or name. sdb_progress shows operations as "
         "they run and sdb_metrics counts them, so the first is left out and the second "
         "compared on the columns that stay put while a statement runs.")
for sql in FIXTURE:
    t.statement(sql)
local = [r for r in relations() if r[0] not in GLOBAL]
for chunk in t.parts(local, 4):
    for name, source, columns in chunk:
        check(t, name, source, columns, OIDS, NAMES)
    for sql in TEARDOWN:
        t.statement(sql)
t.part()
for sql in GLOBAL_FIXTURE:
    t.statement(sql)
for name, source, columns in relations():
    if name in GLOBAL:
        check(t, name, source, columns, OIDS + GLOBAL_OIDS, NAMES + GLOBAL_NAMES)
for sql in GLOBAL_TEARDOWN + TEARDOWN:
    t.statement(sql)
t.write()
