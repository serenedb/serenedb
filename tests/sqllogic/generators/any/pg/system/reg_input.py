from sqllogic_gen import Test, lit

TYPES = [
    "setof int4", "int4 int4", "int4)", "int4(", "numeric(x)", "numeric(99999999999)",
    "pg_catalog.(", "pg_catalog.", ".int4", "int4.",
    "float", "float(0)", "float(-1)", "float(1)", "float(24)", "float(25)", "float(53)",
    "float(54)", "float(x)", "float(3",
    "double precision", "double", "dec(5,2)", "decimal", "decimal(7)", "numeric",
    "national character(3)", "national char varying(4)", "national character varying",
    "national", "national varchar",
    "nchar", "nchar(2)", "char", "char varying(5)", "character varying", "character(0)",
    "char(10485761)", "varchar", "varchar(0)", "varchar(10485760)", "varchar(10485761)",
    "varchar(1,2)",
    "bit", "bit(3)", "bit varying", "bit varying(5)", "bit(0)", "varbit(0)", "varbit(4)",
    "bit(83886081)",
    "time", "time(3)", "time(3) with time zone", "time without time zone", "time(7)",
    "time(-1)", "time(1,2)", "time with zone", "time without", "time with", "timetz(2)",
    "timestamp(2) with time zone", "timestamp(6) without time zone", "timestamptz(3)",
    "timestamp(-1) with time zone", "timestamp(1,2)",
    "interval", "interval(3)", "interval(-1)", "interval(7)", "interval(3,4)",
    "interval year", "interval month", "interval day", "interval hour", "interval minute",
    "interval second", "interval second(3)", "interval second(-1)", "interval year to month",
    "interval day to hour", "interval day to minute", "interval day to second",
    "interval day to second(2)", "interval hour to minute", "interval hour to second",
    "interval minute to second", "interval minute to second(1)", "interval month to year",
    "interval year to day", "interval minute to hour", "interval second to second",
    "interval day to", "interval day to foo", "interval foo", "interval 5",
    "interval year(2)",
    "numeric(5,2,1)", "numeric(0)", "numeric(1001)", "numeric(1000)", "numeric(5,-1001)",
    "numeric(5,1001)", "numeric(5,-2)", "numeric(5, -2)", "numeric(5,-)", "numeric(-5)",
    "int4(3)", "text(5)", "integer(3)",
    "int4 array", "int4 array[5]", "int4 array[x]", "int4 array[", "int4[]", "int4[3]",
    "int4[3][]", "int4[3][4]", "int4[", "int4[x]", "int4[3", "int4 []",
    '"int4"', '"Int4"', '"unterminated', '""', 'pg_catalog."int4"', "a$b", "тип", "_int4",
    "int4$",
    "pg_catalog.int4", "public.int4", "nosuchschema.int4", "a.b.c.d",
    "otherdb.pg_catalog.int4",
    "", "   ", " int4 ", "-5",
    "regin_mood", "public.regin_mood", "regin_mood[]", "_regin_mood", "_regin_mood[]",
    "__regin_mood", "regin_t", "_regin_t", "regin_t[]", "regin_pair", "regin_s.t",
    "regin_mood(3)", "regin_s.regin_e", "information_schema.sql_identifier", "sql_identifier",
    "time with time", "timestamp without time", "varchar(99999999999)", "numeric(-x)",
    "numeric(5,-x)", "numeric()", "numeric(,5)", 'numeric("x")', "bpchar(1,2)", "varbit(1,2)",
    "timetz(1,2)", "timetz(-1)", "timestamptz(-1)", "pg_catalog.time(-1)",
    "pg_catalog.timestamp(1,2)", "pg_catalog.interval(1,2,3)", "pg_catalog.interval(5)",
    "pg_catalog.interval(32767)", "pg_catalog.interval(32767,-1)", "pg_catalog.interval(4)",
    "pg_catalog.interval(4,3)", "int.x", "national.x", 'numeric(-"x")',
]

RELATIONS = [
    "regin_t", "public.regin_t", "regin_s.t", "t", "regin_pair", "regin_mood", "regin_k_pkey",
    "regin_tmp", '"regin_t"', "REGIN_T", '"REGIN_T"', "regin_v", "regin_seq", "a.b.c.d",
    "otherdb.public.regin_t", "", '""', '"unterminated', ".regin_t", "regin_t.", "regin_t x",
    "  regin_t  ", "public . regin_t", "pg_class", "pg_catalog.pg_class",
    "information_schema.tables", "nosuchschema.t", "0", "-", "1259", "00001259", "4294967295",
    "4294967296", "18446744073709551616", "duckdb_tables", "sqlite_master",
]

PROCS = [
    "abs", "now", "pg_catalog.now", "nosuch", "", "regin_f", "regin_s.regin_g", "regin_g",
    "public.regin_f", "a.b.c.d", "otherdb.pg_catalog.now", "0", "-", "1", "999999999",
    "information_schema.now", "information_schema._pg_expandarray",
]

PROCEDURES = [
    "abs(int4)", "abs(integer)", "abs( integer )", "abs(int4", "abs int4", '"abs"(int4)',
    'abs("int4")', "abs(int4,)", "abs(,int4)", "abs(numeric(10,2))", "abs(int4[])",
    "abs((int4)", 'abs("int4)', "abs(none)", "abs(nosuchtype)", "(int4)", '"ab(c"(int4)',
    "now()", "now( )", "regin_f(int4)", "regin_f(int8)", "regin_s.regin_g(integer)",
    "regin_g(int4)", "regin_s.regin_g(int4)", 'abs("a,b")', "abs(int4) ",
    "abs(" + ",".join(["int4"] * 101) + ")",
]

OPERS = ["+", "||/", "-", "nosuch", "", "a.b.c.d", "0"]

OPERATORS = [
    "+(int4,int4)", "+(int4)", "+(int4,int4,int4)", "-(none,int4)", "-(int4,none)",
    "+(none,none)", "+(nosuchtype,int4)", "+", "0", "-",
]

COLLATIONS = [
    "C", '"C"', 'pg_catalog."C"', "POSIX", "default", '"default"', 'public."C"',
    'nosuch."C"', "", "ucs_basic", "a.b.c.d", "nosuch",
]

CONFIGS = ["english", "pg_catalog.english", "simple", "", "nosuch", "a.b.c.d", '"unterminated']

DICTIONARIES = [
    "simple", "pg_catalog.simple", "english_stem", "nosuch.simple", "public.nosuch", "",
    "nosuch", '"unterminated',
]

ROLES = ["postgres", '"postgres"', "nosuch", "a.b", "", "public", '"unterminated']

NAMESPACES = [
    "public", "pg_catalog", "nosuch", "a.b", "", '"public"', "regin_s", '"unterminated',
]

KINDS = [
    "regclass", "regtype", "regproc", "regprocedure", "regrole", "regnamespace",
    "regcollation", "regdictionary", "regconfig", "regoper", "regoperator",
]

FIXTURE = [
    "CREATE SCHEMA regin_s",
    "CREATE TABLE regin_t (a int)",
    "CREATE TABLE regin_s.t (a int)",
    "CREATE TABLE regin_k (id int PRIMARY KEY)",
    "CREATE TYPE regin_mood AS ENUM ('a', 'b')",
    "CREATE TYPE regin_pair AS (x int, y int)",
    "CREATE TYPE regin_s.regin_e AS ENUM ('x')",
    "CREATE VIEW regin_v AS SELECT 1 AS a",
    "CREATE SEQUENCE regin_seq",
    "CREATE FUNCTION regin_f(a int) RETURNS int AS 'SELECT a' LANGUAGE sql",
    "CREATE FUNCTION regin_s.regin_g(a int) RETURNS int AS 'SELECT a' LANGUAGE sql",
    "CREATE FUNCTION regin_g(a int) RETURNS int AS 'SELECT a' LANGUAGE sql",
    "CREATE TEMP TABLE regin_tmp (a int)",
]

TEARDOWN = [
    "DROP TABLE regin_tmp",
    "DROP FUNCTION regin_g(int)",
    "DROP FUNCTION regin_s.regin_g(int)",
    "DROP FUNCTION regin_f(int)",
    "DROP SEQUENCE regin_seq",
    "DROP VIEW regin_v",
    "DROP TYPE regin_s.regin_e",
    "DROP TYPE regin_pair",
    "DROP TYPE regin_mood",
    "DROP TABLE regin_k",
    "DROP TABLE regin_s.t",
    "DROP TABLE regin_t",
    "DROP SCHEMA regin_s",
]

CAUSES = {
    "operators": "pg_operator is empty, so regoper, regoperator and their to_reg* functions resolve no operator",
    "collations": "PostgreSQL's ucs_basic, pg_c_utf8, pg_unicode_fast and unicode collations are missing",
    "text_search": "SereneDB's full-text search has no tsvector, tsquery or text search configurations",
    "keywords": "SereneDB looks up a type name PostgreSQL's grammar reserves as a keyword, "
                "since some of its own types are named so, as union is",
}

DIFFERENCES = {
    "SELECT to_regprocedure('abs(none)')::text AS procedure": "keywords",
    "SELECT 'abs(none)'::regprocedure::text AS procedure": "keywords",
    "SELECT '+'::regoper::text AS oper": "operators",
    "SELECT to_regoper('||/')::text AS oper": "operators",
    "SELECT '||/'::regoper::text AS oper": "operators",
    "SELECT '-'::regoper::text AS oper": "operators",
    "SELECT to_regoperator('+(int4,int4)')::text AS operator": "operators",
    "SELECT '+(int4,int4)'::regoperator::text AS operator": "operators",
    "SELECT to_regoperator('-(none,int4)')::text AS operator": "operators",
    "SELECT '-(none,int4)'::regoperator::text AS operator": "operators",
    "SELECT '-'::regoper::text AS regoper": "operators",
    "SELECT to_regcollation('ucs_basic')::text AS collation": "collations",
    "SELECT 'ucs_basic'::regcollation::text AS collation": "collations",
    "SELECT 'english'::regconfig::text AS config": "text_search",
    "SELECT 'pg_catalog.english'::regconfig::text AS config": "text_search",
    "SELECT 'simple'::regconfig::text AS config": "text_search",
    "SELECT 'simple'::regdictionary::text AS dictionary": "text_search",
    "SELECT 'pg_catalog.simple'::regdictionary::text AS dictionary": "text_search",
    "SELECT 'english_stem'::regdictionary::text AS dictionary": "text_search",
    "SELECT 551::regoper::text AS oper, 551::regoperator::text AS operator, "
    "3748::regconfig::text AS config, 3765::regdictionary::text AS dictionary": "operators",
}

t = Test("The reg* types read and print object names as PostgreSQL does: every type name form "
         "with its type modifiers, relation, function, operator, collation, text search, role "
         "and schema names, numeric OIDs, the to_reg* functions that return NULL instead of "
         "failing, and the visibility functions, every answer recorded from PostgreSQL. A "
         "difference SereneDB keeps is a pair of queries, each engine's answer recorded from it, "
         "with its cause in CAUSES.")
differing = set()


def check(sql):
    cause = DIFFERENCES.get(sql)
    if cause is None:
        t.query(sql)
        return
    assert cause in CAUSES, sql
    differing.add(sql)
    t.query(sql, skipif="serenedb")
    t.query(sql, onlyif="serenedb")


for statement in FIXTURE:
    t.statement(statement)

t.section("Type names: what to_regtype and to_regtypemod read, format_type prints back, and a "
          "cast to regtype reports.")
for text in TYPES:
    value = lit(text)
    check(f"SELECT to_regtype('{value}')::text AS type, to_regtypemod('{value}') AS typmod, "
          f"format_type(to_regtype('{value}'), to_regtypemod('{value}')) AS formatted")
    check(f"SELECT '{value}'::regtype::text AS type")
check("SELECT to_regtype(current_database() || '.pg_catalog.int4')::text AS type")
check("SELECT (current_database() || '.pg_catalog.int4')::regtype::text AS type")

t.section("Relation names.")
for text in RELATIONS:
    value = lit(text)
    check(f"SELECT to_regclass('{value}')::text AS relation")
    check(f"SELECT '{value}'::regclass::text AS relation")
check("SELECT (current_database() || '.public.regin_t')::regclass::text AS relation")

t.section("Function names and signatures.")
for text in PROCS:
    value = lit(text)
    check(f"SELECT to_regproc('{value}')::text AS proc")
    check(f"SELECT '{value}'::regproc::text AS proc")
for text in PROCEDURES:
    value = lit(text)
    check(f"SELECT to_regprocedure('{value}')::text AS procedure")
    check(f"SELECT '{value}'::regprocedure::text AS procedure")

t.section("Operator names and signatures.")
for text in OPERS:
    value = lit(text)
    check(f"SELECT to_regoper('{value}')::text AS oper")
    check(f"SELECT '{value}'::regoper::text AS oper")
for text in OPERATORS:
    value = lit(text)
    check(f"SELECT to_regoperator('{value}')::text AS operator")
    check(f"SELECT '{value}'::regoperator::text AS operator")

t.section("Collations, text search configurations and dictionaries.")
for text in COLLATIONS:
    value = lit(text)
    check(f"SELECT to_regcollation('{value}')::text AS collation")
    check(f"SELECT '{value}'::regcollation::text AS collation")
for text in CONFIGS:
    check(f"SELECT '{lit(text)}'::regconfig::text AS config")
for text in DICTIONARIES:
    check(f"SELECT '{lit(text)}'::regdictionary::text AS dictionary")
check("SELECT pg_input_is_valid('\"unterminated', 'regconfig') AS config, "
      "pg_input_is_valid('\"unterminated', 'regdictionary') AS dictionary, "
      "pg_input_is_valid('nosuch', 'regconfig') AS missing_config, "
      "pg_input_is_valid('nosuch', 'regdictionary') AS missing_dictionary")

t.section("Roles and schemas.")
for text in ROLES:
    value = lit(text)
    check(f"SELECT to_regrole('{value}')::text AS role")
    check(f"SELECT '{value}'::regrole::text AS role")
for text in NAMESPACES:
    value = lit(text)
    check(f"SELECT to_regnamespace('{value}')::text AS namespace")
    check(f"SELECT '{value}'::regnamespace::text AS namespace")

t.section("OIDs that name nothing print as numbers, and InvalidOid as - or 0.")
for oid in (0, 1, 999999999):
    columns = ", ".join(f"{oid}::{kind}::text AS {kind}" for kind in KINDS)
    check(f"SELECT {columns}")
for kind in KINDS:
    check(f"SELECT '-'::{kind}::text AS {kind}")
check("SELECT ('regin_t'::regclass::oid)::regproc::text = ('regin_t'::regclass::oid)::text "
      "AS table_as_proc, ('regin_t'::regclass::oid)::regtype::text = "
      "('regin_t'::regclass::oid)::text AS table_as_type, "
      "('regin_t'::regclass::oid)::regdictionary::text = ('regin_t'::regclass::oid)::text "
      "AS table_as_dictionary")
check("SELECT count(DISTINCT o::regclass::text) AS names FROM "
      "unnest(ARRAY(SELECT g::oid FROM generate_series(1, 5000) AS g)) AS o")
check("SELECT (pg_identify_object('pg_type'::regclass, 'regin_mood'::regtype, 0)).identity AS "
      "type, (pg_identify_object('pg_proc'::regclass, 'regin_f'::regproc, 0)).identity AS proc")
check("SELECT * FROM pg_identify_object('pg_proc'::regclass, 999999999, 0)")
t.statement("SET search_path = information_schema, public")
check("SELECT 'information_schema.sql_identifier'::regtype::text AS type")
t.statement("RESET search_path")
check("SELECT 551::regoper::text AS oper, 551::regoperator::text AS operator, "
      "3748::regconfig::text AS config, 3765::regdictionary::text AS dictionary")
check("SELECT format_type(1, NULL) AS unknown, format_type(999999999, NULL) AS missing, "
      "format_type(0, NULL) AS invalid, format_type('bit'::regtype, -1) AS bit, "
      "format_type('bpchar'::regtype, -1) AS bpchar, format_type('int4'::regtype, 3) AS int4, "
      "format_type('json'::regtype, 3) AS json, format_type('uuid'::regtype, 3) AS uuid")

t.section("Visibility of relations, types and functions.")
check("SELECT pg_table_is_visible(999999999) AS missing, "
      "pg_table_is_visible('regin_tmp'::regclass) AS temporary, "
      "pg_table_is_visible('regin_s.t'::regclass) AS hidden, "
      "pg_table_is_visible('regin_t'::regclass) AS visible")
check("SELECT pg_type_is_visible(999999999) AS missing, "
      "pg_type_is_visible('regin_mood[]'::regtype) AS enum_array, "
      "pg_type_is_visible('regin_t[]'::regtype) AS row_array, "
      "pg_type_is_visible('regin_mood'::regtype) AS enum")
check("SELECT pg_function_is_visible(999999999) AS missing, "
      "pg_function_is_visible(1) AS unknown_builtin, "
      "pg_function_is_visible('regin_s.regin_g'::regproc) AS hidden, "
      "pg_function_is_visible('regin_f'::regproc) AS visible")

for statement in TEARDOWN:
    t.statement(statement)
assert differing == DIFFERENCES.keys(), DIFFERENCES.keys() - differing
t.write()
