from sqllogic_gen import Test

CAUSES = {
    "typmod": "Type modifiers are dropped or rejected",
    "numeric": "A bare numeric is DECIMAL(18,3)",
    "json_array": "json[] reads and prints text as one JSON array instead of an array literal",
    "empty_bit": "An empty bit string becomes B'0'",
    "text_search": "SereneDB's full-text search has no tsvector, tsquery or text search configurations",
}

VALUED = {"array", "matrix", "record", "nested", "nested_matrix", "single", "pair"}

TEXTS = ["'a b'", "'a,b'", "'a\"b'", "'a\\b'", "''", "'NULL'", "'x'"]

TYPES = [
    ("boolean", ["true", "false"]),
    ("smallint", ["1", "-2"]),
    ("integer", ["1", "'-2147483648'"]),
    ("bigint", ["1", "'-9223372036854775808'"]),
    ("real", ["1.5", "'NaN'", "'-Infinity'"]),
    ("double precision", ["1.5", "-1e300", "'Infinity'"]),
    ("numeric(4,1)", ["1.5", "-2.0"]),
    ("numeric(9,2)", ["1234567.89", "-1.00"]),
    ("numeric(18,2)", ["1234567890123456.78", "0.50"]),
    ("numeric(38,2)", ["123456789012345678901234567890123456.78", "-0.01"]),
    ("text", TEXTS),
    ("varchar(10)", ["'a b'", "'a,b'", "''"]),
    ("char(3)", ["'ab'", "'a,'"], {"typmod": VALUED}),
    ("name", ["'abc'", "'a b'"]),
    ('"char"', ["'a'", "' '"]),
    ("bytea", ["'\\x0102'", "'\\x'", "'a b'"]),
    ("date", ["'2024-01-02'", "'infinity'"]),
    ("time", ["'01:02:03'", "'23:59:59.999999'"]),
    ("timetz", ["'01:02:03+02'", "'23:00:00-05:30'"]),
    ("timestamp", ["'2024-01-02 03:04:05'", "'infinity'"]),
    ("timestamptz", ["'2024-01-02 03:04:05+00'", "'-infinity'"]),
    ("interval", ["'1 day 2 hours'", "'-3 mons'"], {},
     {"record", "nested", "nested_matrix", "single", "pair"}),
    ("uuid", ["'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"]),
    ("json", ["'{\"a\": 1}'", "'[1, 2]'", "'\"s\"'"], {"json_array": {"empty", "nested"}}),
    ("bit(3)", ["B'101'", "B'000'"]),
    ("varbit", ["B'1'", "B''"], {"empty_bit": VALUED}),
    ("inet", ["'192.168.0.1'", "'::1/128'"]),
    ("cidr", ["'10.0.0.0/8'"]),
    ("macaddr", ["'08:00:2b:01:02:03'"]),
    ("macaddr8", ["'08:00:2b:01:02:03:04:05'"]),
    ("money", ["'12.34'", "'-1'", "'-1234.56'"]),
    ("point", ["'(1,2)'", "'(-1.5,0)'"]),
    ("line", ["'{1,2,3}'"]),
    ("lseg", ["'[(1,2),(3,4)]'"]),
    ("box", ["'(3,4),(1,2)'"]),
    ("path", ["'[(1,2),(3,4)]'", "'((0,0),(1,1))'"]),
    ("polygon", ["'((0,0),(1,0),(1,1))'"]),
    ("circle", ["'<(1,2),3>'"]),
    ("int4range", ["'[1,5)'", "'empty'"]),
    ("int8range", ["'[1,5)'", "'(,10]'"]),
    ("numrange", ["'[1.5,2.5]'", "'empty'"], {"numeric": VALUED}),
    ("daterange", ["'[2024-01-01,2024-02-01)'"]),
    ("tsrange", ["'[2024-01-01 00:00,2024-01-02 00:00)'"]),
    ("tstzrange", ["'[2024-01-01 00:00+00,)'"]),
    ("int4multirange", ["'{[1,3), [5,7)}'", "'{}'"]),
    ("nummultirange", ["'{[1.5,2.5]}'"], {"numeric": VALUED}),
    ("tid", ["'(1,2)'"]),
    ("pg_lsn", ["'16/B374D848'"]),
    ("oid", ["1", "4294967295"]),
    ("xid", ["'1'", "'4294967295'"], {}, VALUED),
    ("xid8", ["'1'", "'9223372036854775807'"], {}, VALUED),
    ("cid", ["'1'", "'4294967295'"], {}, VALUED),
    ("oidvector", ["'1 2'", "''"]),
    ("int2vector", ["'3 4'", "'5'"]),
    ("tsquery", ["'a & b'", "'c'"], {"text_search": VALUED}, VALUED),
    ("xml", ["'<a>1</a>'", "'<?xml version=\"1.0\"?><b/>'",
             "'<?xml version=\"1.0\" standalone=\"yes\"?><c/>'"]),
    ("wire_mood", ["'sad'", "'happy'"], {}, {"nested", "nested_matrix"}),
    ("wire_wide", ["'l1'", "'l300'"], {}, {"nested", "nested_matrix"}),
    ("wire_huge", ["'l1'", "'l65536'"], {}, {"nested", "nested_matrix"}),
    ("wire_pair", ["ROW(1, 'a b')", "ROW(NULL, 'x')"], {}, {"nested", "nested_matrix"}),
]

REGS = ["regclass", "regtype", "regproc", "regprocedure", "regrole", "regnamespace",
        "regcollation", "regdictionary", "regconfig", "regoper", "regoperator"]

t = Test("Arrays and records of every type print as PostgreSQL prints them, in text and binary: "
         "one- and two-dimensional arrays with NULL elements, empty arrays, records holding the "
         "type, arrays inside records and records inside arrays, every answer recorded from "
         "PostgreSQL. A difference SereneDB keeps is a pair of queries, each engine's answer "
         "recorded from it, with its cause in CAUSES.")


def check(sql, cause=None, binary=True):
    extended = None if binary else "pg-wire-extended"
    if cause is None:
        t.query(sql, skipif=extended)
        return
    assert cause in CAUSES, sql
    if binary:
        t.query(sql, skipif="serenedb")
    else:
        t.query(sql, onlyif="postgres", skipif=extended)
    t.query(sql, onlyif="serenedb", skipif=extended)


t.statement("SET TimeZone = 'UTC'")
t.statement("CREATE TYPE wire_mood AS ENUM ('sad', 'ok', 'happy')")
labels = ", ".join(f"'l{i}'" for i in range(1, 301))
t.statement(f"CREATE TYPE wire_wide AS ENUM ({labels})")
labels = ", ".join(f"'l{i}'" for i in range(1, 65537))
t.statement(f"CREATE TYPE wire_huge AS ENUM ({labels})")
t.statement("CREATE TYPE wire_pair AS (x int, y text)")

for name, values, *rest in TYPES:
    causes, text_only = (rest + [{}, set()])[:2]
    assert set().union(text_only, *causes.values()) <= VALUED | {"empty"}, name
    t.section(f"{name}.")
    cast = [f"{value}::{name}" for value in values]
    first, second = cast[0], cast[-1]
    shapes = {
        "array": f"SELECT ARRAY[{first}, NULL, {second}] AS a",
        "matrix": f"SELECT ARRAY[ARRAY[{first}, {second}], ARRAY[{second}, {first}]] AS a",
        "empty": f"SELECT '{{}}'::{name}[] AS a",
        "record": f"SELECT ROW({', '.join(cast)}, NULL::{name}) AS r",
        "nested": f"SELECT ROW(ARRAY[{first}, {second}], ARRAY[{first}], ARRAY[{second}], "
                  f"ARRAY[NULL::{name}], ARRAY[NULL::{name}, NULL], '{{}}'::{name}[]) AS r",
        "nested_matrix": f"SELECT ROW(ARRAY[ARRAY[{first}, {second}], ARRAY[{second}, {first}]], "
                         f"ARRAY[ARRAY[{first}]], ARRAY[ARRAY[{first}, {second}]], "
                         f"ARRAY[ARRAY[NULL::{name}]]) AS r",
        "single": f"SELECT ARRAY[ROW({first}), ROW({second}), ROW(NULL::{name})] AS a",
        "pair": f"SELECT ARRAY[ROW({first}, {second})] AS a",
    }
    for shape, sql in shapes.items():
        check(sql, next((cause for cause, of in causes.items() if shape in of), None),
              shape not in text_only)

t.section("json arrays inside a record.")
check("SELECT ROW(ARRAY['{\"a\": 1}'::json, '\"s\"'::json], ARRAY['[1, 2]'::json]) AS r")

t.section("Floating point with extra_float_digits 0.")
t.statement("SET extra_float_digits = 0")
for name, value in [("real", "1.1"), ("double precision", "0.1")]:
    check(f"SELECT {value}::{name} AS v, ARRAY[{value}::{name}, NULL] AS a, "
          f"ARRAY[ARRAY[{value}::{name}]] AS m, ROW({value}::{name}) AS r, "
          f"ROW(ARRAY[{value}::{name}], ARRAY[ARRAY[{value}::{name}]]) AS n")
t.statement("RESET extra_float_digits")

t.section("Text that needs quotes inside one container and not the other.")
check("SELECT ARRAY[ROW('NULL'::text), ROW('null'::text), ROW('a{b'::text)] AS a, "
      "ROW(ARRAY['a(b'::text], ARRAY['NULL'::text]) AS r")

t.section("timestamptz in a time zone with a minutes offset.")
t.statement("SET TimeZone = 'Asia/Kolkata'")
check("SELECT ARRAY['2024-01-02 03:04:05+00'::timestamptz, NULL] AS a, "
      "ROW('2024-01-02 03:04:05+00'::timestamptz) AS r, "
      "ROW(ARRAY['2024-01-02 03:04:05+00'::timestamptz]) AS n, "
      "ARRAY[ROW('2024-01-02 03:04:05+00'::timestamptz)] AS s", binary=False)
t.statement("SET TimeZone = 'UTC'")

t.section("Vectors at the top level.")
check("SELECT '1 2'::oidvector AS o, '3 4'::int2vector AS i")

t.section("bytea in the escape format, which only the text protocol shows.")
t.statement("SET bytea_output = 'escape'")
check("SELECT ARRAY[ARRAY['\\x01'::bytea, 'a b'::bytea]] AS m, "
      "ROW(ARRAY[ARRAY['abc'::bytea]]) AS n", binary=False)
check("SELECT ARRAY['\\x0102'::bytea, NULL, 'a b'::bytea] AS a", binary=False)
check("SELECT ROW('\\x0102'::bytea, 'a\\\\b'::bytea) AS r", binary=False)
check("SELECT ROW(ARRAY['abc'::bytea]) AS r, ARRAY[ROW('abc'::bytea)] AS a, "
      "ARRAY[ROW('a b'::bytea)] AS spaced, ROW(ARRAY['\\x01'::bytea]) AS unprintable",
      binary=False)
t.statement("RESET bytea_output")

t.section("The reg* types, by OID and in arrays, as text.")
for kind in REGS:
    check(f"SELECT 0::{kind} AS v, ARRAY[0::{kind}, NULL] AS a, ROW(ARRAY[0::{kind}]) AS r, "
          f"ARRAY[ROW(0::{kind})] AS ra, ROW(0::{kind}) AS f, ARRAY[ARRAY[0::{kind}]] AS m, "
          f"ROW(ARRAY[ARRAY[0::{kind}]]) AS n", binary=False)
for sql in [
    "SELECT 'pg_class'::regclass AS c, ARRAY['pg_class'::regclass, 'pg_type'::regclass] AS a",
    "SELECT 'int4'::regtype AS t, ARRAY['int4'::regtype, 'text[]'::regtype] AS a",
    "SELECT 'now'::regproc AS p, ARRAY['now'::regproc] AS a",
    "SELECT 'abs(int4)'::regprocedure AS p, ARRAY['abs(int4)'::regprocedure] AS a, "
    "ROW(ARRAY['abs(int4)'::regprocedure]) AS r, ARRAY[ROW('abs(int4)'::regprocedure)] AS ra",
    "SELECT ROW(ARRAY['\"C\"'::regcollation]) AS r, ARRAY[ROW('\"C\"'::regcollation)] AS ra",
    "SELECT 'postgres'::regrole AS r, ARRAY['postgres'::regrole] AS a",
    "SELECT 'public'::regnamespace AS n, ARRAY['public'::regnamespace] AS a",
    "SELECT '\"C\"'::regcollation AS c, ARRAY['\"C\"'::regcollation] AS a",
]:
    check(sql, binary=False)

t.section("Records with no fields and nested containers.")
t.query("SELECT ROW() AS r, ARRAY[ROW()] AS a")
t.query("SELECT ARRAY[ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[ARRAY[5, 6], ARRAY[7, 8]]] AS a")
t.query("SELECT ROW(ROW(1, 'a b'), ARRAY[ROW(2, 'c')]) AS r")
t.query("SELECT ARRAY[ROW(ARRAY[1, 2]), ROW('{}'::int[]), ROW(ARRAY[3])] AS a")

t.statement("DROP TYPE wire_pair")
t.statement("DROP TYPE wire_huge")
t.statement("DROP TYPE wire_wide")
t.statement("DROP TYPE wire_mood")
t.statement("RESET TimeZone")
t.write()
