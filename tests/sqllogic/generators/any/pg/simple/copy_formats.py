from sqllogic_gen import Test

CAUSES = {
    "typmod": "Type modifiers are dropped or rejected",
    "numeric": "A bare numeric is DECIMAL(18,3)",
}

FORMATS = ["text", "csv", "binary"]

ALL = {f"{fmt} {what}" for fmt in FORMATS for what in ("bytes", "values")}

TEXTS = ["'a,b'", "'a\"b'", "'a\\b'", "''", "'NULL'", "'\\N'", "E'a\\tb'", "'\\.'"]

TYPES = [
    ("boolean", ["true", "false"]),
    ("smallint", ["1", "'-32768'"]),
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
    ("char(3)", ["'ab'", "'a,'"], {"typmod": ALL}),
    ("name", ["'abc'", "'a b'"]),
    ('"char"', ["'a'", "','"]),
    ("bytea", ["'\\x0102'", "'\\x'", "'a b'"]),
    ("date", ["'2024-01-02'", "'infinity'"]),
    ("time", ["'01:02:03'", "'23:59:59.999999'"]),
    ("timetz", ["'01:02:03+02'", "'23:00:00-05:30'"]),
    ("timestamp", ["'2024-01-02 03:04:05'", "'infinity'"]),
    ("timestamptz", ["'2024-01-02 03:04:05+00'", "'-infinity'"]),
    ("interval", ["'1 day 2 hours'", "'-3 mons'"]),
    ("uuid", ["'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"]),
    ("json", ["'{\"a\": 1}'", "'[1, 2]'", "'\"s,t\"'"]),
    ("jsonb", ["'{\"a\": 1}'", "'[1, \"x\"]'"]),
    ("bit(3)", ["B'101'", "B'000'"]),
    ("varbit", ["B'1'", "B'0110'"]),
    ("inet", ["'192.168.0.1'", "'::1/128'", "'10.1.2.3/8'"]),
    ("cidr", ["'10.0.0.0/8'", "'::/0'"]),
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
    ("numrange", ["'[1.5,2.5]'", "'empty'"], {"numeric": ALL}),
    ("daterange", ["'[2024-01-01,2024-02-01)'"]),
    ("tsrange", ["'[2024-01-01 00:00,2024-01-02 00:00)'"]),
    ("tstzrange", ["'[2024-01-01 00:00+00,)'"]),
    ("int4multirange", ["'{[1,3), [5,7)}'", "'{}'"]),
    ("tid", ["'(1,2)'"]),
    ("pg_lsn", ["'16/B374D848'"]),
    ("oid", ["1", "4294967295"]),
    ("xid", ["'1'", "'4294967295'"]),
    ("xid8", ["'1'", "'18446744073709551615'"]),
    ("cid", ["'1'", "'4294967295'"]),
    ("xml", ["'<a>1</a>'", "'<b x=\"1,2\"/>'"]),
    ("cfm_mood", ["'sad'", "'happy'"]),
    ("cfm_pair", ["ROW(1, 'a b')", "ROW(NULL, 'x,y')"]),
]

t = Test("Every type goes through COPY TO and COPY FROM a file in text, csv and binary: the text "
         "and csv bytes each format writes, and the values each format reads back, of a column "
         "of the type and of an array of it, every answer recorded from PostgreSQL. A difference "
         "SereneDB keeps is a pair of queries, each engine's answer recorded from it, with its "
         "cause in CAUSES.")


TEXT_ONLY = {"jsonb"}


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


t.control("substitution on")
t.statement("SET TimeZone = 'UTC'")
t.statement("CREATE TYPE cfm_mood AS ENUM ('sad', 'ok', 'happy')")
t.statement("CREATE TYPE cfm_pair AS (x int, y text)")
t.statement("CREATE TABLE cfm_lines (l text)")

for k, (name, values, *rest) in enumerate(TYPES):
    causes = rest[0] if rest else {}
    assert set().union(*causes.values()) <= ALL, name
    t.section(f"{name}.")
    table = f"cfm_{k}"
    t.statement(f"CREATE TABLE {table} (id int, v {name}, a {name}[])")
    rows = [f"({i + 1}, {value}::{name}, ARRAY[{value}::{name}, NULL])" for i, value in enumerate(values)]
    rows.append(f"({len(values) + 1}, NULL, NULL)")
    t.statement(f"INSERT INTO {table} VALUES {', '.join(rows)}")
    for fmt in FORMATS:
        path = f"${{__TEST_DIR__}}/{table}.{fmt}"
        t.statement(f"COPY {table} TO '{path}' (FORMAT {fmt})")
        if fmt != "binary":
            t.statement("TRUNCATE cfm_lines")
            t.statement(f"COPY cfm_lines FROM '{path}' (FORMAT csv, DELIMITER E'\\x01', QUOTE E'\\x02')")
            check("SELECT l FROM cfm_lines ORDER BY l",
                  next((cause for cause, of in causes.items() if f"{fmt} bytes" in of), None))
        t.statement(f"CREATE TABLE {table}_{fmt} (id int, v {name}, a {name}[])")
        t.statement(f"COPY {table}_{fmt} FROM '{path}' (FORMAT {fmt})")
        check(f"SELECT id, v, a FROM {table}_{fmt} ORDER BY id",
              next((cause for cause, of in causes.items() if f"{fmt} values" in of), None),
              name not in TEXT_ONLY)

t.statement("RESET TimeZone")
t.write()
