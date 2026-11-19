from sqllogic_gen import Test

CAUSES = {
    "quoted_null": "read_csv reads a quoted empty field as NULL (allow_quoted_nulls)",
    "json_list": "A list of json values renders a NULL element as NULL, which is not JSON, so the list "
                 "does not read back from its text",
    "union_text": "The text of a UNION value does not say which member holds it, so it reads back as the "
                  "VARCHAR member",
    "json_space": "The JSON files hold a json value without the whitespace the value keeps",
    "json_blob": "to_json writes a BLOB as its escaped text, and the JSON reader takes that text as the "
                 "bytes",
}

ALL = {"csv", "csv list", "csv sequential", "ndjson", "json array"}
CSV = {"csv", "csv list", "csv sequential"}
CSV_VALUES = {"csv", "csv sequential"}
CSV_LISTS = {"csv list", "csv sequential"}
JSON = {"ndjson", "json array"}

TEXTS = ["'a,b'", "'a\"b'", "'a\\b'", "''", "'NULL'", "'\\N'", "E'a\\tb'", "'\\.'"]

VARCHARS = ["'é日本😀'", "'a' || chr(13) || chr(10) || 'b'", "'[1, 2]'", "'{a: 1}'", "' lead'",
            "'trail '", "''''", "'null'", "'a=b:c'", "'\\'''"]

SETUP = {
    "dfm_mood": "CREATE TYPE dfm_mood AS ENUM ('sad', 'ok', 'happy')",
    "dfm_pair": "CREATE TYPE dfm_pair AS (x int, y text)",
}

TYPES = [
    ("boolean", ["true", "false"]),
    ("smallint", ["1", "'-32768'"]),
    ("integer", ["1", "'-2147483648'"]),
    ("bigint", ["1", "'-9223372036854775808'"]),
    ("real", ["1.5", "0.1", "'-0'", "'NaN'", "'-Infinity'"]),
    ("double precision", ["1.5", "0.1", "-1e300", "'5e-324'", "'Infinity'", "'NaN'"]),
    ("numeric(4,1)", ["1.5", "-2.0"]),
    ("numeric(9,2)", ["1234567.89", "-1.00"]),
    ("numeric(18,2)", ["1234567890123456.78", "0.50"]),
    ("numeric(38,2)", ["123456789012345678901234567890123456.78", "-0.01"]),
    ("text", TEXTS, {"quoted_null": CSV_VALUES}),
    ("varchar(10)", ["'a b'", "'a,b'", "''"], {"quoted_null": CSV_VALUES}),
    ("char(3)", ["'ab'", "'a,'"]),
    ("name", ["'abc'", "'a b'"]),
    ('"char"', ["'a'", "','"]),
    ("bytea", ["unhex('0102')", "''", "'a b'", "unhex('5c27ff')"], {"json_blob": JSON}),
    ("date", ["'2024-01-02'", "'infinity'"]),
    ("time", ["'01:02:03'", "'23:59:59.999999'"]),
    ("timetz", ["'01:02:03+02'", "'23:00:00-05:30'"]),
    ("timestamp", ["'2024-01-02 03:04:05'", "'infinity'"]),
    ("timestamptz", ["'2024-01-02 03:04:05+00'", "'-infinity'"]),
    ("interval", ["'1 day 2 hours'", "'-3 mons'"]),
    ("uuid", ["'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"]),
    ("json", ["'{\"a\": 1}'", "'[1, 2]'", "'\"s,t\"'"], {"json_list": CSV_LISTS, "json_space": JSON}),
    ("jsonb", ["'{\"a\": 1}'", "'[1, \"x\"]'"], {"json_list": CSV_LISTS, "json_space": JSON}),
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
    ("numrange", ["'[1.5,2.5]'", "'empty'"]),
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
    ("dfm_mood", ["'sad'", "'happy'"]),
    ("dfm_pair", ["ROW(1, 'a b')", "ROW(NULL, 'x,y')"]),
    ("oidvector", ["'1 2'", "'3'"]),
    ("int2vector", ["'3 4'", "'5'"]),
    ("tsquery", ["'a & b'", "'c'"]),
    ("regclass", ["'pg_class'", "0"]),
    ("regtype", ["'int4'", "0"]),
    ("regproc", ["'now'", "0"]),
    ("regprocedure", ["'now()'", "0"]),
    ("regnamespace", ["'public'", "0"]),
    ("regrole", ["'postgres'", "0"]),
    ("regcollation", ["'\"C\"'", "0"]),
    ("regconfig", ["0"]),
    ("regdictionary", ["0"]),
    ("regoper", ["0"]),
    ("regoperator", ["0"]),
    ("tinyint", ["1", "'-128'"]),
    ("utinyint", ["0", "255"]),
    ("usmallint", ["0", "65535"]),
    ("uinteger", ["0", "4294967295"]),
    ("ubigint", ["0", "'18446744073709551615'"]),
    ("hugeint", ["1", "'-170141183460469231731687303715884105728'",
                 "'170141183460469231731687303715884105727'"]),
    ("uhugeint", ["0", "'340282366920938463463374607431768211455'"]),
    ("bignum", ["'1'", "'-123456789012345678901234567890123456789012345678901234567890'"]),
    ("varchar", VARCHARS),
    ("time_ns", ["'01:02:03.123456789'", "'23:59:59.999999999'"]),
    ("timestamp_s", ["'2024-01-02 03:04:05'", "'infinity'"]),
    ("timestamp_ms", ["'2024-01-02 03:04:05.123'", "'-infinity'"]),
    ("timestamp_ns", ["'2024-01-02 03:04:05.123456789'", "'infinity'"]),
    ("timestamptz_ns", ["'2024-01-02 03:04:05.123456789+00'", "'-infinity'"]),
    ("integer[]", ["[1, 2, NULL]", "[]"]),
    ("varchar[]", ["['a,b', 'NULL', NULL]", "['']"]),
    ("integer[][]", ["[[1, 2], [], NULL]"]),
    ("STRUCT(a INTEGER, b VARCHAR)", ["{'a': 1, 'b': 'x'}", "{'a': NULL, 'b': 'a,b'}"]),
    ("STRUCT(k VARCHAR, l INTEGER[])", ["{'k': '[1]', 'l': [1, NULL]}"]),
    ("MAP(VARCHAR, INTEGER)", ["MAP {'a': 1, 'b': NULL}", "MAP {}"]),
    ("MAP(VARCHAR, VARCHAR)", ["MAP {'k=1': 'a,b', 'x y': ''}"]),
    ("INTEGER[2]", ["[1, 2]", "[NULL, 3]"]),
    ("UNION(num INTEGER, str VARCHAR)", ["union_value(num := 42)", "union_value(str := 'a,b')"],
     {"union_text": CSV}),
    ("geometry", ["'POINT(1 2)'", "'LINESTRING(0 0, 1 1)'"]),
]

t = Test("Every type goes through the DuckDB rendering, which read_csv and read_json use to turn "
         "field text into values: a column of the type and a list of it are written to a CSV file "
         "as their VARCHAR casts and to a newline-delimited and an array JSON file by COPY ... "
         "(FORMAT json), and read back, given the columns' types, by read_csv three ways (the "
         "column alone on the default parallel path, the list alone, both with parallel = false), "
         "by read_ndjson and by read_json. Each check compares the values read with the originals, "
         "by value and by their VARCHAR casts. The VARCHAR and to_json renderings of every value "
         "and the types read_csv and read_ndjson detect are recorded. A value that does not come "
         "back is an expected difference with its cause in CAUSES, whose check records the values "
         "read instead.")


def cause_of(causes, check):
    return next((cause for cause, of in causes.items() if check in of), None)


def compare(table, reader, read, ids, cause):
    same = " AND ".join(["o.id IS NOT NULL", "r.id IS NOT NULL"] + [
        f"r.{c} IS NOT DISTINCT FROM o.{c} AND r.{c}::VARCHAR IS NOT DISTINCT FROM o.{c}::VARCHAR"
        for c in read])
    joined = f"FROM {table} o FULL JOIN {reader} r ON r.id = o.id ORDER BY 1"
    if cause is None:
        t.expect(f"SELECT coalesce(o.id, r.id) AS id, {same} AS same {joined}", ["id", "same"],
                 [[i, "t"] for i in ids])
        return
    texts = "".join(f", r.{c}::VARCHAR AS {c}" for c in read)
    t.query(f"SELECT coalesce(o.id, r.id) AS id{texts}, {same} AS same {joined}")


t.control("substitution on")
t.statement("SET TimeZone = 'UTC'")

for k, (name, values, *rest) in enumerate(TYPES):
    causes = rest[0] if rest else {}
    assert set(causes) <= set(CAUSES) and set().union(*causes.values()) <= ALL, name
    t.section(f"{name}.")
    if name in SETUP:
        t.statement(SETUP[name])
    table = f"dfm_{k}"
    path = f"${{__TEST_DIR__}}/{table}"
    t.statement(f"CREATE TABLE {table} (id int, v {name}, a {name}[])")
    rows = [f"({i + 1}, ({value})::{name}, ARRAY[({value})::{name}, NULL])"
            for i, value in enumerate(values)]
    rows.append(f"({len(values) + 1}, NULL, NULL)")
    t.statement(f"INSERT INTO {table} VALUES {', '.join(rows)}")
    t.statement(f"COPY (SELECT id, v::VARCHAR AS v, a::VARCHAR AS a FROM {table}) "
                f"TO '{path}.csv' (FORMAT csv, HEADER)")
    t.statement(f"COPY {table} TO '{path}.ndjson' (FORMAT json)")
    t.statement(f"COPY {table} TO '{path}.json' (FORMAT json, ARRAY true)")
    t.query(f"SELECT id, v::VARCHAR AS v, a::VARCHAR AS a FROM {table} ORDER BY id")
    t.query(f"SELECT id, to_json(v)::VARCHAR AS v, to_json(a)::VARCHAR AS a FROM {table} ORDER BY id")
    columns = f"columns = {{'id': 'INTEGER', 'v': '{name}', 'a': '{name}[]'}}"
    csv = f"read_csv('{path}.csv', {columns}, header = true, auto_detect = false"
    ids = range(1, len(values) + 2)
    for check, reader, read in [
            ("csv", f"{csv})", ["v"]),
            ("csv list", f"{csv})", ["a"]),
            ("csv sequential", f"{csv}, parallel = false)", ["v", "a"])]:
        compare(table, reader, read, ids, cause_of(causes, check))
    t.query(f"SELECT typeof(v) AS v, typeof(a) AS a FROM read_csv('{path}.csv') LIMIT 1")
    for check, reader in [
            ("ndjson", f"read_ndjson('{path}.ndjson', {columns})"),
            ("json array", f"read_json('{path}.json', {columns})")]:
        compare(table, reader, ["v", "a"], ids, cause_of(causes, check))
    t.query(f"SELECT typeof(v) AS v, typeof(a) AS a FROM read_ndjson('{path}.ndjson') LIMIT 1")

t.statement("RESET TimeZone")
t.write()
