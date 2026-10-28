from sqllogic_gen import Test

VALUED = {"array", "matrix", "record", "nested", "nested_matrix", "single", "pair"}
RECORDS = {"record", "nested", "nested_matrix", "single", "pair"}

TYPES = [
    ("tinyint", ["1", "'-128'"]),
    ("utinyint", ["0", "255"]),
    ("usmallint", ["0", "65535"]),
    ("uinteger", ["0", "4294967295"]),
    ("ubigint", ["0", "'18446744073709551615'"]),
    ("hugeint", ["1", "'-170141183460469231731687303715884105728'"]),
    ("uhugeint", ["0", "'340282366920938463463374607431768211455'"]),
    ("time_ns", ["'01:02:03.123456789'", "'23:59:59.999999999'"], VALUED),
    ("timestamp_s", ["'2024-01-02 03:04:05'", "'infinity'"]),
    ("timestamp_ms", ["'2024-01-02 03:04:05.123'", "'-infinity'"]),
    ("timestamp_ns", ["'2024-01-02 03:04:05.123456789'", "'infinity'"], VALUED),
    ("timestamptz_ns", ["'2024-01-02 03:04:05.123456789+00'", "'-infinity'"], VALUED),
    ("geometry", ["'POINT(1 2)'", "'LINESTRING(0 0, 1 1)'"], RECORDS),
]

BINARY_MATRIX = {
    "time_ns": "{{01:02:03.123456,23:59:59.999999},{23:59:59.999999,01:02:03.123456}}",
    "timestamp_ns": "{{\"2024-01-02 03:04:05.123456\",infinity},"
                    "{infinity,\"2024-01-02 03:04:05.123456\"}}",
    "timestamptz_ns": "{{\"2024-01-02 03:04:05.123456+00\",-infinity},"
                      "{-infinity,\"2024-01-02 03:04:05.123456+00\"}}",
}

t = Test("Arrays and records of the DuckDB types PostgreSQL lacks print as the PostgreSQL types "
         "they map to would: one- and two-dimensional arrays with NULL elements, empty arrays, "
         "records holding the type, arrays inside records and records inside arrays, and maps, "
         "fixed-size arrays and the nested lists PostgreSQL arrays cannot hold, quoted only where "
         "the text needs it. The binary format of a type with nanoseconds holds microseconds "
         "(temporal_casts.test), and the extended engine cannot decode a record holding a "
         "geometry: those shapes are checked in text only.")

t.statement("SET TimeZone = 'UTC'")

for name, values, *rest in TYPES:
    text_only = rest[0] if rest else set()
    t.section(f"{name}.")
    first, second = (f"{value}::{name}" for value in values)
    shapes = {
        "array": f"SELECT ARRAY[{first}, NULL, {second}] AS a",
        "matrix": f"SELECT ARRAY[ARRAY[{first}, {second}], ARRAY[{second}, {first}]] AS a",
        "empty": f"SELECT '{{}}'::{name}[] AS a",
        "record": f"SELECT ROW({first}, {second}, NULL::{name}) AS r",
        "nested": f"SELECT ROW(ARRAY[{first}, {second}], ARRAY[{first}], ARRAY[{second}], "
                  f"ARRAY[NULL::{name}], '{{}}'::{name}[]) AS r",
        "nested_matrix": f"SELECT ROW(ARRAY[ARRAY[{first}, {second}], ARRAY[{second}, {first}]], "
                         f"ARRAY[ARRAY[{first}]], ARRAY[ARRAY[{first}, {second}]]) AS r",
        "single": f"SELECT ARRAY[ROW({first}), ROW({second}), ROW(NULL::{name})] AS a",
        "pair": f"SELECT ARRAY[ROW({first}, {second})] AS a",
    }
    for shape, sql in shapes.items():
        t.query(sql, skipif="pg-wire-extended" if shape in text_only else None)
    if name in BINARY_MATRIX:
        t.query(shapes["matrix"], ["a"], [[BINARY_MATRIX[name]]], onlyif="pg-wire-extended")

t.section("A type alias over ubigint.")
t.statement("CREATE TYPE wire_ubigint AS ubigint")
t.query("SELECT 5::wire_ubigint AS v, ARRAY[5::wire_ubigint, NULL] AS a, ROW(5::wire_ubigint) AS r")
t.statement("DROP TYPE wire_ubigint")

t.section("Maps and fixed-size arrays.")
t.query("SELECT ARRAY[map(['a'], [1])] AS m, ROW(map(['a'], [1])) AS r, "
        "ROW(ARRAY[map(['a'], [1])]) AS n, ARRAY[ROW(map(['a b'], [1]))] AS s")
t.query("SELECT ROW(array_value(1, 2)) AS r, ARRAY[ROW(array_value(1, 2))] AS a, "
        "ROW(ARRAY[array_value(1, 2)]) AS n, ROW(ARRAY[array_value(3)]) AS one")

t.section("Empty and NULL sub-arrays, which PostgreSQL arrays cannot hold.")
t.query("SELECT ARRAY[ARRAY[]::int[]] AS a, ARRAY['{}'::int[][]] AS b")
t.query("SELECT ROW(ARRAY[ARRAY[]::int[]]) AS e, ROW(ARRAY[NULL::int[]]) AS n, "
        "ROW(ARRAY[ARRAY['a b'], NULL]) AS m", skipif="pg-wire-extended")

t.section("An element type with no array output.")
t.error("SELECT ARRAY['1'::bignum] AS a", "Array element type not supported")

t.statement("RESET TimeZone")
t.write()
