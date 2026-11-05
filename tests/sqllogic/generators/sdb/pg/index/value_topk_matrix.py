from sqllogic_gen import Test

t = Test("ORDER BY one indexed numeric column with a LIMIT, through an inverted "
         "index and a search table, against the same query on the table "
         "itself: the values come back in the same order, NULLs and "
         "OFFSET included, over several segments with deleted rows.",
         seed=29)

D38_MAX = "9" * 33 + "." + "9" * 5

# column: (type, stored values, (lower, upper, repeated) constants)
COLUMNS = {
    "i": ("INTEGER",
          ["-2147483648", "-7", "0", "4", "5", "6", "2147483647"],
          ("0", "6", "5")),
    "b": ("BIGINT",
          ["-9223372036854775808", "-3000000000", "0", "1", "3000000000",
           "9223372036854775807"],
          ("-3000000000", "3000000000", "1")),
    "ub": ("UBIGINT",
           ["0", "1", "9223372036854775807", "9223372036854775808",
            "18446744073709551615"],
           ("1", "9223372036854775808", "9223372036854775807")),
    "h": ("HUGEINT",
          ["-170141183460469231731687303715884105728", "-1", "0", "1",
           "170141183460469231731687303715884105727"],
          ("-1", "1", "0")),
    "uh": ("UHUGEINT",
           ["0", "1", "170141183460469231731687303715884105727",
            "170141183460469231731687303715884105728",
            "340282366920938463463374607431768211455"],
           ("1", "170141183460469231731687303715884105728::UHUGEINT", "0")),
    "f": ("FLOAT",
          ["'-inf'::FLOAT", "-1.5::FLOAT", "'-0.0'::FLOAT", "0.25::FLOAT",
           "3.5::FLOAT", "'inf'::FLOAT", "'nan'::FLOAT"],
          ("-1.5::FLOAT", "'inf'::FLOAT", "0.25::FLOAT")),
    "d": ("DOUBLE",
          ["'-inf'::DOUBLE", "-1e308", "-1.5", "0.0", "'-0.0'::DOUBLE",
           "1e-308", "'inf'::DOUBLE", "'nan'::DOUBLE"],
          ("-1.5", "1e-308", "0.0")),
    "dt": ("DATE",
           ["'-infinity'::DATE", "'1970-01-01'::DATE", "'2000-02-29'::DATE",
            "'2024-12-31'::DATE", "'infinity'::DATE"],
           ("'1970-01-01'::DATE", "'2024-12-31'::DATE",
            "'2000-02-29'::DATE")),
    "ts": ("TIMESTAMP",
           ["'-infinity'::TIMESTAMP", "'1970-01-01 00:00:00'::TIMESTAMP",
            "'2024-01-01 23:00:00'::TIMESTAMP",
            "'2024-01-02 01:00:00'::TIMESTAMP", "'infinity'::TIMESTAMP"],
           ("'1970-01-01 00:00:00'::TIMESTAMP",
            "'2024-01-02 01:00:00'::TIMESTAMP",
            "'2024-01-01 23:00:00'::TIMESTAMP")),
    "tm": ("TIME",
           ["'00:00:00'::TIME", "'12:30:00'::TIME",
            "'23:59:59.999999'::TIME"],
           ("'00:00:00'::TIME", "'23:59:59.999999'::TIME",
            "'12:30:00'::TIME")),
    "tz": ("TIMETZ",
           ["'00:00:00+15'::TIMETZ", "'10:00:00+05'::TIMETZ",
            "'05:00:00+00'::TIMETZ", "'06:00:00+00'::TIMETZ",
            "'04:00:00-03'::TIMETZ", "'23:59:59-15'::TIMETZ"],
           ("'10:00:00+05'::TIMETZ", "'04:00:00-03'::TIMETZ",
            "'06:00:00+00'::TIMETZ")),
    "d4": ("DECIMAL(4,1)",
           ["-999.9", "-12.5", "0.0", "5.5", "99.9", "999.9"],
           ("-12.5", "99.9", "5.5")),
    "d18": ("DECIMAL(18,3)",
            ["-999999999999999.999", "-1.001", "0.000", "5.123",
             "999999999999999.999"],
            ("-1.001", "5.123", "0.000")),
    "d38": ("DECIMAL(38,5)",
            [f"-{D38_MAX}", "-0.00001", "0.00000",
             "12345678901234567890.12345", D38_MAX],
            ("-0.00001", "12345678901234567890.12345", "0.00000")),
    "u": ("UUID",
          ["'00000000-0000-0000-0000-000000000000'::UUID",
           "'7fffffff-ffff-ffff-ffff-ffffffffffff'::UUID",
           "'80000000-0000-0000-0000-000000000000'::UUID",
           "'c0ffee00-0000-4000-8000-000000000000'::UUID",
           "'ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID"],
          ("'7fffffff-ffff-ffff-ffff-ffffffffffff'::UUID",
           "'c0ffee00-0000-4000-8000-000000000000'::UUID",
           "'80000000-0000-0000-0000-000000000000'::UUID")),
}
ROWS = 72
BATCHES = 3
ORDERS = ["ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST",
          "DESC NULLS LAST"]


def value(column, row):
    _, values, _ = COLUMNS[column]
    if (row + len(column)) % 9 == 0:
        return "NULL"
    return values[(row * 7 + len(column)) % len(values)]


def queries(column):
    lo, hi, eq = COLUMNS[column][2]
    for order in ORDERS:
        yield "", order, "LIMIT 5"
        yield "", order, "LIMIT 4 OFFSET 7"
    yield "", "ASC NULLS FIRST", "LIMIT 1000"
    yield "", "DESC NULLS LAST", "LIMIT 1000 OFFSET 3"
    yield f"WHERE {column} > {lo}", "DESC NULLS FIRST", "LIMIT 5"
    yield f"WHERE {column} >= {lo} AND {column} < {hi}", "ASC NULLS LAST", \
        "LIMIT 6 OFFSET 2"
    yield f"WHERE {column} BETWEEN {lo} AND {hi}", "DESC NULLS LAST", "LIMIT 7"
    yield f"WHERE {column} = {eq}", "ASC NULLS FIRST", "LIMIT 3"
    yield f"WHERE {column} IS NOT NULL", "DESC NULLS FIRST", "LIMIT 4"


definition = "id INTEGER PRIMARY KEY, " + ", ".join(
    f"{name} {spec[0]}" for name, spec in COLUMNS.items())
keys = ", ".join(["id", *COLUMNS])
rows = [f"({row}, {', '.join(value(name, row) for name in COLUMNS)})"
        for row in range(ROWS)]

t.table("vtm", definition)
t.statement(f"CREATE TABLE vtm_s ({definition}) "
            "WITH (storage = 'search', compaction_interval = 0)")
t.statement(f"CREATE INDEX vtm_idx ON vtm USING inverted ({keys}) "
            "WITH (compaction_interval = 0)")
t.index("vtm_s_idx", "vtm_s", keys)
step = ROWS // BATCHES
for start in range(0, ROWS, step):
    t.insert("vtm", rows[start:start + step])
    t.statement(f"INSERT INTO vtm_s SELECT * FROM vtm WHERE id >= {start} "
                f"AND id < {start + step}")
    t.refresh("vtm")
    t.refresh("vtm_s")
t.statement("DELETE FROM vtm WHERE id % 11 = 3")
t.statement("DELETE FROM vtm_s WHERE id % 11 = 3")
t.refresh("vtm")
t.refresh("vtm_s")

for column in COLUMNS:
    t.section(f"{column} {COLUMNS[column][0]}")
    for where, order, limit in queries(column):
        tail = f"{where} ORDER BY {column} {order} {limit}".strip()
        expected = f"SELECT {column} FROM vtm {tail}"
        t.same_list(f"SELECT {column} FROM vtm_idx {tail}", expected)
        t.same_list(f"SELECT {column} FROM vtm_s {tail}", expected)

t.section("Each column's top-k is taken by the scan: no TOP_N above it.")
for column in COLUMNS:
    t.query(f"EXPLAIN SELECT {column} FROM vtm_idx "
            f"ORDER BY {column} DESC LIMIT 5")
t.query("EXPLAIN SELECT u FROM vtm_s WHERE u > "
        "'7fffffff-ffff-ffff-ffff-ffffffffffff'::UUID ORDER BY u LIMIT 5")
t.write()
