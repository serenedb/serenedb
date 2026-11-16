import operator

from sqllogic_gen import Test

ROWS = 5000
FLUSHED = 3000
PIVOT = 7

OPS = [
    ("=", operator.eq),
    ("<>", operator.ne),
    (">", operator.gt),
    (">=", operator.ge),
    ("<", operator.lt),
    ("<=", operator.le),
]

BASE = "((i * 37) % 101)"
K = f"({BASE} - 50)"

COLUMNS = [
    ("c_bool", "BOOLEAN", f"{BASE} % 2 = 0",
     lambda b: b % 2 == 0, "true", True),
    ("c_i8", "TINYINT", f"{K}::TINYINT",
     lambda b: b - 50, f"{PIVOT}::TINYINT", PIVOT),
    ("c_i16", "SMALLINT", f"({K} * 300)::SMALLINT",
     lambda b: (b - 50) * 300, f"{PIVOT * 300}::SMALLINT", PIVOT * 300),
    ("c_i32", "INTEGER", f"({K} * 40000000)::INTEGER",
     lambda b: (b - 50) * 40000000, f"{PIVOT * 40000000}", PIVOT * 40000000),
    ("c_i64", "BIGINT", f"{K}::BIGINT * 100000000000",
     lambda b: (b - 50) * 100000000000, f"{PIVOT * 100000000000}",
     PIVOT * 100000000000),
    ("c_i128", "HUGEINT", f"{K}::HUGEINT * 1000000000000000000000000000000::HUGEINT",
     lambda b: (b - 50) * 10**30,
     f"{PIVOT}::HUGEINT * 1000000000000000000000000000000::HUGEINT",
     PIVOT * 10**30),
    ("c_u8", "UTINYINT", f"{BASE}::UTINYINT",
     lambda b: b, f"{PIVOT + 50}::UTINYINT", PIVOT + 50),
    ("c_u16", "USMALLINT", f"({BASE} * 600)::USMALLINT",
     lambda b: b * 600, f"{(PIVOT + 50) * 600}::USMALLINT", (PIVOT + 50) * 600),
    ("c_u32", "UINTEGER", f"({BASE}::BIGINT * 40000000)::UINTEGER",
     lambda b: b * 40000000, f"{(PIVOT + 50) * 40000000}::UINTEGER",
     (PIVOT + 50) * 40000000),
    ("c_u64", "UBIGINT", f"{BASE}::UBIGINT * 100000000000000000::UBIGINT",
     lambda b: b * 10**17, f"{(PIVOT + 50) * 10**17}::UBIGINT",
     (PIVOT + 50) * 10**17),
    ("c_u128", "UHUGEINT",
     f"{BASE}::UHUGEINT * 1000000000000000000000000000000000000::UHUGEINT",
     lambda b: b * 10**36,
     f"{PIVOT + 50}::UHUGEINT * 1000000000000000000000000000000000000::UHUGEINT",
     (PIVOT + 50) * 10**36),
    ("c_f32", "REAL", f"({K} / 4.0)::REAL",
     lambda b: (b - 50) / 4.0, f"{PIVOT / 4.0}::REAL", PIVOT / 4.0),
    ("c_f64", "DOUBLE", f"{K} / 8.0",
     lambda b: (b - 50) / 8.0, f"{PIVOT / 8.0}::DOUBLE", PIVOT / 8.0),
    ("c_iv", "INTERVAL", f"{K} * INTERVAL '1 minute'",
     lambda b: b - 50, f"INTERVAL '{PIVOT} minutes'", PIVOT),
    ("c_s", "VARCHAR",
     f"'v' || lpad({BASE}::VARCHAR, 3, '0') || '_a_suffix_longer_than_twelve'",
     lambda b: f"v{b:03d}_a_suffix_longer_than_twelve",
     f"'v{PIVOT + 50:03d}_a_suffix_longer_than_twelve'",
     f"v{PIVOT + 50:03d}_a_suffix_longer_than_twelve"),
]


def is_null(i):
    return i % 7 == 3


def base(i):
    return (i * 37) % 101


def value(column, i):
    return None if is_null(i) else column[3](base(i))


t = Test("Every comparison of a pushed filter on every physical type its "
         "kernels dispatch on, on a search table with NULLs, flushed and "
         "unflushed rows and a partial last vector: counted alone, with the "
         "column read and after another column's filter, on a table scanned "
         "by its columns (selection paths) and on one with a primary key, "
         "whose index scan hands the filter whole masks (the mask path). The "
         "generator works out every count from the same formulas.", seed=7)

columns = ", ".join(f"{name} {sql_type}" for name, sql_type, *_ in COLUMNS)
exprs = ", ".join(f"CASE WHEN i % 7 = 3 THEN NULL ELSE {expr} END"
                  for _, _, expr, *_ in COLUMNS)
TABLES = [("sft", "i BIGINT"), ("sftk", "i BIGINT PRIMARY KEY")]
for table, key in TABLES:
    t.statement(f"CREATE TABLE {table} ({key}, {columns}) "
                "WITH (compaction_interval = 0, row_group_size = 2048)")
    t.statement(f"INSERT INTO {table} SELECT i, {exprs} FROM range(0, {FLUSHED}) r(i)")
    t.statement(f"VACUUM (FLUSH_TABLE) {table}")
    t.statement(f"INSERT INTO {table} SELECT i, {exprs} FROM range({FLUSHED}, {ROWS}) r(i)")

CHECKS = [(name, to_value, constant_sql, constant)
          for name, _, _, to_value, constant_sql, constant in COLUMNS]
CHECKS.append(("c_bool", COLUMNS[0][3], "false", False))

for table, _ in TABLES:
    for name, to_value, constant_sql, constant in CHECKS:
        t.section(f"{table}.{name} against {constant_sql}.")
        column = (name, None, None, to_value)
        for op_sql, op in OPS:
            hits = [i for i in range(ROWS)
                    if value(column, i) is not None and op(value(column, i), constant)]
            t.expect(f"SELECT count(*) AS n FROM {table} WHERE {name} {op_sql} {constant_sql}",
                     ["n"], [[len(hits)]])
            t.expect(f"SELECT count({name}) AS n, sum(i) AS s FROM {table} "
                     f"WHERE {name} {op_sql} {constant_sql}",
                     ["n", "s"], [[len(hits), sum(hits) if hits else "NULL"]])
            narrowed = [i for i in hits if i % 3 != 0]
            t.expect(f"SELECT count({name}) AS n FROM {table} "
                     f"WHERE i % 3 <> 0 AND {name} {op_sql} {constant_sql}",
                     ["n"], [[len(narrowed)]])
t.write()
