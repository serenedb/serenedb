from sqllogic_gen import Test

t = Test("DECIMAL, UUID, UBIGINT, HUGEINT and UHUGEINT columns, and bounds an "
         "INTEGER column can't hold exactly, through an inverted index and a "
         "search table, against the same predicate on the table itself: "
         "mismatches is always 0.", seed=17)

D16_MAX = "9" * 30 + "." + "9" * 5
UUIDS = ["'00000000-0000-0000-0000-000000000000'::UUID",
         "'00000000-0000-0000-0000-000000000001'::UUID",
         "'7fffffff-ffff-ffff-ffff-ffffffffffff'::UUID",
         "'80000000-0000-0000-0000-000000000000'::UUID",
         "'c0ffee00-0000-4000-8000-000000000000'::UUID",
         "'ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID"]

# column: (type, stored values, constants of the predicates)
COLUMNS = {
    "i": ("INTEGER",
          ["-2147483648", "-7", "0", "4", "5", "6", "7", "2147483647"],
          ["5", "5.5", "-6.5", "4.4999", "2147483647.5", "3000000000",
           "-3000000000"]),
    "d2": ("DECIMAL(4,1)",
           ["-999.9", "-12.5", "0.0", "5.5", "99.9", "999.9"],
           ["5.5", "5.55", "-12.45", "0", "1000.0", "-1000.0"]),
    "d4": ("DECIMAL(9,2)",
           ["-9999999.99", "-5.12", "0.00", "1.25", "5.12", "5.13",
            "9999999.99"],
           ["5.12", "5.125", "-5.125", "1.249", "10000000", "-10000000.5"]),
    "d8": ("DECIMAL(18,3)",
           ["-999999999999999.999", "-1.001", "0.000", "5.123", "9999.999",
            "999999999999999.999"],
           ["5.123", "5.1235", "-1.0015", "0.0001", "1000000000000000"]),
    "d16": ("DECIMAL(38,5)",
            [f"-{D16_MAX}", "-0.00001", "0.00000",
             "12345678901234567890.12345", D16_MAX],
            ["0", "-0.000005", "12345678901234567890.12345",
             "12345678901234567890.123455", "10000000000"]),
    "u": ("UUID", UUIDS,
          [UUIDS[0], UUIDS[2], UUIDS[3], UUIDS[5]]),
    "ub": ("UBIGINT",
           ["0", "1", "9223372036854775807", "9223372036854775808",
            "18446744073709551615"],
           ["0", "9223372036854775808", "9223372036854775807.5", "-1",
            "18446744073709551616"]),
    "h": ("HUGEINT",
          ["-170141183460469231731687303715884105728", "-1", "0", "1",
           "170141183460469231731687303715884105727"],
          ["-1", "0", "0.5e0", "-0.5e0", "1.8e38", "-1.8e38"]),
    "uh": ("UHUGEINT",
           ["0", "1", "170141183460469231731687303715884105727",
            "170141183460469231731687303715884105728",
            "340282366920938463463374607431768211455"],
           ["1::UHUGEINT", "170141183460469231731687303715884105728::UHUGEINT",
            "1.5e0", "-1e10", "3.5e38"]),
}
LISTS = {"ld": "d4", "lu": "u", "lh": "h", "lub": "ub"}
LIST_RANGES = {"ld": ("5.12", "5.13"), "lh": ("-1", "0"),
               "lub": ("9223372036854775808", "18446744073709551615")}
ROWS = 48


def scalar(column, row):
    _, values, _ = COLUMNS[column]
    if (row + len(column)) % 9 == 0:
        return "NULL"
    return values[(row * 7 + len(column)) % len(values)]


def array(column, row):
    if row % 11 == 0:
        return "NULL"
    _, values, _ = COLUMNS[LISTS[column]]
    items = [values[(row * (k + 3)) % len(values)] for k in range(row % 4)]
    if row % 13 == 0:
        items.append("NULL")
    cast = COLUMNS[LISTS[column]][0]
    return f"[{', '.join(items)}]::{cast}[]"


def scalar_predicates(column):
    _, _, constants = COLUMNS[column]
    preds = []
    for c in constants:
        for op in ("=", "<>", "<", "<=", ">", ">="):
            preds.append((f"{column} {op} {c}", f"{column} {op} {c}"))
        preds.append((f"NOT ({column} = {c})", f"NOT ({column} = {c})"))
        for fn, op in (("ts_lt", "<"), ("ts_le", "<="), ("ts_gt", ">"),
                       ("ts_ge", ">=")):
            preds.append((f"{column} @@ {fn}({c})", f"{column} {op} {c}"))
    lo, hi = constants[0], constants[1]
    preds += [
        (f"{column} IN ({', '.join(constants)})",
         f"{column} IN ({', '.join(constants)})"),
        (f"{column} NOT IN ({lo}, {hi})", f"{column} NOT IN ({lo}, {hi})"),
        (f"{column} BETWEEN {lo} AND {hi}", f"{column} BETWEEN {lo} AND {hi}"),
        (f"{column} NOT BETWEEN {lo} AND {hi}",
         f"{column} NOT BETWEEN {lo} AND {hi}"),
        (f"{column} IS NULL", f"{column} IS NULL"),
        (f"{column} IS NOT NULL", f"{column} IS NOT NULL"),
    ]
    return preds


def list_predicates(column):
    _, _, constants = COLUMNS[LISTS[column]]
    preds = []
    for c in constants:
        for fn, op in (("ts_le", "<="), ("ts_gt", ">")):
            preds.append((f"{column} @@ {fn}({c})",
                          f"len(list_filter({column}, lambda x: x {op} {c}))"
                          " > 0"))
    if column in LIST_RANGES:
        lo, hi = LIST_RANGES[column]
        preds.append((f"{column} @@ ts_between({lo}, {lo}, true, true)",
                      f"len(list_filter({column}, lambda x: x = {lo})) > 0"))
        preds.append((f"{column} @@ ts_between({lo}, {hi}, true, false)",
                      f"len(list_filter({column}, lambda x: x >= {lo} AND "
                      f"x < {hi})) > 0"))
    return preds


columns = ", ".join(f"{name} {spec[0]}" for name, spec in COLUMNS.items())
lists = ", ".join(f"{name} {COLUMNS[child][0]}[]"
                  for name, child in LISTS.items())
definition = f"id INTEGER PRIMARY KEY, {columns}, {lists}"
keys = ", ".join(["id", *COLUMNS, *LISTS])
rows = []
for row in range(ROWS):
    values = [scalar(name, row) for name in COLUMNS]
    values += [array(name, row) for name in LISTS]
    rows.append(f"({row}, {', '.join(values)})")

t.table("wnm", definition)
t.table("wnm_s", definition, search=True)
t.index("wnm_idx", "wnm", keys)
t.index("wnm_s_idx", "wnm_s", keys)
t.insert("wnm", rows)
t.statement("INSERT INTO wnm_s SELECT * FROM wnm")
t.refresh("wnm")
t.refresh("wnm_s")

for column in COLUMNS:
    t.section(f"{column} {COLUMNS[column][0]}")
    for pred, truth in scalar_predicates(column):
        expected = f"SELECT id FROM wnm WHERE {truth}"
        t.same_rows(f"SELECT id FROM wnm_idx WHERE {pred}", expected)
        t.same_rows(f"SELECT id FROM wnm_s WHERE {pred}", expected)
    _, _, constants = COLUMNS[column]
    for c in constants[:2]:
        t.same_rows(f"SELECT count(*) FROM wnm_s WHERE {column} <= {c}",
                    f"SELECT count(*) FROM wnm WHERE {column} <= {c}")

for column in LISTS:
    t.section(f"{column} {COLUMNS[LISTS[column]][0]}[]")
    for pred, truth in list_predicates(column):
        expected = f"SELECT id FROM wnm WHERE {truth}"
        t.same_rows(f"SELECT id FROM wnm_idx WHERE {pred}", expected)
        t.same_rows(f"SELECT id FROM wnm_s WHERE {pred}", expected)

t.section("The plan prints each bound as a value of the column type.")
for pred in ("d4 = 5.12", "d4 @@ ts_lt(5.125)", "d16 = 0",
             f"u = {UUIDS[3]}", "ub > 9223372036854775807", "h = -1",
             "uh @@ ts_ge(1.5)", "i @@ ts_le(5.5)"):
    t.query(f"EXPLAIN SELECT id FROM wnm_s WHERE {pred}")
t.write()
