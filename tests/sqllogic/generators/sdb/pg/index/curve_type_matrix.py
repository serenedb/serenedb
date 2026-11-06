from sqllogic_gen import Test

t = Test("Boxes and ordinary ranges on every coordinate type and on pairs of "
         "types, at the limits of the types and with bounds of other types, "
         "against the same conditions on the table: mismatches is always 0.",
         seed=2026)
rng = t.rng

BOXES = 16
ROWS = 400


def literal(value, type_name):
    return f"'{value}'::{type_name}"


def integers(type_name, bits, signed):
    low = -(1 << (bits - 1)) if signed else 0
    high = (1 << (bits - 1)) - 1 if signed else (1 << bits) - 1
    near = {low, low + 1, low + 2, high - 2, high - 1, high, 0, 1, 2, 3}
    if signed:
        near |= {-1, -2, -3}
    near |= {rng.randint(low, high) for _ in range(6)}
    near |= {rng.randint(max(low, -50), min(high, 50)) for _ in range(12)}
    return [literal(value, type_name) for value in sorted(near)]


def listed(type_name, values):
    return [literal(value, type_name) for value in values]


FLOATS = ["-inf", "-3.4028235e38", "-1e10", "-1.5", "-1", "-0.1", "-1e-45",
          "-0.0", "0", "1e-45", "1.17549435e-38", "0.1", "1", "1.5", "2",
          "16777216", "16777217", "33554434", "1e10", "3.4028235e38", "inf",
          "nan"]
DOUBLES = ["-inf", "-1.7976931348623157e308", "-1e300", "-1.5", "-1", "-0.1",
           "-5e-324", "-0.0", "0", "5e-324", "2.2250738585072014e-308", "0.1",
           "1", "1.5", "2", "16777217", "9007199254740992", "9007199254740993",
           "1e300", "1.7976931348623157e308", "inf", "nan"]
DATES = ["-infinity", "0001-01-01", "1582-10-15", "1969-12-31", "1970-01-01",
         "1970-01-02", "2000-02-29", "2024-03-01", "2024-03-02", "2024-03-03",
         "2024-03-04", "2024-03-05", "9999-12-31", "infinity"]
TIMESTAMPS = ["-infinity", "0001-01-01 00:00:00", "1969-12-31 23:59:59.999999",
              "1970-01-01 00:00:00", "1970-01-01 00:00:00.000001",
              "2024-03-01 12:00:00", "2024-03-01 12:00:00.000001",
              "2024-03-01 12:00:00.000002", "2024-03-02 00:00:00",
              "9999-12-31 23:59:59.999999", "infinity"]
ZONED = ["-infinity", "1969-12-31 23:59:59.999999+00", "1970-01-01 00:00:00+00",
         "1970-01-01 03:00:00.000001+03", "2024-03-01 12:00:00+00",
         "2024-03-01 15:00:00+03", "2024-03-01 12:00:00.000001+00",
         "2024-03-02 00:00:00-05", "9999-12-31 23:59:59+00", "infinity"]
NETWORKS = ["0.0.0.0", "0.0.0.1", "10.0.0.0/8", "10.0.0.0/24", "10.0.0.1",
            "10.0.0.1/24", "10.0.0.255/24", "10.0.1.1/16", "127.0.0.1",
            "192.168.0.1/32", "255.255.255.255", "::", "::1",
            "2001:db8::/32", "2001:db8::1", "2001:db8::2/64",
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"]

VALUES = {
    "TINYINT": integers("TINYINT", 8, True),
    "SMALLINT": integers("SMALLINT", 16, True),
    "INTEGER": integers("INTEGER", 32, True),
    "BIGINT": integers("BIGINT", 64, True),
    "UTINYINT": integers("UTINYINT", 8, False),
    "USMALLINT": integers("USMALLINT", 16, False),
    "UINTEGER": integers("UINTEGER", 32, False),
    "UBIGINT": integers("UBIGINT", 64, False),
    "FLOAT": listed("FLOAT", FLOATS),
    "DOUBLE": listed("DOUBLE", DOUBLES),
    "DATE": listed("DATE", DATES),
    "TIMESTAMP": listed("TIMESTAMP", TIMESTAMPS),
    "TIMESTAMPTZ": listed("TIMESTAMPTZ", ZONED),
    "INET": listed("INET", NETWORKS),
}

RANGES = {
    "TINYINT": ("-20", "20"),
    "SMALLINT": ("-20", "2000"),
    "INTEGER": ("-20", "2000000"),
    "BIGINT": ("-20", "2000000000000"),
    "UTINYINT": ("3", "200"),
    "USMALLINT": ("3", "2000"),
    "UINTEGER": ("3", "2000000"),
    "UBIGINT": ("3", "2000000000000"),
    "FLOAT": ("-10", "10"),
    "DOUBLE": ("-10", "10"),
    "DATE": ("2024-01-01", "2024-12-31"),
    "TIMESTAMP": ("2024-03-01 00:00:00", "2024-03-02 00:00:00"),
    "TIMESTAMPTZ": ("2024-03-01 00:00:00+00", "2024-03-02 00:00:00+00"),
    "INET": ("10.0.0.0", "10.255.0.0"),
}

INTEGER_BOUNDS = ["-1", "300", "-300", "70000", "-70000", "5000000000",
                  "-5000000000", "'9223372036854775807'::BIGINT",
                  "'-9223372036854775808'::BIGINT",
                  "'18446744073709551615'::UBIGINT",
                  "'9223372036854775808'::UBIGINT", "200::UTINYINT",
                  "60000::USMALLINT", "4000000000::UINTEGER",
                  "(-100)::TINYINT", "(-30000)::SMALLINT"]
FLOAT_BOUNDS = ["1", "-1", "16777217", "33554435", "9007199254740993",
                "'9223372036854775807'::BIGINT",
                "'18446744073709551615'::UBIGINT", "0.1", "-0.1", "0.1::FLOAT",
                "0.1::DOUBLE", "1.5::DECIMAL(10,2)", "0.1::DECIMAL(20,18)",
                "16777217.0::DECIMAL(20,1)", "(-0.0)::DOUBLE", "(-0.0)::FLOAT"]


def foreign_bounds(type_name):
    if type_name in ("FLOAT", "DOUBLE"):
        return FLOAT_BOUNDS
    return INTEGER_BOUNDS if "INT" in type_name else []


def pick_value(values):
    if rng.randrange(8) == 0:
        return "NULL"
    return rng.choice(values)


def pick_side(type_name):
    values = VALUES[type_name]
    first = rng.randrange(len(values))
    last = rng.randrange(first, len(values))
    if rng.randrange(4) == 0:
        last = min(first + rng.randrange(3), len(values) - 1)
    low, high = values[first], values[last]
    kind = rng.randrange(12)
    if kind == 0:
        return None, high
    if kind == 1:
        return low, None
    if kind == 2:
        return None, None
    if kind == 3:
        return high, low
    foreign = foreign_bounds(type_name)
    if foreign and kind in (4, 5):
        return rng.choice(foreign), high
    if foreign and kind in (6, 7):
        return low, rng.choice(foreign)
    return low, high


def comparison(column, low, high, left, right):
    if left and right and low and high:
        if low == high:
            return f"{column} = {low}"
        return f"{column} BETWEEN {low} AND {high}"
    parts = []
    if low:
        parts.append(f"{column} {'>=' if left else '>'} {low}")
    if high:
        parts.append(f"{column} {'<=' if right else '<'} {high}")
    return " AND ".join(parts)


def check(table, index, call, sides, left, right):
    (a_low, a_high), (b_low, b_high) = sides
    lower = f"ROW({a_low or 'NULL'}, {b_low or 'NULL'})"
    upper = f"ROW({a_high or 'NULL'}, {b_high or 'NULL'})"
    ranges = [comparison("a", a_low, a_high, left, right),
              comparison("b", b_low, b_high, left, right)]
    truth = " AND ".join(
        ["a IS NOT NULL", "b IS NOT NULL"] + [r for r in ranges if r])
    t.same_rows(f"SELECT id FROM {index} WHERE {call} @@ "
                f"ts_between({lower}, {upper}, {str(left).lower()}, {str(right).lower()})",
                f"SELECT id FROM {table} WHERE {truth}")
    if all(ranges):
        condition = " AND ".join(ranges)
        t.same_rows(f"SELECT id FROM {index} WHERE {condition}",
                    f"SELECT id FROM {table} WHERE {condition}")


def table_of(number, first, second):
    table = f"ctm_{number}"
    t.section(f"{first} and {second}.")
    t.table(table, f"id INTEGER PRIMARY KEY, a {first}, b {second}")
    specials = [(a, rng.choice(VALUES[second])) for a in VALUES[first]]
    specials += [(rng.choice(VALUES[first]), b) for b in VALUES[second]]
    points = specials + [(pick_value(VALUES[first]), pick_value(VALUES[second]))
                         for _ in range(ROWS - len(specials))]
    t.insert(table, (f"({i}, {a}, {b})" for i, (a, b) in enumerate(points)))
    low_a, high_a = (literal(v, first) for v in RANGES[first])
    low_b, high_b = (literal(v, second) for v in RANGES[second])
    calls = {
        "morton": "curve(ROW(a, b))",
        "hilbert": "curve(ROW(a, b), kind := 'hilbert')",
        "coarse": "curve(ROW(a, b), max_level := 5)",
        "normal": f"curve(ROW(a, b), lower := ROW({low_a}, {low_b}), "
                  f"upper := ROW({high_a}, {high_b}), max_level := 9)",
        "normal_hilbert": f"curve(ROW(a, b), kind := 'hilbert', "
                          f"lower := ROW({low_a}, {low_b}), "
                          f"upper := ROW({high_a}, {high_b}))",
    }
    for name, call in calls.items():
        t.index(f"{table}_{name}", table, f"id, ({call})")
    t.refresh(table)
    for name, call in calls.items():
        for _ in range(BOXES):
            sides = (pick_side(first), pick_side(second))
            left, right = rng.choice([True, True, False]), rng.choice([True, True, False])
            check(table, f"{table}_{name}", call, sides, left, right)


names = list(VALUES)
pairs = [(name, name) for name in names]
pairs += [(name, names[(i + 3) % len(names)]) for i, name in enumerate(names)]
for number, (first, second) in enumerate(pairs):
    if number % 5 == 0:
        t.part()
    table_of(number, first, second)

t.write()
