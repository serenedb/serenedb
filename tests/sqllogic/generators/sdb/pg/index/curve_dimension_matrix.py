from sqllogic_gen import Test

t = Test("Two to eight coordinates at every kind of depth, in both curves, with "
         "boxes bounded on any subset of the axes, against the same conditions "
         "on the table: mismatches is always 0.", seed=256)
rng = t.rng

ROWS = 500
BOXES = 10
MIXED = ["TINYINT", "BIGINT", "DOUBLE", "DATE", "TIMESTAMP", "INET", "UBIGINT",
         "FLOAT"]


def small(type_name):
    if type_name in ("BIGINT", "INTEGER", "TINYINT"):
        return str(rng.randrange(-4, 8))
    if type_name == "UBIGINT":
        return str(rng.randrange(8))
    if type_name in ("DOUBLE", "FLOAT"):
        return str(rng.randrange(-8, 16) / 2)
    if type_name == "DATE":
        return f"'2024-03-{rng.randrange(1, 9):02}'::DATE"
    if type_name == "TIMESTAMP":
        return f"'2024-03-01 12:00:{rng.randrange(8):02}'::TIMESTAMP"
    return f"'10.0.{rng.randrange(4)}.{rng.randrange(8)}'::INET"


def wide(type_name):
    if type_name == "TINYINT":
        return rng.choice(["-128", "127"])
    if type_name == "BIGINT":
        return rng.choice(["'-9223372036854775808'::BIGINT",
                           "'9223372036854775807'::BIGINT"])
    if type_name == "UBIGINT":
        return "'18446744073709551615'::UBIGINT"
    if type_name in ("DOUBLE", "FLOAT"):
        return f"'{rng.choice(['nan', 'inf', '-inf', '1e30', '-1e30'])}'::{type_name}"
    if type_name == "DATE":
        return rng.choice(["'infinity'::DATE", "'-infinity'::DATE"])
    if type_name == "TIMESTAMP":
        return rng.choice(["'infinity'::TIMESTAMP", "'-infinity'::TIMESTAMP"])
    return rng.choice(["'::1'::INET", "'2001:db8::1/64'::INET"])


def coordinate(type_name):
    kind = rng.randrange(24)
    if kind == 0:
        return "NULL"
    return wide(type_name) if kind <= 2 else small(type_name)


def side(type_name):
    kind = rng.randrange(8)
    first, second = small(type_name), small(type_name)
    if kind <= 2:
        return None, None
    if kind == 3:
        return first, first
    if kind == 4:
        return first, None
    if kind == 5:
        return None, first
    return first, second


def comparison(column, low, high, left, right):
    parts = []
    if low:
        parts.append(f"{column} {'>=' if left else '>'} {low}")
    if high:
        parts.append(f"{column} {'<=' if right else '<'} {high}")
    return parts


def check(table, index, call, sides, left, right):
    columns = [f"c{axis}" for axis in range(len(sides))]
    lower = ", ".join(low or "NULL" for low, _ in sides)
    upper = ", ".join(high or "NULL" for _, high in sides)
    ranges = []
    for column, (low, high) in zip(columns, sides):
        ranges += comparison(column, low, high, left, right)
    truth = " AND ".join([f"{c} IS NOT NULL" for c in columns] + ranges)
    t.same_rows(f"SELECT id FROM {index} WHERE {call} @@ ts_between(ROW({lower}), "
                f"ROW({upper}), {str(left).lower()}, {str(right).lower()})",
                f"SELECT id FROM {table} WHERE {truth}")
    if all(low and high for low, high in sides):
        condition = " AND ".join(ranges)
        t.same_rows(f"SELECT id FROM {index} WHERE {condition}",
                    f"SELECT id FROM {table} WHERE {condition}")


def run(number, types, duplicates):
    dimensions = len(types)
    table = f"cdm_{number}"
    columns = [f"c{axis}" for axis in range(dimensions)]
    point = f"ROW({', '.join(columns)})"
    deepest = min(64, 256 // dimensions)
    t.section(f"{dimensions} coordinates: {', '.join(types)}.")
    t.table(table, "id INTEGER PRIMARY KEY, " + ", ".join(
        f"{column} {type_name}" for column, type_name in zip(columns, types)))
    points = []
    for i in range(ROWS):
        if points and rng.randrange(100) < duplicates:
            points.append(rng.choice(points))
        else:
            points.append([coordinate(type_name) for type_name in types])
    t.insert(table, (f"({i}, {', '.join(p)})" for i, p in enumerate(points)))
    calls = {
        "default": f"curve({point})",
        "hilbert": f"curve({point}, kind := 'hilbert')",
        "level1": f"curve({point}, max_level := 1)",
        "level2": f"curve({point}, kind := 'hilbert', max_level := 2)",
        "level3": f"curve({point}, max_level := 3)",
        "deepest": f"curve({point}, max_level := {deepest})",
        "deepest_hilbert": f"curve({point}, kind := 'hilbert', max_level := {deepest})",
    }
    for name, call in calls.items():
        t.index(f"{table}_{name}", table, f"id, ({call})")
    t.refresh(table)
    for name, call in calls.items():
        index = f"{table}_{name}"
        t.same_rows(f"SELECT id FROM {index} WHERE {call} @@ ts_between(NULL, NULL, true, true)",
                    f"SELECT id FROM {table} WHERE " + " AND ".join(
                        f"{c} IS NOT NULL" for c in columns))
        for _ in range(BOXES):
            sides = [side(type_name) for type_name in types]
            left, right = rng.choice([True, True, False]), rng.choice([True, True, False])
            check(table, index, call, sides, left, right)


number = 0
for dimensions in range(2, 9):
    t.part()
    for types, duplicates in (([MIXED[axis] for axis in range(dimensions)], 0),
                              (["BIGINT"] * dimensions, 30),
                              (["DOUBLE"] * dimensions, 60)):
        run(number, types, duplicates)
        number += 1

t.write()
