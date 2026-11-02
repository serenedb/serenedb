from sqllogic_gen import Test

GRIDS = [
    ("(VALUES (1, to_date('0001-01-01 BC', 'YYYY-MM-DD BC')), (2, to_date('0002-06-15 BC', 'YYYY-MM-DD BC')), "
     "(3, to_date('1001-03-10 BC', 'YYYY-MM-DD BC')), (4, DATE '2021-01-04'), (5, DATE '2000-12-31'), "
     "(6, DATE '0001-01-01')) v(id, value)", 6,
     ["year", "decade", "century", "millennium", "isoyear", "isodow", "dow"]),
    ("(VALUES (1, to_timestamp('0001-01-01 12:00 BC', 'YYYY-MM-DD HH24:MI BC')::timestamp), "
     "(2, TIMESTAMP '2021-01-04 12:00:00')) v(id, value)", 2,
     ["year", "decade", "century", "millennium", "isoyear", "isodow"]),
]

UNITS = [
    ("DATE '2021-01-03'", ["year", "doy", "epoch"]),
    ("TIME '10:11:12.5'", ["hour", "second", "epoch"]),
    ("TIMETZ '10:11:12.5+02'", ["hour", "timezone", "epoch"]),
    ("TIMESTAMP '2021-01-03 10:11:12.5'", ["year", "millisecond", "epoch"]),
    ("TIMESTAMPTZ '2021-01-03 10:11:12.5+00'", ["year", "timezone", "epoch"]),
    ("INTERVAL '1 year 2 months 3 days 04:05:06.5'", ["year", "month", "epoch"]),
]

INTERVALS = [
    "pg_catalog.extract('millisecond', INTERVAL '1 minute 2.5 seconds')",
    "pg_catalog.extract('minute', INTERVAL '1 hour 2 minutes')",
    "pg_catalog.extract('decade', INTERVAL '25 years')",
    "pg_catalog.extract('millennium', INTERVAL '2500 years')",
    "pg_catalog.extract('decade', INTERVAL '-25 years')",
]

SOURCES = [("year", "DATE '2021-01-03'"), ("hour", "TIME '10:11:12'"), ("hour", "TIMETZ '10:11:12+02'"),
           ("year", "TIMESTAMP '2021-01-03 10:11:12'"), ("year", "TIMESTAMPTZ '2021-01-03 10:11:12+00'"),
           ("year", "INTERVAL '1 day'")]
NULLS = ([f"pg_catalog.extract(NULL::text, {source})" for _, source in SOURCES] +
         [f"pg_catalog.extract(NULLIF('{unit}', '{unit}'), {source})" for unit, source in SOURCES])

REJECTED = [
    "pg_catalog.extract('microsecond', DATE '2021-01-03')",
    "pg_catalog.extract('millisecond', DATE '2021-01-03')",
    "pg_catalog.extract('second', DATE '2021-01-03')",
    "pg_catalog.extract('minute', DATE '2021-01-03')",
    "pg_catalog.extract('timezone_hour', DATE '2021-01-03')",
    "pg_catalog.extract('timezone_minute', TIMESTAMP '2021-01-03 01:02:03')",
    "pg_catalog.extract('now', TIMETZ '10:11:12+01')",
    "pg_catalog.extract('fortnight', TIMETZ '10:11:12+01')",
    "pg_catalog.extract('now', TIMESTAMP 'infinity')",
    "pg_catalog.extract('fortnight', TIMESTAMP '2021-01-03 01:02:03')",
    "pg_catalog.extract('fortnight', TIMESTAMPTZ '2021-01-03 01:02:03+00')",
    "pg_catalog.extract('fortnight', TIMESTAMPTZ 'infinity')",
    "pg_catalog.extract('now', TIMESTAMPTZ 'infinity')",
    "pg_catalog.extract('now', TIMESTAMPTZ '2021-01-03 01:02:03+00')",
    "pg_catalog.extract('now', INTERVAL '1 day')",
] + [f"pg_catalog.extract('{unit}', INTERVAL '1 day')" for unit in (
    "julian", "isoyear", "isodow", "doy", "timezone", "timezone_hour", "timezone_minute")] + [
    "pg_catalog.extract(repeat('a', 62) || 'éb', DATE '2021-01-03')",
    "pg_catalog.extract(repeat('a', 70), DATE '2021-01-03')",
]

t = Test("pg_catalog.extract, PostgreSQL's numeric extract: each unit of a type over a list of "
         "values, the units each type takes and rejects, NULL units, and offsets in other time "
         "zones, every answer recorded from PostgreSQL.")
t.statement("SET TimeZone = 'UTC'")
t.section("Each unit over a list of values.")
for values, rows, units in GRIDS:
    for unit in units:
        t.query(f"SELECT id, pg_catalog.extract('{unit}', value) AS a FROM {values} ORDER BY id",
                ["id", "a"], rows)
for source, units in UNITS:
    names = ", ".join(f"('{unit}')" for unit in units)
    t.query(f"SELECT u, pg_catalog.extract(u, {source})::float8 AS a FROM (VALUES {names}) v(u) "
            "ORDER BY u", ["u", "a"], len(units))
for expression in INTERVALS:
    t.query(f"SELECT {expression} AS a", ["a"])
t.section("NULL units and units from a parameter.")
for expression in NULLS:
    t.query(f"SELECT {expression} AS a", ["a"])
t.statement("PREPARE extract_unit(text) AS SELECT pg_catalog.extract($1, TIMESTAMP '2021-01-03 10:11:12.5') AS a")
t.query("EXECUTE extract_unit(NULL)", ["a"])
t.query("EXECUTE extract_unit('second')", ["a"])
t.section("Units a type rejects.")
for expression in REJECTED:
    t.query(f"SELECT {expression} AS a", ["a"])
t.section("Offsets in other time zones.")
t.statement("SET TimeZone = 'America/New_York'")
for unit in ("hour", "timezone"):
    t.query(f"SELECT id, pg_catalog.extract('{unit}', t) AS a FROM (VALUES "
            "(1, TIMESTAMPTZ '1800-01-01 00:00:00.0005+00'), (2, TIMESTAMPTZ '2400-07-01 00:00:00+00')) v(id, t) "
            "ORDER BY id", ["id", "a"], 2)
t.statement("SET TimeZone = 'Etc/GMT-1'")
t.query("SELECT pg_catalog.extract('timezone', TIMESTAMPTZ '294247-01-10 04:00:54.775806+00') AS a", ["a"])
t.statement("SET TimeZone = 'UTC'")
t.write()
