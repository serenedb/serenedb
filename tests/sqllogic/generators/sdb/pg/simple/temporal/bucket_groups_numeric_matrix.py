from sqllogic_gen import Test
from temporal_gen import diff, named

t = Test("Numeric buckets (floor, round, integer division, logarithms) over every "
         "numeric type, negative values, NaN and infinities, and date buckets "
         "turned into numbers, text and other temporal types: every grouping "
         "returns the same keys, counts and sums with compressed materialization "
         "off and on.")

NUMERIC = {
    "num": (
        "floor(d / 100) * 100", "floor(d / 100)", "x // 100", "x // 100 * 100", "x / 100", "x / 100 * 100",
        "(x / 100)::INTEGER", "round(d, -2)", "round(d, -1)", "round(d)", "round(d / 100) * 100",
        "trunc(neg / 7.0)", "trunc(neg / 7.0) * 7", "ceil(dec)", "ceil(dec / 3)", "floor(dec * 10)",
        "round(dec, -1)", "floor(d / 100.0)::DECIMAL(10,2)", "-x // 10", "-(x // 10)", "floor(log10(x + 1))",
        "floor(sqrt(x))", "floor(ln(x + 1))", "floor(log2(x + 1))", "floor(exp(x / 5000.0))", "floor(cbrt(x))",
        "sign(neg)", "floor(s16 / 1000.0)", "t8 // 10", "u8 // 25", "u32 // 5000", "big // 100000000",
        "huge // 7000000049", "floor(f / 100)", "floor(f) // 100", "floor(d / 100) * 100 + 50",
        "floor(d / 100) * 100 - 50", "100 - floor(d / 100)", "floor(d / 100) / 10.0", "floor(d / 100) * 0.1",
        "(floor(d / 100) * 100)::INTEGER", "(floor(d / 100) * 100)::VARCHAR", "'bin ' || (x // 100)::VARCHAR",
        "'[' || (floor(d / 100) * 100)::INTEGER::VARCHAR || ')'", "floor(d / 100)::FLOAT",
        "floor(d / 100)::HUGEINT", "floor(d / 100)::DECIMAL(18,3) * 2", "(x // 100)::DOUBLE / 2",
        "concat('b', x // 100)", "round(d, 1)", "floor(d) % 10", "abs(neg) // 10", "x * 2", "x + 1",
        "floor(d) + floor(d / 2)", "floor(sqrt(x)) * floor(sqrt(x))",
    ),
    "numx": (
        "floor(d / 100) * 100", "floor(d)", "round(d, -1)", "big // 1000000007", "trunc(d / 50)",
        "floor(big / 1e12)", "ceil(dec * 3)", "round(dec, -1) / 10", "(big / 1000)::INTEGER", "big // 3 // 5 // 7",
    ),
    "nan": ("floor(d / 100) * 100", "floor(d)", "round(d, -1)", "trunc(d)", "d // 100"),
}
DATES = (
    "date_trunc('hour', ts) + INTERVAL '30 minutes'", "date_trunc('day', ts) + INTERVAL '6 hours'",
    "date_trunc('day', ts) - INTERVAL '1 day'", "date_trunc('week', ts) + INTERVAL '3 days'",
    "date_trunc('month', ts) + INTERVAL '1 month'", "date_trunc('hour', ts) + INTERVAL '1 month'",
    "epoch_ms(date_trunc('hour', ts))", "epoch_us(date_trunc('hour', ts))", "epoch_ns(date_trunc('minute', ts))",
    "epoch(date_trunc('minute', ts))", "epoch(date_trunc('hour', ts)) / 3600", "epoch(date_trunc('hour', ts)) / 3600.0",
    "epoch_ms(date_trunc('hour', ts)) / 1000", "(epoch_ms(date_trunc('hour', ts)) / 1000)::BIGINT",
    "epoch_ms(date_trunc('hour', ts)) // 3600000", "epoch_ms(date_trunc('hour', ts)) * 2",
    "epoch(date_trunc('day', ts)) / 86400", "epoch_us(time_bucket(INTERVAL '15 minutes', ts))",
    "date_trunc('hour', ts)::VARCHAR", "'h=' || date_trunc('hour', ts)::VARCHAR", "date_trunc('week', ts)::DATE",
    "date_trunc('hour', ts)::DATE", "date_trunc('day', ts)::DATE + 1", "(date_trunc('day', ts)::DATE + 7)::TIMESTAMP",
    "time_bucket(INTERVAL '2 hours', ts)::TIMESTAMP_NS", "date_trunc('hour', ts)::TIMESTAMP_MS",
    "date_trunc('minute', ts)::TIMESTAMP_S", "date_trunc('minute', ts)::TIMESTAMP_MS::VARCHAR",
    "strftime(ts, '%Y-%m') || '!'", "'<' || strftime(ts, '%Y-%m-%d') || '>'", "monthname(ts) || '-'",
    "dayname(ts) || '!'", "concat(dayname(ts), ' ', monthname(ts))", "last_day(ts)::TIMESTAMP", "last_day(ts) + 1",
    "last_day(ts)::VARCHAR", "epoch_ms(date_trunc('hour', tstz))", "epoch(date_trunc('day', tstz))",
    "date_trunc('day', tstz)::TIMESTAMP", "date_trunc('hour', tstz)::TIMESTAMP", "date_trunc('day', tstz)::DATE",
    "date_trunc('day', tstz) + INTERVAL '12 hours'", "date_trunc('hour', tstz) + INTERVAL '30 minutes'",
    "date_trunc('hour', tstz)::VARCHAR", "time_bucket(INTERVAL '1 day', tstz, 'Asia/Tokyo') + INTERVAL '1 hour'",
    "(date_trunc('hour', ts) + INTERVAL '30 minutes')::VARCHAR", "concat('d', date_trunc('day', ts)::DATE)",
    "date_trunc('month', dt)::VARCHAR", "date_trunc('week', dt) + INTERVAL '1 day'",
    "time_bucket(INTERVAL '7 days', dt)::VARCHAR", "date_trunc('hour', ts)::TIMESTAMPTZ",
    "date_trunc('day', ts)::TIMESTAMPTZ", "date_trunc('day', ts)::TIMESTAMPTZ::VARCHAR",
    "TRY_CAST(date_trunc('day', ts) AS DATE)", "date_trunc('hour', ts) + INTERVAL '30 minutes' + INTERVAL '15 minutes'",
    "strftime(date_trunc('hour', ts), '%H')", "-epoch(date_trunc('hour', ts))",
)

t.statement("SET TimeZone = 'Europe/Berlin';")
t.statement("CREATE TABLE num AS SELECT i AS x, (i * 1.5)::DOUBLE AS d, (i * 0.25)::FLOAT AS f, "
            "((i % 1000)::DECIMAL(9,2) / 7)::DECIMAL(12,4) AS dec, (i % 200 - 100)::TINYINT AS t8, "
            "(i % 20000 - 10000)::SMALLINT AS s16, (i * 1000003)::BIGINT AS big, (i * 7)::HUGEINT * 1000000007 "
            "AS huge, (i % 200)::UTINYINT AS u8, (i * 3)::UINTEGER AS u32, TIMESTAMP '2025-01-01' + INTERVAL (i) "
            "MINUTE AS ts, (TIMESTAMP '2025-03-29 00:00:00' + INTERVAL (i) MINUTE)::TIMESTAMPTZ AS tstz, "
            "(DATE '2025-01-01' + (i % 500)::INTEGER)::DATE AS dt, (i % 300 - 100)::INTEGER AS neg, i AS v "
            "FROM range(20000) t(i);")
t.statement("INSERT INTO num VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
            "NULL, -1);")
t.statement("CREATE TABLE numx AS SELECT (i - 5000) * 0.37 AS d, (i - 5000)::BIGINT * 1000003 AS big, "
            "((i - 5000) * 0.01)::DECIMAL(10,2) AS dec, i AS v FROM range(10000) t(i);")
t.statement("CREATE TABLE nan AS SELECT d, v FROM numx UNION ALL SELECT 'nan'::DOUBLE, -1 UNION ALL SELECT "
            "'inf'::DOUBLE, -2 UNION ALL SELECT '-inf'::DOUBLE, -3;")
diff(t, "num", [named(table, expr, table) for table, exprs in NUMERIC.items() for expr in exprs],
     "compressed_materialization", ["shape", "k", "c", "s"])
diff(t, "dt", [named("dt", expr, "num") for expr in DATES], "compressed_materialization", ["shape", "k", "c", "s"])
t.statement("SET error_on_division_by_zero = false;")
t.query("SELECT count(*) AS c, count(b) AS nn FROM (SELECT x // 0 AS b, count(*) AS c FROM num GROUP BY 1);",
        ["c", "nn"])
t.statement("RESET error_on_division_by_zero;")
t.query("SELECT count(*) AS c FROM (SELECT floor(d / 0) AS b, count(*) AS c FROM num GROUP BY 1);", ["c"])

t.write()
