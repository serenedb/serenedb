from sqllogic_gen import Test
from temporal_gen import diff, named

t = Test("Buckets written as epoch arithmetic, date_bin with odd origins and CASE "
         "classifications, on timestamps, zoned timestamps, dates and rows before "
         "1970: every grouping returns the same keys, counts and sums with "
         "compressed materialization off and on.")

EPOCHS = (
    "floor(epoch({c}) / 3600) * 3600",
    "floor(epoch({c}) / 86400) * 86400",
    "to_timestamp(floor(epoch({c}) / 3600) * 3600)",
    "to_timestamp(floor(epoch({c}) / 900) * 900)",
    "floor(date_part('epoch', {c}) / 3600) * 3600",
    "floor(extract(epoch FROM {c}) / 300) * 300",
    "to_timestamp(floor(extract(epoch FROM {c}) / 300) * 300)",
    "epoch_ms({c}) // 3600000 * 3600000",
    "epoch_ms(epoch_ms({c}) // 3600000 * 3600000)",
    "epoch_us({c}) // 3600000000",
    "make_timestamp(epoch_us({c}) // 3600000000 * 3600000000)",
    "to_timestamp(floor(epoch({c}) / 3600) * 3600 + 1800)",
    "to_timestamp(floor(epoch({c}) / 604800) * 604800)::DATE",
    "to_timestamp(floor(epoch({c}) / 86400) * 86400)::DATE",
    "floor(epoch({c}) / 3600)",
    "ceil(epoch({c}) / 3600)",
    "round(epoch_ms({c}) / 3600000)",
    "(epoch({c}) / 3600)::BIGINT",
    "floor(epoch({c}) / 3600) * 3600 / 60",
    "epoch({c}) // 3600",
    "floor(epoch({c}) / 1.5)",
)
OLD = (
    "floor(epoch(ts) / 3600) * 3600",
    "to_timestamp(floor(epoch(ts) / 3600) * 3600)",
    "epoch_ms(ts) // 86400000 * 86400000",
    "make_timestamp(epoch_us(ts) // 3600000000 * 3600000000)",
)
BINS = (
    "date_bin(INTERVAL '15 minutes', ts, TIMESTAMP '2001-01-01 00:05')",
    "date_bin(INTERVAL '15 minutes', ts, TIMESTAMP '2001-01-01')",
    "date_bin(INTERVAL '1 day', ts, TIMESTAMP '2000-01-01 06:00')",
    "date_bin(INTERVAL '90 minutes', ts, TIMESTAMP '2030-01-01')",
    "date_bin(INTERVAL '1 hour', ts, TIMESTAMP '1999-12-31 23:30')",
    "date_bin(INTERVAL '3 days', ts, TIMESTAMP '2024-12-01')",
    "date_bin(INTERVAL '15 minutes', ts, TIMESTAMP '2001-01-01')::VARCHAR",
    "date_bin(INTERVAL '15 minutes', ts, TIMESTAMP '2001-01-01') + INTERVAL '5 minutes'",
    "epoch_ms(date_bin(INTERVAL '1 hour', ts, TIMESTAMP '2000-01-01'))",
)
CASES = (
    "CASE WHEN price < 100 THEN 'low' WHEN price < 500 THEN 'mid' ELSE 'high' END",
    "CASE WHEN price < 100 THEN 'low' WHEN price < 500 THEN 'mid' END",
    "CASE WHEN price < 100 THEN 'a' WHEN price < 500 THEN 'b' ELSE 'a' END",
    "CASE WHEN service = 'checkout' THEN 1 WHEN sev > 2 THEN 2 ELSE 3 END",
    "CASE WHEN price < 100 THEN 'low' ELSE NULL END",
    "CASE WHEN price IS NULL THEN 'none' WHEN price < 100 THEN 'low' ELSE 'high' END",
    "CASE WHEN delta < 0 THEN -1 WHEN delta = 0 THEN 0 ELSE 1 END",
    "CASE WHEN delta < -100 THEN -100.5 WHEN delta < 100 THEN 0.5 ELSE 100.5 END",
    "CASE WHEN price < 100 THEN DATE '2020-01-01' ELSE DATE '2021-01-01' END",
    "CASE WHEN price < 100 THEN TRUE ELSE FALSE END",
    "CASE sev WHEN 0 THEN 'zero' WHEN 1 THEN 'one' ELSE 'many' END",
    "'bin:' || CASE WHEN price < 100 THEN 'low' ELSE 'high' END",
    "(CASE WHEN price < 100 THEN 1 ELSE 2 END) * 10",
    "CASE WHEN price < 100 THEN price ELSE 0 END",
    "CASE WHEN price < 100 THEN 'low' ELSE service END",
    "CASE WHEN ts < TIMESTAMP '2025-01-15' THEN 'early' WHEN ts < TIMESTAMP '2025-02-15' THEN 'mid' ELSE 'late' END",
    "CASE WHEN date_trunc('day', ts) = DATE '2025-01-01' THEN 'newyear' ELSE 'other' END",
)
CASTS = (
    ("ts", "epoch_ms(ts::TIMESTAMP_MS) // 3600000"),
    ("ts", "epoch_us(ts::TIMESTAMP_NS) // 3600000000"),
    ("ts", "floor(epoch(ts::TIMESTAMP_S) / 3600) * 3600"),
    ("ts", "floor(date_part('epoch', ts::TIMESTAMP_MS) / 3600) * 3600"),
    ("ts", "epoch_ms(ts::DATE) // 86400000"),
    ("ts", "epoch_ms(ts::TIMESTAMP_S::TIMESTAMP_MS) // 3600000"),
    ("ts", "epoch_ms(ts::TIMESTAMPTZ) // 3600000"),
    ("d", "epoch_ms(d::TIMESTAMP) // 86400000"),
    ("d", "floor(epoch(d::TIMESTAMP_S) / 86400) * 86400"),
    ("tstz", "epoch_ms(tstz::TIMESTAMP) // 3600000"),
)

shapes = [named(c, expr.format(c=c), "g") for c in ("ts", "tstz", "d") for expr in EPOCHS]
shapes += [named("old", expr, "old") for expr in OLD]
shapes += [named(prefix, expr, table) for expr in BINS for prefix, table in (("bin", "g"), ("old bin", "old"))]
shapes += [named("case", expr, "g") for expr in CASES]
shapes += [named(c, expr, "g") for c, expr in CASTS]

t.statement("SET TimeZone = 'Europe/Berlin';")
t.statement("CREATE TABLE g AS SELECT TIMESTAMP '2024-12-28 00:00:00' + INTERVAL (i * 7) MINUTE AS ts, "
            "(TIMESTAMP '2024-12-28 00:00:00' + INTERVAL (i * 7) MINUTE)::TIMESTAMPTZ AS tstz, "
            "(DATE '2024-12-28' + (i % 300)::INTEGER)::DATE AS d, (i % 1000)::DOUBLE AS price, "
            "(i % 1000 - 500)::INTEGER AS delta, CASE i % 5 WHEN 0 THEN 'checkout' WHEN 1 THEN 'cart' "
            "ELSE 'web' END AS service, (i % 5)::SMALLINT AS sev, i AS v FROM range(50000) t(i);")
t.statement("INSERT INTO g VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, -1);")
t.statement("CREATE TABLE old AS SELECT TIMESTAMP '1969-12-25 00:00:00' + INTERVAL (i * 173) MINUTE AS ts, "
            "i AS v FROM range(4000) t(i);")
diff(t, "e", shapes, "compressed_materialization", ["shape", "k", "c", "s"])
t.query("SELECT k, c FROM (SELECT CASE WHEN price < 100 THEN 'low' WHEN price < 500 THEN 'mid' ELSE 'high' END "
        "AS k, count(*) AS c FROM g GROUP BY 1) ORDER BY 1;", ["k", "c"], 3)
t.query("SELECT k::VARCHAR AS k, c FROM (SELECT date_bin(INTERVAL '1 day', ts, TIMESTAMP '2000-01-01 06:00') AS k, "
        "count(*) AS c FROM g WHERE v < 1500 GROUP BY 1) ORDER BY 1;", ["k", "c"], 10)
t.query("SELECT date_bin(INTERVAL '1 month', ts, TIMESTAMP '2000-01-01') AS k, count(*) AS c FROM g GROUP BY 1;",
        ["k", "c"])

t.write()
