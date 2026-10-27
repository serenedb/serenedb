from sqllogic_gen import Test, lit
from temporal_gen import diff

t = Test("Pairs of nested buckets grouped together (a day and its hours, a month "
         "and its weeks, buckets and plain columns, across DST transitions): "
         "every grouping returns the same pairs of keys, counts and sums with "
         "compressed materialization off and on.")

PAIRS = {
    "g": (
        ("date_trunc('day', ts)", "date_trunc('hour', ts)"),
        ("date_trunc('hour', ts)", "date_trunc('day', ts)"),
        ("date_trunc('month', ts)", "date_trunc('day', ts)"),
        ("date_trunc('month', ts)", "date_trunc('week', ts)"),
        ("date_trunc('week', ts)", "date_trunc('day', ts)"),
        ("date_trunc('year', ts)", "date_trunc('quarter', ts)"),
        ("date_trunc('quarter', ts)", "date_trunc('month', ts)"),
        ("date_trunc('decade', ts)", "date_trunc('year', ts)"),
        ("date_trunc('century', ts)", "date_trunc('decade', ts)"),
        ("date_trunc('day', ts)", "time_bucket(INTERVAL '6 hours', ts)"),
        ("time_bucket(INTERVAL '2 hours', ts)", "date_trunc('hour', ts)"),
        ("time_bucket(INTERVAL '1 day', ts)", "time_bucket(INTERVAL '15 minutes', ts)"),
        ("time_bucket(INTERVAL '90 minutes', ts)", "time_bucket(INTERVAL '30 minutes', ts)"),
        ("time_bucket(INTERVAL '90 minutes', ts)", "time_bucket(INTERVAL '1 hour', ts)"),
        ("date_trunc('day', ts)", "strftime(ts, '%Y-%m-%d %H')"),
        ("strftime(ts, '%Y-%m')", "date_trunc('day', ts)"),
        ("strftime(ts, '%Y-%m-%d')", "date_trunc('hour', ts)"),
        ("date_trunc('day', ts)::DATE", "date_trunc('hour', ts)"),
        ("date_trunc('week', ts)::DATE", "date_trunc('day', ts)::DATE"),
        ("date_trunc('day', ts) + INTERVAL '6 hours'", "date_trunc('hour', ts)"),
        ("epoch_ms(date_trunc('day', ts))", "date_trunc('hour', ts)"),
        ("date_trunc('day', ts)", "date_trunc('hour', ts)::TIMESTAMP_MS"),
        ("concat(year(ts), '-Q', quarter(ts))", "date_trunc('month', ts)"),
        ("concat(year(ts), '-Q', quarter(ts))", "strftime(ts, '%Y-%m')"),
        ("monthname(ts)", "date_trunc('day', ts)"),
        ("date_trunc('day', ts)", "sev"),
        ("sev", "date_trunc('hour', ts)"),
        ("date_trunc('day', ts)", "floor(price / 100)"),
        ("date_trunc('day', ts)", "date_trunc('hour', tstz)"),
        ("date_trunc('day', d)", "date_trunc('month', d)"),
        ("date_trunc('month', d)", "d"),
        ("date_trunc('day', tstz)", "date_trunc('hour', tstz)"),
        ("date_trunc('month', tstz)", "date_trunc('day', tstz)"),
        ("date_trunc('month', tstz)::DATE", "date_trunc('hour', tstz)"),
        ("time_bucket(INTERVAL '1 day', tstz, 'Asia/Tokyo')", "time_bucket(INTERVAL '1 hour', tstz, 'Asia/Tokyo')"),
        ("date_bin(INTERVAL '1 day', ts, TIMESTAMP '2000-01-01')", "date_bin(INTERVAL '1 hour', ts, TIMESTAMP '2000-01-01')"),
        ("date_bin(INTERVAL '1 day', ts, TIMESTAMP '2000-01-01 06:00')", "date_trunc('hour', ts)"),
        ("floor(epoch(ts) / 86400) * 86400", "floor(epoch(ts) / 3600) * 3600"),
        ("to_timestamp(floor(epoch(ts) / 86400) * 86400)", "to_timestamp(floor(epoch(ts) / 3600) * 3600)"),
    ),
    "dst": (
        ("date_trunc('day', tstz)", "date_trunc('hour', tstz)"),
        ("date_trunc('month', tstz)", "date_trunc('day', tstz)"),
        ("date_trunc('week', tstz)", "date_trunc('day', tstz)"),
        ("date_trunc('day', tstz)", "date_trunc('minute', tstz)"),
        ("time_bucket(INTERVAL '1 day', tstz, 'America/Havana')", "time_bucket(INTERVAL '1 hour', tstz, 'America/Havana')"),
        ("time_bucket(INTERVAL '7 days', tstz, 'Europe/Berlin')", "time_bucket(INTERVAL '1 day', tstz, 'Europe/Berlin')"),
        ("date_trunc('day', tstz)", "time_bucket(INTERVAL '1 hour', tstz, 'Europe/Berlin')"),
        ("strftime(tstz, '%Y-%m-%d')", "date_trunc('hour', tstz)"),
    ),
}

shapes = [f"SELECT '{lit(f'{table} {k1} vs {k2}')}' AS shape, k1::VARCHAR AS k1, k2::VARCHAR AS k2, c, s FROM "
          f"(SELECT {k1} AS k1, {k2} AS k2, count(*) AS c, sum(v) AS s FROM {table} GROUP BY 1, 2)"
          for table, pairs in PAIRS.items() for k1, k2 in pairs]

t.statement("SET TimeZone = 'Europe/Berlin';")
t.statement("CREATE TABLE g AS SELECT TIMESTAMP '2024-12-28 00:00:00' + INTERVAL (i * 7) MINUTE AS ts, "
            "(TIMESTAMP '2024-12-28 00:00:00' + INTERVAL (i * 7) MINUTE)::TIMESTAMPTZ AS tstz, "
            "(DATE '2024-12-28' + (i % 300)::INTEGER)::DATE AS d, (i % 1000)::DOUBLE AS price, "
            "(i % 5)::SMALLINT AS sev, i AS v FROM range(50000) t(i);")
t.statement("INSERT INTO g VALUES (NULL, NULL, NULL, NULL, NULL, -1);")
t.statement("CREATE TABLE dst AS SELECT tstz, v FROM (SELECT TIMESTAMPTZ '2025-03-28 00:00:00+00' + INTERVAL "
            "(i * 41) MINUTE AS tstz, i AS v FROM range(0, 400) t(i) UNION ALL SELECT TIMESTAMPTZ "
            "'2025-10-24 00:00:00+00' + INTERVAL (i * 41) MINUTE, 1000 + i FROM range(0, 400) t(i) UNION ALL "
            "SELECT TIMESTAMPTZ '2025-11-01 12:00:00+00' + INTERVAL (i * 41) MINUTE, 2000 + i FROM range(0, 100) "
            "t(i));")
diff(t, "n", shapes, "compressed_materialization", ["shape", "k1", "k2", "c", "s"])
t.query("SELECT m::VARCHAR AS m, d::VARCHAR AS d, h::VARCHAR AS h, c FROM (SELECT date_trunc('month', ts) AS m, "
        "date_trunc('day', ts) AS d, date_trunc('hour', ts) AS h, count(*) AS c FROM g WHERE v BETWEEN 400 AND 460 "
        "GROUP BY 1, 2, 3) ORDER BY 3;", ["m", "d", "h", "c"], 8)

t.write()
