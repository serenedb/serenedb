from sqllogic_gen import Test, lit

t = Test("SELECT DISTINCT over buckets, labels, casts and coordinates, through "
         "subqueries, unions, joins and filters, and over infinities, NULL and "
         "empty inputs, in sessions of zones with DST: every query returns the "
         "same rows with compressed materialization off and on, and every plan "
         "serializes.")

SHAPES = (
    ("dt hour", "SELECT DISTINCT date_trunc('hour', ts) k FROM dr"),
    ("dt month", "SELECT DISTINCT date_trunc('month', ts) k FROM dr"),
    ("dt week", "SELECT DISTINCT date_trunc('week', ts) k FROM dr"),
    ("dt millennium", "SELECT DISTINCT date_trunc('millennium', ts) k FROM dr"),
    ("dt hour zoned", "SELECT DISTINCT date_trunc('hour', tstz) k FROM dr"),
    ("dt day zoned", "SELECT DISTINCT date_trunc('day', tstz) k FROM dr"),
    ("dt second: declines", "SELECT DISTINCT date_trunc('second', ts) k FROM dr"),
    ("time_bucket 15 min", "SELECT DISTINCT time_bucket(INTERVAL '15 minutes', ts) k FROM dr"),
    ("time_bucket 7 min", "SELECT DISTINCT time_bucket(INTERVAL '7 minutes', ts) k FROM dr"),
    ("time_bucket 1 month", "SELECT DISTINCT time_bucket(INTERVAL '1 month', ts) k FROM dr"),
    ("time_bucket offset", "SELECT DISTINCT time_bucket(INTERVAL '15 minutes', ts, INTERVAL '5 minutes') k FROM dr"),
    ("time_bucket zoned", "SELECT DISTINCT time_bucket(INTERVAL '15 minutes', tstz) k FROM dr"),
    ("time_bucket zone arg", "SELECT DISTINCT time_bucket(INTERVAL '1 day', tstz, 'America/Havana') k FROM dr"),
    ("time_bucket zone origin", "SELECT DISTINCT time_bucket(INTERVAL '1 day', tstz, 'America/Havana', "
                                "TIMESTAMPTZ '2025-01-01 05:00:00+00') k FROM dr"),
    ("date_bin", "SELECT DISTINCT date_bin(INTERVAL '90 seconds', ts, TIMESTAMP '2025-01-01 00:00:07') k FROM dr"),
    ("time_bucket on TIME", "SELECT DISTINCT time_bucket(INTERVAL '15 minutes', tm) k FROM dr"),
    ("strftime date", "SELECT DISTINCT strftime(ts, '%Y-%m-%d') k FROM dr"),
    ("strftime time of day", "SELECT DISTINCT strftime(ts, '%H:%M') k FROM dr"),
    ("strftime time of day zoned", "SELECT DISTINCT strftime(tstz, '%H') k FROM dr"),
    ("strftime 12 hour: declines", "SELECT DISTINCT strftime(ts, '%I') k FROM dr"),
    ("monthname", "SELECT DISTINCT monthname(ts) k FROM dr"),
    ("dayname zoned", "SELECT DISTINCT dayname(tstz) k FROM dr"),
    ("last_day", "SELECT DISTINCT last_day(ts) k FROM dr"),
    ("cast to DATE", "SELECT DISTINCT tstz::DATE k FROM dr"),
    ("cast hour to TIME", "SELECT DISTINCT date_trunc('hour', ts)::TIME k FROM dr"),
    ("cast to TIME: declines", "SELECT DISTINCT ts::TIME k FROM dr"),
    ("coordinates year month", "SELECT DISTINCT year(ts) y, month(ts) m FROM dr"),
    ("coordinates year week: declines", "SELECT DISTINCT year(ts) y, week(ts) w FROM dr"),
    ("nested day and hour", "SELECT DISTINCT date_trunc('day', ts) a, date_trunc('hour', ts) b FROM dr"),
    ("label year quarter", "SELECT DISTINCT year(ts) || '-Q' || quarter(ts) k FROM dr"),
    ("epoch of a truncation", "SELECT DISTINCT epoch_ms(date_trunc('hour', ts)) k FROM dr"),
    ("interval shift", "SELECT DISTINCT date_trunc('hour', ts) + INTERVAL '30 minutes' k FROM dr"),
    ("numeric bin", "SELECT DISTINCT v // 100 k FROM dr"),
    ("epoch bin", "SELECT DISTINCT floor(epoch(ts) / 900) * 900 k FROM dr"),
    ("bucket plus plain group", "SELECT DISTINCT date_trunc('hour', ts) k, v % 3 m FROM dr"),
    ("bucket plus varchar group", "SELECT DISTINCT date_trunc('hour', ts) k, v::VARCHAR m FROM dr"),
    ("bucket twice same column", "SELECT DISTINCT date_trunc('hour', ts) a, date_trunc('hour', ts) b FROM dr"),
    ("at time zone", "SELECT DISTINCT date_trunc('hour', ts AT TIME ZONE 'America/Havana') k FROM dr"),
    ("filtered", "SELECT DISTINCT date_trunc('hour', ts) k FROM dr WHERE ts > TIMESTAMP '2025-03-30 00:00:00'"),
    ("alias below", "SELECT DISTINCT k FROM (SELECT date_trunc('hour', ts) k FROM dr)"),
    ("wrapped alias below", "SELECT DISTINCT k::VARCHAR || 'x' k FROM (SELECT date_trunc('hour', ts) k FROM dr)"),
    ("union all below", "SELECT DISTINCT date_trunc('hour', ts) k FROM (SELECT ts FROM dr UNION ALL SELECT ts FROM dr)"),
    ("aggregate below", "SELECT DISTINCT date_trunc('hour', ts) k FROM (SELECT ts, count(*) c FROM dr GROUP BY 1)"),
    ("distinct of distinct", "SELECT DISTINCT k FROM (SELECT DISTINCT date_trunc('hour', ts) k FROM dr)"),
    ("distinct on: declines", "SELECT DISTINCT ON (date_trunc('hour', ts)) v k FROM dr ORDER BY "
                              "date_trunc('hour', ts), v"),
    ("ordered", "SELECT DISTINCT date_trunc('hour', ts) k FROM dr ORDER BY 1"),
    ("limited", "SELECT DISTINCT date_trunc('hour', ts) k FROM dr ORDER BY 1 LIMIT 5"),
    ("joined below", "SELECT DISTINCT date_trunc('hour', a.ts) k FROM dr a JOIN dr b ON a.v = b.v"),
    ("null keys only", "SELECT DISTINCT date_trunc('hour', ts) k FROM dr WHERE ts IS NULL"),
    ("single row", "SELECT DISTINCT date_trunc('hour', ts) k FROM dr WHERE v = 7"),
    ("empty input", "SELECT DISTINCT date_trunc('hour', ts) k FROM dr WHERE false"),
    ("infinity input", "SELECT DISTINCT date_trunc('hour', ts) k FROM inf"),
    ("infinity zoned", "SELECT DISTINCT date_trunc('day', tstz) k FROM inf"),
    ("infinity time of day", "SELECT DISTINCT strftime(ts, '%H:%M') k FROM inf"),
)

union = "\nUNION ALL ".join(f"SELECT '{lit(name)}' AS shape, to_json(q)::VARCHAR AS k FROM ({sql}) q"
                            for name, sql in SHAPES)

t.statement("CREATE TABLE dr AS SELECT TIMESTAMP '2025-03-29 12:00:00' + INTERVAL (i * 37) MINUTE AS ts, "
            "(TIMESTAMP '2025-03-29 12:00:00' + INTERVAL (i * 37) MINUTE)::TIMESTAMPTZ AS tstz, "
            "(TIMESTAMP '2025-03-29 12:00:00' + INTERVAL (i * 37) MINUTE)::TIME AS tm, i AS v FROM range(3000) t(i);")
t.statement("INSERT INTO dr VALUES (NULL, NULL, NULL, -1);")
t.statement("CREATE TABLE inf AS SELECT ts, ts::TIMESTAMPTZ AS tstz, i AS v FROM (SELECT unnest([TIMESTAMP "
            "'-infinity', TIMESTAMP 'infinity', TIMESTAMP '2025-06-01 03:04:05', NULL]) AS ts, unnest([0, 1, 2, 3]) "
            "AS i);")
t.statement("CREATE OR REPLACE TABLE shapes AS SELECT * FROM (VALUES\n"
            + ",\n".join(f"\t('{lit(name)}', '{lit(sql)}')" for name, sql in SHAPES) + ") t(shape, sql);")
for zone in ("Europe/Berlin", "America/Havana", "Australia/Lord_Howe"):
    t.statement(f"SET TimeZone = '{zone}';")
    t.statement("SET disabled_optimizers = 'compressed_materialization';")
    t.statement(f"CREATE OR REPLACE TABLE ref_d AS\n{union};")
    t.statement("SET disabled_optimizers = '';")
    t.statement(f"CREATE OR REPLACE TABLE got_d AS\n{union};")
    t.query("SELECT count(*) AS rows, count(DISTINCT shape) AS shapes,\n"
            "\t(SELECT count(*) FROM (SELECT * FROM ref_d EXCEPT ALL SELECT * FROM got_d)) AS lost,\n"
            "\t(SELECT count(*) FROM (SELECT * FROM got_d EXCEPT ALL SELECT * FROM ref_d)) AS gained\n"
            "FROM ref_d;", ["rows", "shapes", "lost", "gained"])
    t.query("SELECT count(*) AS unserializable FROM (SELECT json_serialize_plan(sql, optimize := true)::VARCHAR AS "
            "plan FROM shapes) WHERE plan LIKE '%\"error\":true%';", ["unserializable"])

t.write()
