from sqllogic_gen import Test
from temporal_gen import named, total

t = Test("Time-of-day buckets (hour and minute formats, casts to TIME, buckets of "
         "TIME values, buckets with zones, origins and offsets, millisecond bins) "
         "grouped in sessions of zones with DST and odd offsets: every grouping "
         "returns the same keys, counts and sums with compressed materialization "
         "off and on.")

SHAPES = (
    ("tod", "strftime(ts, '%H')"),
    ("tod", "strftime(ts, '%H:%M')"),
    ("tod", "strftime(ts, '%H:%M:%S')"),
    ("tod", "strftime(ts, '%I %p')"),
    ("tod", "strftime(ts, '%I')"),
    ("tod", "strftime(tstz, '%H')"),
    ("tod", "strftime(tstz, '%H:%M')"),
    ("tod", "strftime(tstz, 'hour %H of the day')"),
    ("tod", "strftime(tstz, '%I:%M %p')"),
    ("tod", "strftime(tstz, '%H:%M %z')"),
    ("tod", "date_trunc('hour', ts)::TIME"),
    ("tod", "date_trunc('minute', ts)::TIME"),
    ("tod", "date_trunc('day', ts)::TIME"),
    ("tod", "date_trunc('hour', tstz)::TIME"),
    ("tod", "date_trunc('minute', tstz)::TIME"),
    ("tod", "date_trunc('day', tstz)::TIME"),
    ("tod", "time_bucket(INTERVAL '15 minutes', ts)::TIME"),
    ("tod", "time_bucket(INTERVAL '30 minutes', tstz)::TIME"),
    ("tod", "time_bucket(INTERVAL '15 minutes', tstz, 'America/Havana')::TIME"),
    ("tod", "ts::TIME"),
    ("tod", "time_bucket(INTERVAL '15 minutes', tm)"),
    ("tod", "time_bucket(INTERVAL '1 hour', tm)"),
    ("tod", "time_bucket(INTERVAL '90 minutes', tm)"),
    ("tod", "time_bucket(INTERVAL '7 minutes', tm)"),
    ("tod", "time_bucket(INTERVAL '6 hours', tm)"),
    ("msn", "date_trunc('millisecond', ts)"),
    ("msn", "date_trunc('microsecond', ts)"),
    ("msn", "date_trunc('millisecond', tstz)"),
    ("tod", "time_bucket(INTERVAL '1 day', tstz, 'America/Havana', TIMESTAMPTZ '2025-01-01 05:00:00+00')"),
    ("tod", "time_bucket(INTERVAL '1 month', tstz, 'America/Havana', TIMESTAMPTZ '2024-02-15 05:00:00+00')"),
    ("tod", "time_bucket(INTERVAL '90 minutes', tstz, 'Europe/Berlin', TIMESTAMPTZ '2025-01-01 00:07:00+00')"),
    ("tod", "time_bucket(INTERVAL '2 days', tstz, 'Australia/Lord_Howe', TIMESTAMPTZ '2025-03-01 00:00:00+00')"),
    ("tod", "time_bucket(INTERVAL '3 months', tstz, 'Pacific/Kiritimati', TIMESTAMPTZ '2024-05-01 00:00:00+00')"),
    ("tod", "time_bucket(INTERVAL '45 minutes', tstz, 'Asia/Kathmandu', TIMESTAMPTZ '2025-06-01 00:00:00+00')"),
    ("tod", "date_bin(INTERVAL '3 hours', ts, TIMESTAMP '2025-01-01 00:00:07')::TIME"),
    ("tod", "date_bin(INTERVAL '7 minutes', ts, TIMESTAMP '2025-01-01 00:00:07')::TIME"),
    ("tod", "date_bin(INTERVAL '1 hour', ts, TIMESTAMP '2025-01-01 00:00:00')::TIME"),
    ("tod", "time_bucket(INTERVAL '15 minutes', ts, INTERVAL '5 minutes')::TIME"),
    ("tod", "time_bucket(INTERVAL '15 minutes', ts, INTERVAL '15 minutes')::TIME"),
    ("tod", "time_bucket(INTERVAL '1 hour', ts, TIMESTAMP '2025-01-01 00:17:00')::TIME"),
    ("tod", "time_bucket(INTERVAL '30 minutes', tstz, INTERVAL '10 minutes')::TIME"),
    ("tod", "date_trunc('hour', ts + INTERVAL '20 minutes')::TIME"),
    ("msn", "date_bin(INTERVAL '150 milliseconds', ts, TIMESTAMP '2025-01-01 00:00:00.007')::TIME"),
    ("msn", "time_bucket(INTERVAL '150 milliseconds', ts, INTERVAL '7 milliseconds')::TIME"),
)

shapes = [named(table, expr, table) for table, expr in SHAPES]

t.statement("SET TimeZone = 'Europe/Berlin';")
t.statement("CREATE TABLE tod AS SELECT TIMESTAMP '2025-03-25 00:00:00' + INTERVAL (i * 37) MINUTE AS ts, "
            "(TIMESTAMP '2025-03-25 00:00:00' + INTERVAL (i * 37) MINUTE)::TIMESTAMPTZ AS tstz, "
            "(TIMESTAMP '2025-03-25 00:00:00' + INTERVAL (i * 37) MINUTE)::TIME AS tm, i AS v FROM range(20000) t(i);")
t.statement("INSERT INTO tod VALUES (NULL, NULL, NULL, -1);")
t.statement("CREATE TABLE msn AS SELECT TIMESTAMP '2025-01-01 00:00:00' + INTERVAL (i) MILLISECOND AS ts, "
            "(TIMESTAMP '2025-01-01 00:00:00' + INTERVAL (i) MILLISECOND)::TIMESTAMPTZ AS tstz, i AS v "
            "FROM range(20000) t(i);")
t.statement("INSERT INTO msn VALUES (NULL, NULL, -1);")
for zone in ("Europe/Berlin", "America/Havana", "Australia/Lord_Howe", "Pacific/Chatham", "America/Santiago",
             "Etc/UTC"):
    t.statement(f"SET TimeZone = '{zone}';")
    total(t, "tod", shapes, "compressed_materialization", len(shapes))
t.statement("SET TimeZone = 'Europe/Berlin';")
t.query("SELECT time_bucket(INTERVAL '1 day', TIMESTAMPTZ '2025-03-30 12:00:00+00', 'America/Havana', TIMESTAMPTZ "
        "'2025-01-01 05:00:00+00')::VARCHAR AS day_bucket, time_bucket(INTERVAL '1 month', TIMESTAMPTZ "
        "'2025-03-30 12:00:00+00', 'America/Havana', TIMESTAMPTZ '2024-02-15 05:00:00+00')::VARCHAR AS month_bucket, "
        "time_bucket(INTERVAL '90 minutes', TIMESTAMPTZ '2025-03-30 12:20:00+00', 'America/Havana', TIMESTAMPTZ "
        "'2025-01-01 00:07:00+00')::VARCHAR AS micros_bucket;", ["day_bucket", "month_bucket", "micros_bucket"])
t.query("SELECT time_bucket(INTERVAL '1 day', TIMESTAMPTZ '2025-04-06 10:00:00+00', 'Australia/Lord_Howe', TIMESTAMPTZ "
        "'2025-03-01 00:00:00+00')::VARCHAR AS lord_howe, time_bucket(INTERVAL '2 months', TIMESTAMPTZ "
        "'2025-07-15 10:00:00+00', 'Pacific/Kiritimati', TIMESTAMPTZ '2024-05-01 00:00:00+00')::VARCHAR AS kiritimati;",
        ["lord_howe", "kiritimati"])
for width, message in (("1 month 3 days", "Month intervals cannot have day or time component"),
                       ("0 seconds", "Period must be greater than 0")):
    t.error(f"SELECT time_bucket(INTERVAL '{width}', tstz, 'America/Havana', TIMESTAMPTZ '2025-01-01 05:00:00+00') "
            "AS b, count(*) AS c FROM tod GROUP BY 1;", f"db error: ERROR: {message}")
t.query("SELECT count(*) AS buckets FROM (SELECT time_bucket(INTERVAL '1 day', tstz, 'America/Havana', TIMESTAMPTZ "
        "'2025-01-01 05:00:00+00') AS b FROM tod GROUP BY 1);", ["buckets"])
t.query("SELECT strftime(TIMESTAMP '2025-03-25 13:07:42', '%H:%M') AS naive, strftime(TIMESTAMPTZ "
        "'2025-03-30 00:30:00+00', '%H:%M') AS zoned, date_trunc('hour', TIMESTAMP '2025-03-25 13:07:42')::TIME::VARCHAR "
        "AS cast_time, time_bucket(INTERVAL '15 minutes', TIME '13:07:42')::VARCHAR AS time_bucket;",
        ["naive", "zoned", "cast_time", "time_bucket"])

t.write()
