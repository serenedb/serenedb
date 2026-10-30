from sqllogic_gen import Test
from temporal_gen import named

t = Test("Buckets of arbitrary widths (seconds to months, per-row widths, origins "
         "and offsets, time zones) on timestamps and zoned timestamps: every "
         "grouping returns the same keys, counts and sums with compressed "
         "materialization off and on, and invalid widths fail.")

SHAPES = (
    ("iv", "time_bucket(INTERVAL '10 seconds', ts)"),
    ("iv", "time_bucket(INTERVAL '90 seconds', ts)"),
    ("iv", "time_bucket(INTERVAL '7 minutes', ts)"),
    ("iv", "time_bucket(INTERVAL '15 minutes', ts)"),
    ("iv", "time_bucket(INTERVAL '45 minutes', ts)"),
    ("iv", "time_bucket(INTERVAL '1 hour', ts)"),
    ("iv", "time_bucket(INTERVAL '3 hours', ts)"),
    ("iv", "time_bucket(INTERVAL '150 milliseconds', ts)"),
    ("ivw", "time_bucket(INTERVAL '2 days', ts)"),
    ("ivw", "time_bucket(INTERVAL '1 month', ts)"),
    ("ivw", "time_bucket(INTERVAL '4 months', ts)"),
    ("iv", "time_bucket(INTERVAL '15 minutes', ts, INTERVAL '5 minutes')"),
    ("iv", "time_bucket(INTERVAL '90 seconds', ts, INTERVAL '-20 seconds')"),
    ("iv", "time_bucket(INTERVAL '90 seconds', ts, TIMESTAMP '2025-01-01 00:00:07')"),
    ("iv", "time_bucket(INTERVAL '7 minutes', ts, DATE '2024-12-30')"),
    ("ivw", "time_bucket(INTERVAL '5 months', ts, TIMESTAMP '2024-02-17 00:00:00')"),
    ("iv", "date_bin(INTERVAL '90 seconds', ts, TIMESTAMP '2025-01-01 00:00:07')"),
    ("iv", "date_bin(INTERVAL '7 minutes', ts, TIMESTAMP '2024-12-31 23:58:00')"),
    ("iv", "date_bin(INTERVAL '1 hour 30 minutes', ts, TIMESTAMP '2025-01-01 00:00:00')"),
    ("ivn", "date_trunc('second', ts)"),
    ("iv", "date_trunc('minute', ts)"),
    ("iv", "date_trunc('hour', ts)"),
    ("iv", "floor(epoch(ts) / 900) * 900"),
    ("iv", "epoch_ms(ts) // 60000"),
    ("iv", "to_timestamp(floor(epoch(ts) / 900) * 900)"),
    ("iv", "time_bucket(INTERVAL (v % 3 + 1) MINUTE, ts)"),
    ("iv", "time_bucket(INTERVAL '90 seconds', tstz)"),
    ("iv", "time_bucket(INTERVAL '7 minutes', tstz)"),
    ("iv", "time_bucket(INTERVAL '15 minutes', tstz)"),
    ("iv", "time_bucket(INTERVAL '1 hour', tstz)"),
    ("ivw", "time_bucket(INTERVAL '1 day', tstz)"),
    ("ivw", "time_bucket(INTERVAL '1 month', tstz)"),
    ("iv", "time_bucket(INTERVAL '15 minutes', tstz, INTERVAL '5 minutes')"),
    ("iv", "time_bucket(INTERVAL '90 seconds', tstz, TIMESTAMPTZ '2025-01-01 00:00:07+00')"),
    ("iv", "date_trunc('minute', tstz)"),
    ("iv", "date_trunc('hour', tstz)"),
    ("iv", "time_bucket(INTERVAL '15 minutes', tstz, 'America/Havana')"),
    ("iv", "time_bucket(INTERVAL '45 minutes', tstz, 'Australia/Lord_Howe')"),
    ("ivw", "time_bucket(INTERVAL '1 day', tstz, 'America/Havana')"),
    ("ivw", "time_bucket(INTERVAL '1 month', tstz, 'Pacific/Kiritimati')"),
    ("iv", "floor(epoch(tstz) / 900) * 900"),
)
ERRORS = (
    ("time_bucket(INTERVAL '1 month 2 hours', ts)", "ivw", "Month intervals cannot have day or time component"),
    ("time_bucket(INTERVAL '0 seconds', ts)", "iv", "Period must be greater than 0"),
    ("time_bucket(INTERVAL '-5 minutes', ts)", "iv", "Period must be greater than 0"),
    ("date_bin(INTERVAL '1 month', ts, TIMESTAMP '2025-01-01 00:00:00')", "iv",
     "timestamps cannot be binned into intervals containing months or years"),
    ("date_bin(INTERVAL '0 seconds', ts, TIMESTAMP '2025-01-01 00:00:00')", "iv", "stride must be greater than zero"),
)

union = "\nUNION ALL ".join(named(table, expr, table) for table, expr in SHAPES)

t.statement("SET TimeZone = 'Europe/Berlin';")
for name, start, step in (("iv", "2025-03-29 12:00:00", "i * 17"), ("ivw", "2023-11-01 00:00:00", "i * 3671"),
                          ("ivn", "2025-01-01 00:00:00", "i")):
    t.statement(f"CREATE TABLE {name} AS SELECT TIMESTAMP '{start}' + INTERVAL ({step}) SECOND AS ts, "
                f"(TIMESTAMP '{start}' + INTERVAL ({step}) SECOND)::TIMESTAMPTZ AS tstz, i AS v FROM range(20000) t(i);")
    t.statement(f"INSERT INTO {name} VALUES (NULL, NULL, -1);")
t.statement("SET disabled_optimizers = 'compressed_materialization';")
t.statement(f"CREATE TABLE ref_iv AS\n{union}\n;")
t.statement("SET disabled_optimizers = '';")
t.statement(f"CREATE TABLE got_iv AS\n{union}\n;")
t.query("SELECT count(*) AS rows, count(DISTINCT shape) AS shapes FROM ref_iv;", ["rows", "shapes"])
t.query("SELECT count(*) AS ref_minus_got FROM (SELECT * FROM ref_iv EXCEPT ALL SELECT * FROM got_iv);",
        ["ref_minus_got"])
t.query("SELECT count(*) AS got_minus_ref FROM (SELECT * FROM got_iv EXCEPT ALL SELECT * FROM ref_iv);",
        ["got_minus_ref"])
for expr, table, message in ERRORS:
    t.error(f"SELECT {expr} AS b, count(*) AS c FROM {table} GROUP BY 1;", f"db error: ERROR: {message}")
t.query("SELECT time_bucket(INTERVAL '90 minutes', TIMESTAMPTZ '2025-03-30 00:30:00+00')::VARCHAR AS spring, "
        "time_bucket(INTERVAL '90 minutes', TIMESTAMPTZ '2025-10-26 00:30:00+00')::VARCHAR AS fall, "
        "time_bucket(INTERVAL '7 minutes', TIMESTAMP '2025-03-29 12:03:00')::VARCHAR AS naive, "
        "date_bin(INTERVAL '90 seconds', TIMESTAMP '2025-03-29 12:03:00', TIMESTAMP '2025-01-01 00:00:07')::VARCHAR "
        "AS binned;", ["spring", "fall", "naive", "binned"])
t.query("SELECT count(*) AS buckets, min(b)::VARCHAR AS first, max(b)::VARCHAR AS last FROM (SELECT "
        "time_bucket(INTERVAL '7 minutes', ts) AS b FROM iv GROUP BY 1);", ["buckets", "first", "last"])
t.query("SELECT count(*) AS buckets FROM (SELECT time_bucket(INTERVAL '15 minutes', tstz, 'America/Havana') AS b "
        "FROM iv GROUP BY 1);", ["buckets"])

t.write()
