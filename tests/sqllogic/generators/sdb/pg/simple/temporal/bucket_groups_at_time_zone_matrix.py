from sqllogic_gen import Test
from temporal_gen import diff, labelled

t = Test("Buckets of timestamps converted with AT TIME ZONE and timezone() to "
         "zones with DST, half-hour offsets and skipped days, in sessions of other "
         "zones, over finite rows and over rows with infinity: every grouping "
         "returns the same keys, counts and sums with compressed materialization "
         "off and on.")

ZONES = ("UTC", "Europe/Berlin", "America/Havana", "America/Santiago", "Asia/Kolkata", "Australia/Lord_Howe",
         "Pacific/Kiritimati", "America/St_Johns")
PARTS = ("hour", "day", "week", "month", "quarter", "year")
FINITE = (
    ("timezone day", "date_trunc('day', timezone('{z}', tstz))"),
    ("tb 6h", "time_bucket(INTERVAL '6 hours', tstz AT TIME ZONE '{z}')"),
    ("tb 1d", "time_bucket(INTERVAL '1 day', tstz AT TIME ZONE '{z}')"),
    ("sf", "strftime(tstz AT TIME ZONE '{z}', '%Y-%m-%d %H')"),
    ("sf month", "strftime(tstz AT TIME ZONE '{z}', '%Y-%m')"),
    ("label", "year(tstz AT TIME ZONE '{z}') || '-Q' || quarter(tstz AT TIME ZONE '{z}')"),
    ("naive to tz hour", "date_trunc('hour', ts AT TIME ZONE '{z}')"),
    ("naive to tz day date", "(ts AT TIME ZONE '{z}')::DATE"),
    ("double", "date_trunc('day', (ts AT TIME ZONE 'UTC') AT TIME ZONE '{z}')"),
    ("epoch", "floor(epoch(tstz AT TIME ZONE '{z}') / 3600) * 3600"),
    ("monthname", "monthname(tstz AT TIME ZONE '{z}')"),
)


def shapes():
    out = []
    for z in ZONES:
        both = [(f"dt {part}", f"date_trunc('{part}', tstz AT TIME ZONE '{z}')") for part in PARTS]
        both.append(("date", f"(tstz AT TIME ZONE '{z}')::DATE"))
        for label, expr in both:
            out += [labelled(f"{z} {label}", expr, "gf"), labelled(f"{z} {label} inf", expr, "g")]
        out += [labelled(f"{z} {label}", expr.replace("{z}", z), "gf") for label, expr in FINITE]
    return out


t.statement("SET TimeZone = 'Etc/UTC';")
t.statement("CREATE TABLE g AS SELECT tstz, (tstz AT TIME ZONE 'UTC')::TIMESTAMP AS ts, v FROM (SELECT TIMESTAMPTZ "
            "'2025-03-07 00:00:00+00' + INTERVAL (i * 53) MINUTE AS tstz, i AS v FROM range(0, 1500) t(i) UNION ALL "
            "SELECT TIMESTAMPTZ '2025-10-24 00:00:00+00' + INTERVAL (i * 53) MINUTE, 10000 + i FROM range(0, 1500) "
            "t(i) UNION ALL SELECT TIMESTAMPTZ '2024-12-30 12:00:00+00' + INTERVAL (i * 53) MINUTE, 20000 + i FROM "
            "range(0, 300) t(i) UNION ALL SELECT TIMESTAMPTZ '1969-12-25 00:00:00+00' + INTERVAL (i * 173) MINUTE, "
            "30000 + i FROM range(0, 600) t(i));")
t.statement("INSERT INTO g VALUES (NULL, NULL, -1), (TIMESTAMPTZ 'infinity', TIMESTAMP 'infinity', -2);")
t.statement("CREATE TABLE gf AS SELECT * FROM g WHERE isfinite(tstz) OR tstz IS NULL;")
for k, session in enumerate(("Europe/Berlin", "Pacific/Kiritimati", "Asia/Kolkata"), 1):
    t.statement(f"SET TimeZone = '{session}';")
    diff(t, str(k), shapes(), "compressed_materialization", ["shape", "k", "c", "s"])

t.write()
