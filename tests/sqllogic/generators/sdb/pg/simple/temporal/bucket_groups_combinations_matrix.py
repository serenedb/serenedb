from sqllogic_gen import Test
from temporal_gen import keyed, total

t = Test("Bucket shapes (date_trunc, strftime, time_bucket, date_bin and the like) "
         "over tables of timestamps, grouped in statements of many shapes: with "
         "every optimizer on, each shape returns the groups, counts and sums it "
         "does with compressed materialization off, in Europe/Berlin and, for the "
         "shapes the session's zone can change, in Australia/Lord_Howe and "
         "America/Havana.")

PARTS = ("microseconds", "milliseconds", "second", "minute", "hour", "day", "week", "month", "quarter", "year",
         "decade", "century", "millennium", "isoyear", "yearweek", "dayofweek", "dayofyear", "epoch")
TRUNCATED = (("ts", "cmb"), ("tstz", "cmb"), ("d", "cmb"), ("tns", "cmb"), ("tms", "cmb"), ("tsec", "cmb"),
             ("ts", "cmbn"), ("ts", "cmbw"), ("d", "cmbw"), ("ts", "cmbx"), ("tstz", "cmbx"))
SPECS = ("%Y", "%y", "%G", "%V", "%m", "%-m", "%b", "%B", "%d", "%-d", "%j", "%-j", "%a", "%A", "%w", "%u", "%H",
         "%-H", "%I", "%-I", "%p", "%M", "%-M", "%S", "%-S", "%f", "%n", "%g", "%z", "%Z", "%U", "%W", "%c", "%x",
         "%X")
FORMATTED = (("ts", "cmb"), ("tstz", "cmb"), ("d", "cmbw"))
COMPOSITES = (
    "%Y %m", "%Y|%H", "%Y %M", "%Y|%G", "%Y %V", "%Y|%a", "%Y %I", "%Y|%y", "%Y %b", "%Y|%z",
    "%m %Y", "%m %d", "%m|%M", "%m %S", "%m|%V", "%m %j", "%m|%I", "%m %p", "%m|%b", "%m %f",
    "%d|%Y", "%d %m", "%d %H", "%d|%S", "%d %G", "%d|%j", "%d %a", "%d|%p", "%d %y", "%d|%f",
    "%d %z", "%H|%m", "%H %d", "%H %M", "%H|%G", "%H %V", "%H|%a", "%H %I", "%H|%y", "%H %b",
    "%H|%z", "%M %Y", "%M|%d", "%M %H", "%M %S", "%M|%V", "%M %j", "%M|%I", "%M %p", "%M|%b",
    "%M %f", "%S|%Y", "%S %m", "%S|%H", "%S %M", "%S %G", "%S|%j", "%S %a", "%S|%p", "%S %y",
    "%S|%f", "%S %z", "%G|%m", "%G %d", "%G|%M", "%G %S", "%G %V", "%G|%a", "%G %I", "%G|%y",
    "%G %b", "%G|%z", "%V %Y", "%V|%d", "%V %H", "%V|%S", "%V %G", "%V %j", "%V|%I", "%V %p",
    "%V|%b", "%V %f", "%j|%Y", "%j %m", "%j|%H", "%j %M", "%j|%G", "%j %V", "%j %a", "%j|%p",
    "%j %y", "%j|%f", "%j %z", "%a|%m", "%a %d", "%a|%M", "%a %S", "%a|%V", "%a %j", "%a %I",
    "%a|%y", "%a %b", "%a|%z", "%I %Y", "%I|%d", "%I %H", "%I|%S", "%I %G", "%I|%j", "%I %a",
    "%I %p", "%I|%b", "%I %f", "%p|%Y", "%p %m", "%p|%H", "%p %M", "%p|%G", "%p %V", "%p|%a",
    "%p %I", "%p %y", "%p|%f", "%p %z", "%y|%m", "%y %d", "%y|%M", "%y %S", "%y|%V", "%y %j",
    "%y|%I", "%y %p", "%y %b", "%y|%z", "%b %Y", "%b|%d", "%b %H", "%b|%S", "%b %G", "%b|%j",
    "%b %a", "%b|%p", "%b %y", "%b %f", "%f|%Y", "%f %m", "%f|%H", "%f %M", "%f|%G", "%f %V",
    "%f|%a", "%f %I", "%f|%y", "%f %b", "%f %z", "%z|%m", "%z %d", "%z|%M", "%z %S", "%z|%V",
    "%z %j", "%z|%I", "%z %p", "%z|%b", "%z %f",
)
WIDTHS = ("1 microsecond", "150 milliseconds", "1 second", "10 seconds", "90 seconds", "1 minute", "7 minutes",
          "15 minutes", "45 minutes", "1 hour", "3 hours", "36 hours", "1 day", "2 days", "7 days", "1 month",
          "3 months", "4 months", "1 year")
ZONES = ("America/Havana", "Australia/Lord_Howe", "Pacific/Kiritimati", "Asia/Kathmandu", "Etc/UTC")
ORIGINS = ("2025-01-01 00:00:00+00", "2024-02-15 05:00:00+00")
BINS = ("90 seconds", "7 minutes", "1 hour 30 minutes", "2 days")
TIME_WIDTHS = ("1 second", "15 seconds", "1 minute", "7 minutes", "15 minutes", "20 minutes", "45 minutes", "1 hour",
               "90 minutes", "2 hours", "5 hours", "6 hours", "8 hours", "1 day")
TIME_PARTS = ("microseconds", "milliseconds", "second", "minute", "hour", "day", "month")
OTHERS = """
ts::TIME @cmb
tstz::TIME @cmb
time_bucket(INTERVAL '15 minutes', ts)::TIME @cmb
time_bucket(INTERVAL '15 minutes', tstz)::TIME @cmb
floor(epoch(ts) / 900) * 900 @cmb
floor(epoch(tstz) / 3600) * 3600 @cmb
epoch_ms(ts) // 60000 @cmb
epoch(ts) // 86400 @cmb
to_timestamp(floor(epoch(ts) / 900) * 900) @cmb
epoch_ms(date_trunc('hour', ts)) @cmb
date_trunc('hour', ts) + INTERVAL '30 minutes' @cmb
date_trunc('day', ts) - INTERVAL '12 hours' @cmb
'bin ' || (epoch(ts) // 3600)::VARCHAR @cmb
date_trunc('hour', ts)::VARCHAR @cmb
date_trunc('month', ts)::DATE @cmb
date_trunc('day', tstz)::DATE @cmb
last_day(ts) @cmb
last_day(tstz) @cmb
monthname(ts) @cmb
dayname(tstz) @cmb
year(ts) || '-Q' || quarter(ts) @cmb
isoyear(ts) || '-W' || week(ts) @cmb
monthname(tstz) || ' ' || year(tstz) @cmb
CASE WHEN v % 2 = 0 THEN 'even' ELSE 'odd' END @cmb
v // 100 @cmb
v // 100 * 100 @cmb
round(v / 7.0, -1) @cmb
floor(v / 13) @cmb
date_trunc('hour', ts AT TIME ZONE 'America/Havana') @cmb
date_trunc('day', timezone('Asia/Kolkata', tstz)) @cmb
strftime(tstz, '%H:%M') @cmb
strftime(tstz, '%H') @cmb
strftime(ts, '%H:%M:%S') @cmb
year(ts); month(ts) @cmb
year(tstz); month(tstz) @cmb
year(ts); month(ts); day(ts) @cmb
isoyear(ts); week(ts) @cmb
year(ts); quarter(ts) @cmb
year(ts); week(ts) @cmb
date_trunc('day', ts); date_trunc('hour', ts) @cmb
date_trunc('month', tstz); date_trunc('day', tstz) @cmb
hour(ts); minute(ts) @cmb
year(d); month(d) @cmbw
"""
ZONED = ("tstz", "::TIME", "AT TIME ZONE", "timezone(")


def bucket(width, args):
    return f"time_bucket(INTERVAL '{width}', {args})"


def shapes():
    out = [(f"date_trunc('{part}', {c})", table) for part in PARTS for c, table in TRUNCATED]
    out += [("date_trunc('era', tstz)", table) for table in ("cmb", "cmbx")]
    out += [(f"strftime({c}, '{spec}')", table) for spec in SPECS for c, table in FORMATTED]
    out.append(("strftime(d, '%%')", "cmbw"))
    out += [(f"strftime({'tstz' if ' ' in f else 'ts'}, '{f}')", "cmb") for f in COMPOSITES]
    for width in WIDTHS:
        out += [(bucket(width, c), table) for c, table in (("ts", "cmb"), ("tstz", "cmb"), ("d", "cmbw"),
                                                          ("ts", "cmbx"))]
        out += [(bucket(width, "ts, INTERVAL '5 minutes'"), "cmb"),
                (bucket(width, "ts, TIMESTAMP '2025-01-01 00:00:07'"), "cmb"),
                (bucket(width, "tstz, TIMESTAMPTZ '2025-01-01 00:00:07+00'"), "cmb")]
        out += [(bucket(width, f"tstz, '{zone}'"), "cmb") for zone in ZONES]
        out += [(bucket(width, f"tstz, '{zone}', TIMESTAMPTZ '{origin}'"), "cmb") for zone in ZONES[:2]
                for origin in ORIGINS]
    width = "1 day 6 hours"
    out += [(bucket(width, c), table) for c, table in (("ts", "cmb"), ("d", "cmbw"), ("ts", "cmbx"))]
    out += [(bucket(width, "ts, INTERVAL '5 minutes'"), "cmb"),
            (bucket(width, "ts, TIMESTAMP '2025-01-01 00:00:07'"), "cmb")]
    out += [(f"date_bin(INTERVAL '{width}', ts, TIMESTAMP '2025-01-01 00:00:07')", "cmb") for width in BINS]
    out += [(bucket(width, "tm"), table) for width in TIME_WIDTHS for table in ("cmb", "cmbn")]
    out += [(f"date_trunc('{part}', {c})::TIME", table) for part in TIME_PARTS
            for c, table in (("ts", "cmb"), ("tstz", "cmb"), ("ts", "cmbn"))]
    out += [line.rsplit(" @", 1) for line in OTHERS.strip().split("\n")]
    return [(keys.split("; "), table) for keys, table in out]


every = shapes()
zoned = [(keys, table) for keys, table in every if any(mark in "; ".join(keys) for mark in ZONED)]

t.statement("SET TimeZone = 'Europe/Berlin';")
t.statement("CREATE TABLE cmb AS SELECT TIMESTAMP '2025-03-29 12:00:00' + INTERVAL (i * 37) MINUTE AS ts, "
            "(TIMESTAMP '2025-03-29 12:00:00' + INTERVAL (i * 37) MINUTE)::TIMESTAMPTZ AS tstz, "
            "(TIMESTAMP '2025-03-29 12:00:00' + INTERVAL (i * 37) MINUTE)::DATE AS d, "
            "(TIMESTAMP '2025-03-29 12:00:00' + INTERVAL (i * 37) MINUTE)::TIMESTAMP_NS AS tns, "
            "(TIMESTAMP '2025-03-29 12:00:00' + INTERVAL (i * 37) MINUTE)::TIMESTAMP_MS AS tms, "
            "(TIMESTAMP '2025-03-29 12:00:00' + INTERVAL (i * 37) MINUTE)::TIMESTAMP_S AS tsec, "
            "(TIMESTAMP '2025-03-29 12:00:00' + INTERVAL (i * 37) MINUTE)::TIME AS tm, i AS v FROM range(600) t(i);")
t.statement("CREATE TABLE cmbn AS SELECT TIMESTAMP '2025-01-01 00:00:00' + INTERVAL (i * 3) MILLISECOND AS ts, "
            "(TIMESTAMP '2025-01-01 00:00:00' + INTERVAL (i * 3) MILLISECOND)::TIMESTAMPTZ AS tstz, "
            "(TIMESTAMP '2025-01-01 00:00:00' + INTERVAL (i * 3) MILLISECOND)::TIME AS tm, i AS v FROM range(600) t(i);")
t.statement("CREATE TABLE cmbw AS SELECT TIMESTAMP '2019-02-27 23:30:00' + INTERVAL (i * 41) HOUR AS ts, "
            "(TIMESTAMP '2019-02-27 23:30:00' + INTERVAL (i * 41) HOUR)::TIMESTAMPTZ AS tstz, "
            "(TIMESTAMP '2019-02-27 23:30:00' + INTERVAL (i * 41) HOUR)::DATE AS d, i AS v FROM range(600) t(i);")
t.statement("CREATE TABLE cmbx AS SELECT ts, ts::TIMESTAMPTZ AS tstz, v FROM (VALUES (TIMESTAMP '2025-03-30 "
            "01:59:59.999999', 1), (TIMESTAMP '2025-03-30 02:00:00', 2), (TIMESTAMP '2025-10-26 02:30:00', 3), "
            "(TIMESTAMP '1969-12-31 23:59:59.999999', 4), (TIMESTAMP '1970-01-01 00:00:00', 5), (TIMESTAMP "
            "'1900-01-01 00:00:00', 6), (TIMESTAMP '2299-12-31 00:00:00', 7), (NULL, 8)) t(ts, v);")
t.part()
t.statement("SET TimeZone = 'Europe/Berlin';")
total(t, "cmb", [keyed(keys, table) for keys, table in every], "compressed_materialization")
t.part()
for zone in ("Australia/Lord_Howe", "America/Havana"):
    t.statement(f"SET TimeZone = '{zone}';")
    total(t, "cmb", [keyed(keys, table) for keys, table in zoned], "compressed_materialization")

t.write()
