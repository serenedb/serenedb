from sqllogic_gen import Test
from temporal_gen import diff, labelled

t = Test("Truncations, buckets, names and formats of nanosecond, millisecond and "
         "second timestamps, over regular rows and over values a fraction of a "
         "microsecond from a day, month or the epoch: every grouping returns the "
         "same keys, counts and sums with compressed materialization off and on.")

PRECISIONS = (("ts_ns", "ns"), ("ts_ms", "ms"), ("ts_s", "s"))
PARTS = ("second", "minute", "hour", "day", "week", "month", "quarter", "year")
WIDTHS = ("90 seconds", "2 hours", "1 day", "1 month")
FORMATS = ("%Y-%m-%d", "%Y-%m-%d %H", "%Y-%m-%d %H:%M", "%Y-%m", "%G-%V", "%y-%m-%d", "%Y-%j",
           "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")
BOUNDARY_FORMATS = ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m", "%G-%V", "%Y-%m-%d %H:%M:%S.%f")


def common(c, prefix, table):
    return [labelled(f"{prefix}mn {c}", f"monthname({c})", table), labelled(f"{prefix}dn {c}", f"dayname({c})", table)]


regular = []
for c, _ in PRECISIONS:
    regular += [labelled(f"dt {p} {c}", f"date_trunc('{p}', {c})", "prec") for p in PARTS]
    regular += [labelled(f"tb {w} {c}", f"time_bucket(INTERVAL '{w}', {c})", "prec") for w in WIDTHS]
    regular += common(c, "", "prec")
    regular += [labelled(f"ld {c}", f"last_day({c})", "prec"), labelled(f"date {c}", f"{c}::DATE", "prec")]
regular += [labelled(f"sf {f} {short}", f"strftime({c}, '{f}')", "prec") for f in FORMATS for c, short in PRECISIONS]
boundary = []
for c, _ in PRECISIONS:
    boundary += [labelled(f"nsb dt {p} {c}", f"date_trunc('{p}', {c})", "nsb") for p in ("second", "day", "month")]
    boundary += [labelled(f"nsb tb day {c}", f"time_bucket(INTERVAL '1 day', {c})", "nsb"),
                 labelled(f"nsb tb sec {c}", f"time_bucket(INTERVAL '1 second', {c})", "nsb"),
                 labelled(f"nsb date {c}", f"{c}::DATE", "nsb")]
    boundary += common(c, "nsb ", "nsb")
boundary += [labelled(f"nsb sf {f}", f"strftime(ts_ns, '{f}')", "nsb") for f in BOUNDARY_FORMATS]

t.statement("SET TimeZone = 'Etc/UTC';")
t.statement("CREATE TABLE prec AS SELECT ts::TIMESTAMP_NS AS ts_ns, ts::TIMESTAMP_MS AS ts_ms, ts::TIMESTAMP_S AS "
            "ts_s, ts, ts::TIMESTAMPTZ AS tstz, ts::DATE AS d, i AS v FROM (SELECT TIMESTAMP '2024-12-23 00:00:00' "
            "+ INTERVAL (i * 37) MINUTE + INTERVAL (i % 7) SECOND AS ts, i FROM range(0, 1500) t(i));")
t.statement("INSERT INTO prec VALUES (NULL, NULL, NULL, NULL, NULL, NULL, -1);")
t.statement("CREATE TABLE nsb AS SELECT ts_ns, ts_ns::TIMESTAMP_MS AS ts_ms, ts_ns::TIMESTAMP_S AS ts_s, v FROM "
            "(SELECT TIMESTAMP_NS '2025-01-01 23:59:59.9999996' + INTERVAL (i) MICROSECOND AS ts_ns, i AS v FROM "
            "range(-3, 4) t(i) UNION ALL SELECT TIMESTAMP_NS '1969-12-31 23:59:59.9999996' + INTERVAL (i) "
            "MICROSECOND, 100 + i FROM range(-3, 4) t(i) UNION ALL SELECT TIMESTAMP_NS '2025-01-31 23:59:59.9999995' "
            "+ INTERVAL (i) MICROSECOND, 200 + i FROM range(-3, 4) t(i) UNION ALL SELECT TIMESTAMP_NS "
            "'2025-01-01 00:00:00.0000004' + INTERVAL (i) MICROSECOND, 300 + i FROM range(-3, 4) t(i) UNION ALL "
            "SELECT TIMESTAMP_NS '1969-12-28 23:59:59.9999999' + INTERVAL (i) MICROSECOND, 400 + i FROM range(-3, 4) "
            "t(i) UNION ALL SELECT TIMESTAMP_NS '1970-01-01 00:00:00.0000005', 500 UNION ALL SELECT TIMESTAMP_NS "
            "'1969-12-31 23:59:59.9999985', 501 UNION ALL SELECT NULL, 502);")
for name, selects in (("prec", regular), ("nsb", boundary)):
    diff(t, name, selects, "compressed_materialization", ["shape", "k", "c", "s"])

t.write()
