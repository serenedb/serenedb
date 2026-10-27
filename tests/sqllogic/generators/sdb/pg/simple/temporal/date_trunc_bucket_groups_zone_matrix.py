from sqllogic_gen import Test
from temporal_gen import diff, labelled

t = Test("Truncations, buckets and formats of zoned timestamps grouped in sessions "
         "of zones with DST, half-hour and quarter-hour offsets, skipped days and "
         "changed rules, over rows around their transitions and before 1970: "
         "every grouping returns the same keys, counts and sums with compressed "
         "materialization off and on.")

ZONES = (
    ("Europe/Berlin", "America/New_York", "America/St_Johns", "Asia/Kolkata", "Asia/Kathmandu",
     "Australia/Lord_Howe", "Pacific/Kiritimati", "Pacific/Chatham", "America/Santiago", "Asia/Beirut",
     "America/Havana", "Etc/GMT+12", "Africa/Casablanca"),
    ("Europe/Dublin", "Asia/Tokyo", "Asia/Amman", "Asia/Gaza", "America/Asuncion", "Asia/Tehran",
     "America/Sao_Paulo", "Pacific/Apia", "Pacific/Kwajalein", "Asia/Pyongyang", "America/Caracas",
     "Europe/Volgograd", "Pacific/Norfolk"),
)
STARTS = (
    "2025-03-07 00:00:00", "2025-03-28 00:00:00", "2025-04-04 00:00:00", "2025-09-05 00:00:00",
    "2025-10-03 00:00:00", "2025-10-24 00:00:00", "2025-10-31 12:00:00", "2024-12-30 12:00:00",
    "2024-02-28 12:00:00", "2025-03-29 00:00:00", "2011-12-29 00:00:00", "1993-08-19 00:00:00",
    "2018-05-03 12:00:00", "2016-04-30 12:00:00", "2021-10-28 12:00:00", "2022-10-27 12:00:00",
    "2024-03-22 12:00:00", "2022-09-20 12:00:00", "2019-02-15 12:00:00", "2020-12-26 12:00:00",
    "2019-10-05 12:00:00",
)
PARTS = ("second", "minute", "hour", "day", "week", "month", "quarter", "year", "decade", "century")
WIDTHS = ("15 minutes", "1 hour", "6 hours", "1 day", "7 days", "1 month", "3 months", "1 year")
FORMATS = ("%Y-%m-%d %H", "%Y-%m-%d", "%G-%V", "%Y-%m", "%y-%m-%d")
OTHERS = (
    ("mn", "monthname(tstz)", "dst"),
    ("dn", "dayname(tstz)", "dst"),
    ("ld", "last_day(tstz)", "dst"),
    ("cast date", "tstz::DATE", "dst"),
    ("old cast date", "tstz::DATE", "old"),
    ("ym", "year(tstz) * 100 + month(tstz)", "dst"),
    ("hour part", "date_part('hour', tstz)", "dst"),
    ("dow", "dayofweek(tstz)", "dst"),
    ("naive to tz hour", "date_trunc('hour', ts::TIMESTAMPTZ)", "naive"),
    ("naive to tz day", "date_trunc('day', ts::TIMESTAMPTZ)", "naive"),
    ("date to tz day", "date_trunc('day', d::TIMESTAMPTZ)", "naive"),
    ("tz to naive hour", "date_trunc('hour', tstz::TIMESTAMP)", "dst"),
    ("tz to naive day", "strftime(tstz::TIMESTAMP, '%Y-%m-%d')", "dst"),
    ("naive tb tz", "time_bucket(INTERVAL '1 day', ts::TIMESTAMPTZ)", "naive"),
)


def shapes(zone):
    out = []
    for part in PARTS:
        out += [labelled(f"dt {part}", f"date_trunc('{part}', tstz)", "dst"),
                labelled(f"old dt {part}", f"date_trunc('{part}', tstz)", "old")]
    for width in WIDTHS:
        bucket = f"time_bucket(INTERVAL '{width}', tstz"
        out += [labelled(f"tb {width}", f"{bucket})", "dst"),
                labelled(f"tbz {width}", f"{bucket}, '{zone}')", "dst"),
                labelled(f"tbz Tokyo {width}", f"{bucket}, 'Asia/Tokyo')", "dst"),
                labelled(f"old tbz {width}", f"{bucket}, '{zone}')", "old")]
    out += [labelled("tb origin", "time_bucket(INTERVAL '1 day', tstz, TIMESTAMPTZ '2025-01-01 00:00:00+00')", "dst"),
            labelled("tb offset", "time_bucket(INTERVAL '1 day', tstz, INTERVAL '6 hours')", "dst")]
    out += [labelled(f"sf {f}", f"strftime(tstz, '{f}')", "dst") for f in FORMATS]
    return out + [labelled(*other) for other in OTHERS]


t.statement("SET TimeZone = 'Etc/UTC';")
t.statement("CREATE TABLE dst AS SELECT tstz, v FROM (" + " UNION ALL ".join(
    f"SELECT TIMESTAMPTZ '{start}+00' + INTERVAL (i * 53) MINUTE, {k * 1000} + i FROM range(0, 100) t(i)"
    for k, start in enumerate(STARTS)) + ") t(tstz, v);")
t.statement("INSERT INTO dst VALUES (NULL, -1);")
t.statement("CREATE TABLE old AS SELECT tstz, v FROM (SELECT TIMESTAMPTZ '1998-12-25 00:00:00+00' + INTERVAL "
            "(i * 173) MINUTE AS tstz, i AS v FROM range(0, 300) t(i) UNION ALL SELECT TIMESTAMPTZ "
            "'1969-12-20 00:00:00+00' + INTERVAL (i * 173) MINUTE, 1000 + i FROM range(0, 300) t(i));")
t.statement("CREATE TABLE naive AS SELECT (tstz AT TIME ZONE 'UTC')::TIMESTAMP AS ts, (tstz AT TIME ZONE "
            "'UTC')::DATE AS d, v FROM dst WHERE tstz IS NOT NULL;")
for part, zones in enumerate(ZONES):
    t.part()
    for k, zone in enumerate(zones, 1 + part * len(zones)):
        t.statement(f"SET TimeZone = '{zone}';")
        diff(t, str(k), shapes(zone), "compressed_materialization", ["shape", "k", "c", "s"])

t.write()
