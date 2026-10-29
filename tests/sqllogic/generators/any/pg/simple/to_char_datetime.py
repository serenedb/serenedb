from sqllogic_gen import Test

GRIDS = [
    ("(VALUES (TIMESTAMP '1997-02-10 17:32:01'), (to_date('0097-02-16 BC', 'YYYY-MM-DD BC') + TIME '07:32:01'), "
     "(TIMESTAMP '2000-12-31 00:00:00'), (TIMESTAMP '10000-01-01 12:00:00')) v(t)", 4,
     ["'DAY Day day DY Dy dy MONTH Month month RM rm MON Mon mon FMRM FMrm'"]),
    ("(VALUES (TIMESTAMP '1997-02-10 17:32:01'), (to_date('0097-02-16 BC', 'YYYY-MM-DD BC') + TIME '07:32:01'), "
     "(TIMESTAMP '2000-12-31 00:00:00'), (TIMESTAMP '2024-12-30 00:00:00'), (TIMESTAMP '10000-01-01 12:00:00')) v(t)",
     5, ["'Y,YYY YYYY YYY YY Y CC Q MM WW DDD DD D J IYYY IYY IY I IW IDDD ID'",
         "'FMY,YYY FMYYYY FMYYY FMYY FMY FMCC FMQ FMMM FMWW FMDDD FMDD FMD FMJ FMIYYY FMIW FMIDDD FMID'"]),
    ("(VALUES (TIMESTAMP '1997-02-10 17:32:01'), (to_date('0097-02-16 BC', 'YYYY-MM-DD BC') + TIME '07:32:01'), "
     "(TIMESTAMP '2000-12-31 00:00:00')) v(t)", 3,
     ["'YYYY A.D. YYYY a.d. YYYY B.C. YYYY b.c. AD ad BC bc'",
      "'HH:MI:SS A.M. a.m. P.M. p.m. AM am PM pm'"]),
    ("(VALUES (TIMESTAMP '2024-03-01 00:00:00'), (TIMESTAMP '2024-03-02 12:00:00'), "
     "(TIMESTAMP '2024-03-03 01:02:03'), (TIMESTAMP '2024-03-04 13:14:15'), (TIMESTAMP '2024-03-11 23:59:59'), "
     "(TIMESTAMP '2024-03-12 00:00:00'), (TIMESTAMP '2024-03-13 00:00:00'), (TIMESTAMP '2024-03-21 00:00:00'), "
     "(TIMESTAMP '2024-03-22 00:00:00'), (TIMESTAMP '2024-03-23 00:00:00')) v(t)", 10,
     ["'DDTH DDth FMDDTH HH HH12 HH24 FMHH12 FMHH24 MI SS FMMI FMSS SSSS SSSSS'"]),
    ("(VALUES (TIMESTAMP '2018-11-02 12:34:56'), (TIMESTAMP '2018-11-02 12:34:56.78'), "
     "(TIMESTAMP '2018-11-02 12:34:56.78901')) v(t)", 3,
     ["'FF1 FF2 FF3 FF4 FF5 FF6  ff1 ff2 ff3 ff4 ff5 ff6  MS US'"]),
]

SCALARS = [
    "to_char(TIMESTAMP '2018-11-02 12:34:56.789012', 'YYYYTH YYYYth Jth IDTH DTH WWth Wth Qth MMth CCth "
    "HH24th MIth SSth MSth USth FF1th SSSSth IYYYth IWth IDDDth DDDth YYth Yth YYYth Y,YYYth')",
    "to_char(TIMESTAMP '2018-11-02 12:34:56', 'YYYY-MM-DD FX HH24:MI:SS')",
    "to_char(TIMESTAMP '2018-11-02 12:34:56', 'YYYYFM')",
    """to_char(TIMESTAMP '2018-11-02 12:34:56', 'YYYY "abc')""",
    r"""to_char(TIMESTAMP '2018-11-02 12:34:56', 'YYYY "a\"b" \"MM\" DD')""",
    r"""to_char(TIMESTAMP '2018-11-02', E'YYYY "x\\')""",
    r"to_char(TIMESTAMP '2018-11-02', 'YYYY\')",
    r"to_char(TIMESTAMP '2018-11-02', 'YYYY\x')",
    "to_char(TIMESTAMP '2018-11-02', 'YYYY~MM' || chr(127) || 'DD é')",
    r"""to_char(TIMESTAMP '1997-02-10 17:32:01', E'"HH:MI:SS is" HH:MI:SS "\\"text between quote marks\\""')""",
    "to_char(TIMESTAMP '1997-02-10 17:32:01', 'HH24--text--MI--text--SS')",
    "to_char(INTERVAL '1 year 2 months 3 days 04:05:06.789', "
    "'YYYY Y,YYY YYY YY Y CC MM DD DDD HH HH12 HH24 MI SS MS US SSSS WW W J Q IYYY')",
    "to_char(INTERVAL '-1 year -2 months -3 days -04:05:06.789', "
    "'YYYY Y,YYY YYY YY Y CC MM DD DDD HH HH12 HH24 MI SS MS US SSSS WW W J Q FMYYYY FMCC')",
    "to_char(INTERVAL '250 years', 'CC FMCC')",
    "to_char(INTERVAL '-250 years', 'CC FMCC')",
    "to_char(INTERVAL '10000 years', 'CC FMCC')",
    "to_char(INTERVAL '-10000 years', 'CC FMCC')",
    "to_char(INTERVAL '9999 years', 'CC')",
    "to_char(TIME '13:14:15.123456', 'HH HH12 HH24 MI SS MS US SSSS AM am A.M. a.m. PM pm FF1 FF2 FF3 FF4 FF5 FF6')",
    "to_char(TIME '00:00:00', 'HH12 AM')",
    "to_char(INTERVAL '1 day', '')",
    "to_char(TIME '01:00', '')",
    "to_char(TIMESTAMPTZ '2020-01-01 00:00+00', '')",
    "to_char(TIMESTAMP 'infinity', 'YYYY')",
    "to_char(TIMESTAMPTZ '-infinity', 'YYYY')",
    "to_char(TIMESTAMPTZ 'infinity', '')",
    "to_char(TIMESTAMP '2020-01-01', '')",
]

MONTHS = ["to_char(i * interval '1mon', 'rm')", "to_char(i * interval '1mon', 'RM')",
          "to_char(i * interval '1mon', 'FMRM')", "to_char(i * interval '1mon', 'Q')"]

REJECTED = [f"to_char(INTERVAL '1 day', '{form}')" for form in (
    "TZ", "tz", "TZH", "TZM", "OF", "A.D.", "AD", "a.d.", "ad", "Month", "Mon", "Day", "Dy", "D", "ID")]
REJECTED.append("to_char(TIME '01:00', 'Month')")

ZONE_NOON = ("SELECT to_char(TIMESTAMPTZ '2012-12-12 12:00:00+00', "
             "'YYYY-MM-DD HH24:MI:SS TZ tz OF FMOF TZH:TZM') AS a")
ZONES = [
    ("UTC", ZONE_NOON, 1),
    ("GMT", ZONE_NOON, 1),
    ("Asia/Kathmandu", ZONE_NOON, 1),
    ("America/Sao_Paulo", ZONE_NOON, 1),
    ("Etc/GMT-14", ZONE_NOON, 1),
    ("America/St_Johns", "SELECT to_char(TIMESTAMPTZ '2012-12-12 12:00:00+00', "
     "'YYYY-MM-DD HH24:MI:SS OF FMOF TZH:TZM') AS a", 1),
    ("Europe/Berlin", "SELECT to_char(t, 'YYYY-MM-DD HH24:MI:SS OF') AS a FROM (VALUES "
     "(TIMESTAMPTZ '2021-03-28 00:30:00+00'), (TIMESTAMPTZ '2021-03-28 01:30:00+00'), "
     "(TIMESTAMPTZ '2021-10-31 00:30:00+00'), (TIMESTAMPTZ '2021-10-31 01:30:00+00')) v(t) ORDER BY t", 4),
    ("America/New_York", "SELECT to_char(t, 'YYYY-MM-DD HH24:MI:SS.US OF TZH:TZM') AS a FROM (VALUES "
     "(TIMESTAMPTZ '1800-01-01 00:00:00.0005+00'), (TIMESTAMPTZ '1800-01-01 00:00:00+00'), "
     "(TIMESTAMPTZ '2400-07-01 00:00:00.0005+00'), (TIMESTAMPTZ '2012-12-12 12:00:00+00')) v(t) ORDER BY t", 4),
    ("Etc/GMT-1", "SELECT to_char(TIMESTAMPTZ '294247-01-10 02:00:54.775807+00', "
     "'YYYY-MM-DD HH24:MI:SS.US') AS a", 1),
]

t = Test("to_char of timestamps, times and intervals, from PostgreSQL's horology and interval "
         "tests: each value list under each format of its group, the one-off cases, the formats "
         "an interval rejects and the output in several time zones, every answer recorded from "
         "PostgreSQL.")
t.statement("SET TimeZone = 'UTC'")
t.section("Value lists under the formats of their group.")
for values, rows, formats in GRIDS:
    for form in formats:
        t.query(f"SELECT to_char(t, {form}) AS a FROM {values} ORDER BY t", ["a"], rows)
for expression in MONTHS:
    t.query(f"SELECT i, {expression} AS a FROM generate_series(-13, 13) i ORDER BY i", ["i", "a"], 27)
t.query("SELECT to_char(t, f) AS a FROM (VALUES (TIMESTAMP '2020-01-02 03:04:05', 'YYYY'), "
        "(TIMESTAMP '2020-01-02 03:04:05', 'YYYY'), (TIMESTAMP '2021-01-02 03:04:05', 'MM/DD'), "
        "(TIMESTAMP '2022-01-02 03:04:05', 'HH24')) v(t, f) ORDER BY a", ["a"], 4)
t.section("One-off values and formats.")
for expression in SCALARS:
    t.query(f"SELECT {expression} AS a", ["a"])
t.section("Formats an interval or a time rejects.")
for expression in REJECTED:
    t.query(f"SELECT {expression} AS a", ["a"])
t.section("Output in other time zones.")
for zone, sql, rows in ZONES:
    t.statement(f"SET TimeZone = '{zone}'")
    t.query(sql, ["a"], rows)
t.statement("SET TimeZone = 'UTC'")
t.write()
