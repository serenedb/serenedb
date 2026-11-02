from sqllogic_gen import Test
from temporal_gen import diff, predicate

t = Test("Predicates on monotone functions of temporal and numeric columns: every "
         "comparison with a constant, including infinities, NULL and values past "
         "the column's range, and with the same function of a constant around a "
         "DST transition in that transition's zone, counts and sums the same rows "
         "with the expression rewriter off and on.")

FAMILIES = (
    (("date_trunc('day', ts)", "date_trunc('hour', ts)", "date_trunc('month', ts)",
      "time_bucket(INTERVAL '15 minutes', ts)", "time_bucket(INTERVAL '1 day', ts, TIMESTAMP '2000-01-01 06:00')",
      "date_bin(INTERVAL '2 hours', ts, TIMESTAMP '2000-01-01 01:00')", "date_trunc('day', ts) + INTERVAL '6 hours'",
      "d::TIMESTAMP", "ts::DATE::TIMESTAMP", "date_trunc('day', tsms)::TIMESTAMP", "ts + INTERVAL '90 minutes'",
      "ts - INTERVAL '1 month'"),
     ("=", ">=", ">", "<", "<=", "<>"),
     ("TIMESTAMP '2025-01-15'", "TIMESTAMP '2025-01-15 12:00'", "TIMESTAMP '2024-01-01'", "TIMESTAMP '2030-01-01'",
      "'infinity'::TIMESTAMP", "'-infinity'::TIMESTAMP", "NULL::TIMESTAMP")),
    (("ts::DATE", "tstz::DATE", "date_trunc('day', ts)::DATE", "last_day(ts)", "d + 1", "date_trunc('month', d)::DATE"),
     ("=", ">=", "<", "<>"),
     ("DATE '2025-01-15'", "DATE '2030-01-01'", "'infinity'::DATE", "NULL::DATE")),
    (("year(ts)", "year(tstz)", "year(d)", "date_part('year', ts)", "decade(ts)", "isoyear(ts)", "century(ts)"),
     ("=", ">=", "<", ">", "<="),
     ("2025", "2024", "2026", "NULL")),
    (("epoch_ms(ts)", "epoch_us(ts)", "epoch(ts)", "epoch_ms(tstz)", "date_part('epoch', ts)"),
     ("=", ">=", "<"),
     ("1736899200000", "1736899200000.5", "-1", "9223372036854775807")),
    (("floor(price / 100)", "ceil(price / 100)", "trunc(price / 100)", "round(price / 100)", "floor(price) // 100",
      "price // 100", "floor(price / 100) * 100", "floor(log10(price + 1))", "round(price, -2)", "-price"),
     ("=", ">=", "<"),
     ("3", "3.5", "0", "-1", "1e300", "'inf'::DOUBLE", "'nan'::DOUBLE", "NULL")),
    (("v // 1000", "v / 1000", "floor(v / 1000.0)", "round(v, -3)", "(v + 5) // 10", "(v * 2) // 100",
      "v::DOUBLE / 1000", "sev::BIGINT * 1000"),
     ("=", ">=", "<"),
     ("3", "0", "-1", "NULL")),
)
SHAPES = (
    ("between day", "date_trunc('day', ts) BETWEEN TIMESTAMP '2025-01-15' AND TIMESTAMP '2025-01-20'"),
    ("between day exclusive",
     "date_trunc('day', ts) > TIMESTAMP '2025-01-15' AND date_trunc('day', ts) < TIMESTAMP '2025-01-20'"),
    ("between unaligned", "date_trunc('day', ts) BETWEEN TIMESTAMP '2025-01-15 06:00' AND TIMESTAMP '2025-01-20 18:00'"),
    ("between date cast", "ts::DATE BETWEEN DATE '2025-01-15' AND DATE '2025-01-20'"),
    ("between epoch", "epoch_ms(ts) BETWEEN 1736899200000 AND 1736985600000"),
    ("between year", "year(ts) BETWEEN 2024 AND 2025"),
    ("between bins", "floor(price / 100) BETWEEN 2 AND 4"),
    ("in days",
     "date_trunc('day', ts) IN (TIMESTAMP '2025-01-15', TIMESTAMP '2025-01-17', TIMESTAMP '2025-01-15 12:00')"),
    ("in with null", "date_trunc('day', ts) IN (TIMESTAMP '2025-01-15', NULL)"),
    ("in years", "year(ts) IN (2024, 2025, 2027)"),
    ("in bins", "floor(v / 1000) IN (1, 2, 5)"),
    ("not in", "date_trunc('day', ts) NOT IN (TIMESTAMP '2025-01-15', TIMESTAMP '2025-01-17')"),
    ("or context", "date_trunc('day', ts) = TIMESTAMP '2025-01-15' OR sev = 3"),
    ("or nulled", "year(ts) = 2025 OR sev = 3"),
    ("and context", "year(ts) = 2025 AND floor(price / 100) = 3"),
    ("not context", "NOT (date_trunc('day', ts) = TIMESTAMP '2025-01-15')"),
    ("not nulled", "NOT (year(ts) = 2024)"),
    ("nested not", "NOT (year(ts) = 2025 OR sev = 3)"),
    ("case in filter", "CASE WHEN date_trunc('day', ts) = TIMESTAMP '2025-01-15' THEN sev = 1 ELSE sev = 2 END"),
    ("coalesce in filter", "coalesce(year(ts) = 2025, false)"),
    ("is distinct", "date_trunc('day', ts) IS DISTINCT FROM TIMESTAMP '2025-01-15'"),
    ("is not distinct", "date_trunc('day', ts) IS NOT DISTINCT FROM TIMESTAMP '2025-01-15'"),
    ("reversed", "TIMESTAMP '2025-01-15' = date_trunc('day', ts)"),
    ("reversed lt", "TIMESTAMP '2025-01-15' < date_trunc('day', ts)"),
    ("cast const", "date_trunc('day', ts) = '2025-01-15'::TIMESTAMP"),
    ("string const", "date_trunc('day', ts) = '2025-01-15'"),
    ("date const", "date_trunc('day', ts) = DATE '2025-01-15'"),
    ("having", "1 = 1"),
)
TRANSITIONS = (
    ("America/Havana", "2025-11-02 05:00:00+00"),
    ("Europe/Berlin", "2025-10-26 01:00:00+00"),
    ("Antarctica/Casey", "2010-03-04 15:00:00+00"),
    ("Australia/Lord_Howe", "2025-04-05 15:00:00+00"),
    ("Pacific/Kiritimati", "1994-12-31 10:00:00+00"),
    ("Africa/Cairo", "2023-10-26 21:00:00+00"),
    ("America/Sao_Paulo", "2018-02-18 02:00:00+00"),
)
OFFSETS = ("- INTERVAL '1 day'", "", "+ INTERVAL '1 day'")
OPERATORS = ("=", ">=", "<", "<=", ">")
ZONED = (
    ("date_trunc('day', tstz)", "date_trunc('day', {at})"),
    ("date_trunc('hour', tstz)", "date_trunc('hour', {at})"),
    ("date_trunc('week', tstz)", "date_trunc('week', {at})"),
    ("tstz::DATE", "({at})::DATE"),
    ("time_bucket(INTERVAL '1 day', tstz, '{zone}')", "time_bucket(INTERVAL '1 day', {at}, '{zone}')"),
    ("time_bucket(INTERVAL '6 hours', tstz)", "time_bucket(INTERVAL '6 hours', {at})"),
    ("year(tstz)", "year({at})"),
    ("last_day(tstz)", "last_day({at})"),
    ("epoch_ms(tstz)", "epoch_ms({at})"),
)


def constant_shapes():
    out = []
    for exprs, operators, constants in FAMILIES:
        for constant in constants:
            for op in operators:
                for expr in exprs:
                    where = f"{expr} {op} {constant}"
                    out.append(predicate(where, where, "g"))
    return out + [predicate(label, where, "g") for label, where in SHAPES]


def transition_shapes(zone, ts):
    at = f"TIMESTAMPTZ '{ts}'"
    out = []
    for offset in OFFSETS:
        for op in OPERATORS:
            for column, constant in ZONED:
                column = column.format(zone=zone)
                value = constant.format(zone=zone, at=f"{at} {offset}")
                out.append(predicate(f"{zone} {column} {op} {offset}",
                                     f"zone = '{zone}' AND {column} {op} {value}", "zt"))
    for label, where in (
        ("between", f"tstz::DATE BETWEEN ({at} - INTERVAL '1 day')::DATE AND ({at})::DATE"),
        ("in", f"date_trunc('day', tstz) IN (date_trunc('day', {at}), "
               f"date_trunc('day', {at} + INTERVAL '2 days'))"),
        ("interval", f"tstz + INTERVAL '2 hours' >= {at}"),
        ("or", f"(date_trunc('day', tstz) = date_trunc('day', {at}) OR v % 97 = 0)"),
    ):
        out.append(predicate(f"{zone} {label}", f"zone = '{zone}' AND {where}", "zt"))
    return out


t.part()
t.statement("SET TimeZone = 'Europe/Berlin';")
t.statement("CREATE TABLE g AS SELECT TIMESTAMP '2024-12-28 00:00:00' + INTERVAL (i * 7) MINUTE AS ts, "
            "(TIMESTAMP '2024-12-28 00:00:00' + INTERVAL (i * 7) MINUTE)::TIMESTAMPTZ AS tstz, "
            "(DATE '2024-12-28' + (i % 300)::INTEGER)::DATE AS d, (i % 1000)::DOUBLE AS price, "
            "(i % 5)::SMALLINT AS sev, i AS v, (TIMESTAMP '2024-12-28 00:00:00' + INTERVAL (i * 7) "
            "MINUTE)::TIMESTAMP_MS AS tsms FROM range(50000) t(i);")
t.statement("INSERT INTO g VALUES (NULL, NULL, NULL, NULL, NULL, -1, NULL), ('infinity', 'infinity', "
            "'infinity', 'inf', 1, -2, NULL), ('-infinity', '-infinity', '-infinity', '-inf', 2, -3, NULL), "
            "(TIMESTAMP '2025-01-15 12:00:00', TIMESTAMPTZ '2025-01-15 12:00:00+00', DATE '2025-01-15', "
            "'nan', 3, -4, TIMESTAMP '2025-01-15 12:00:00');")
diff(t, "p", constant_shapes(), "expression_rewriter", ["shape", "c", "s"])
t.query("SELECT sum(CASE WHEN date_trunc('day', ts) = TIMESTAMP '2025-01-15' THEN 1 ELSE 0 END), "
        "sum(CASE WHEN year(ts) = 2025 THEN 1 ELSE 0 END), count(*) FILTER (WHERE date_trunc('day', ts) >= "
        "TIMESTAMP '2025-01-15'), bool_or(year(ts) >= 2030), bool_and(coalesce(date_trunc('hour', ts) < "
        "TIMESTAMP '2030-01-01', true)) FROM g;", ["sum", "sum", "count", "bool_or", "bool_and"])
t.query("SELECT date_trunc('day', ts) = TIMESTAMP '2025-01-15' AS eq, year(ts) >= 2025 AS ge, "
        "floor(price / 100) = 3 AS bin, count(*) FROM g GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;",
        ["eq", "ge", "bin", "count"], 7)
t.query("SELECT k, count(*) FROM (SELECT date_trunc('day', ts) AS k FROM g) GROUP BY k HAVING date_trunc('day', k) = "
        "TIMESTAMP '2025-01-15' OR year(k) = 2030 ORDER BY 1;", ["k", "count"])

t.part()
t.statement("CREATE TABLE zt AS SELECT zone, make_timestamptz(epoch_us(t) + (m - 576) * 300000000 + "
            "(m * 7919) % 300000000) AS tstz, m AS v FROM (VALUES ('Antarctica/Casey', TIMESTAMPTZ "
            "'2010-03-04 15:00:00+00'), ('Antarctica/Casey', TIMESTAMPTZ '2012-02-21 17:00:00+00'), "
            "('America/Havana', TIMESTAMPTZ '2025-11-02 05:00:00+00'), ('America/Havana', TIMESTAMPTZ "
            "'2025-03-09 05:00:00+00'), ('Europe/Berlin', TIMESTAMPTZ '2025-10-26 01:00:00+00'), "
            "('Europe/Berlin', TIMESTAMPTZ '2025-03-30 01:00:00+00'), ('Australia/Lord_Howe', TIMESTAMPTZ "
            "'2025-04-05 15:00:00+00'), ('Pacific/Kiritimati', TIMESTAMPTZ '1994-12-31 10:00:00+00'), "
            "('Africa/Cairo', TIMESTAMPTZ '2023-10-26 21:00:00+00'), ('America/Sao_Paulo', TIMESTAMPTZ "
            "'2018-02-18 02:00:00+00')) a(zone, t), range(0, 1153) r(m);")
for zone, ts in TRANSITIONS:
    t.statement(f"SET TimeZone = '{zone}';")
    diff(t, zone.lower().replace("/", "_"), transition_shapes(zone, ts), "expression_rewriter", ["shape", "c", "s"])
t.statement("SET TimeZone = 'Europe/Berlin';")
t.query("SELECT " + ", ".join(f"date_bin(INTERVAL '2 hours', {value}, TIMESTAMP '2000-01-01 01:00')::VARCHAR"
                              for value in ("'infinity'::TIMESTAMP", "'-infinity'::TIMESTAMP", "NULL::TIMESTAMP",
                                            "TIMESTAMP '2025-01-15 10:20'")) + ";", ["date_bin"] * 4)

t.write()
