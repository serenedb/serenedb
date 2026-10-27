from sqllogic_gen import Test
from temporal_gen import diff, named

t = Test("Text labels concatenated from date parts (year and quarter, ISO week, "
         "month names, decades) of timestamps, dates and zoned timestamps, with "
         "infinities, before 1900 and in a session at UTC+14: every grouping "
         "returns the same keys, counts and sums with compressed materialization "
         "off and on.")

LABELS = (
    "year({c}) || '-Q' || quarter({c})",
    "year({c}) || 'Q' || quarter({c})",
    "'[' || year({c}) || '-Q' || quarter({c}) || ']'",
    "year({c})::VARCHAR || '-' || month({c})",
    "year({c}) || '/' || dayofyear({c})",
    "isoyear({c}) || '-W' || week({c})",
    "isoyear({c}) || '-W' || week({c}) || '-' || isodow({c})",
    "year({c}) || '-' || month({c}) || '-' || day({c})",
    "year({c}) || '-' || month({c}) || '-' || day({c}) || ' ' || hour({c})",
    "year({c}) || '-' || month({c}) || '-' || day({c}) || ' ' || hour({c}) || ':' || minute({c})",
    "monthname({c}) || ' ' || year({c})",
    "year({c}) || ' ' || monthname({c})",
    "dayname({c}) || ', ' || strftime({c}, '%Y-%m-%d')",
    "strftime({c}, '%Y') || ' Q' || quarter({c})",
    "strftime({c}, '%Y-%m') || '/' || day({c})",
    "decade({c}) || 's'",
    "century({c})::VARCHAR",
    "millennium({c}) || 'M'",
    "'FY' || year({c})",
    "date_part('quarter', {c}) || '/' || date_part('year', {c})",
    "datepart('year', {c}) || '.' || datepart('month', {c})",
    "yearweek({c}) || ''",
    "yearweek({c})::VARCHAR",
    "concat(year({c}), '-', quarter({c}))",
    "concat('y', year({c}), 'm', month({c}))",
    "year({c}) || quarter({c})",
    "quarter({c}) || '-' || month({c})",
    "'Q' || quarter({c})",
    "year({c}) || '-' || week({c})",
    "isoyear({c}) || '-' || month({c})",
    "year({c}) || 'x' || monthname({c})",
    "monthname({c}) || year({c})",
    "year({c}) || '-' || day({c})",
    "year({c}) || '-' || month({c}) || '-' || dayname({c})",
    "year({c}) || '-' || month({c}) || ' ' || hour({c})",
    "year({c}) || '-' || dayofweek({c})",
    "concat(year({c}), '-', lpad(month({c})::VARCHAR, 2, '0'))",
    "year({c}) || '-' || month({c}) || '-' || day({c}) || ' ' || hour({c}) || ':' || minute({c}) || ':' || second({c})",
)
EDGES = (
    "year({c}) || '-Q' || quarter({c})",
    "isoyear({c}) || '-W' || week({c})",
    "monthname({c}) || ' ' || year({c})",
    "decade({c}) || 's'",
)
KIRITIMATI = (
    "year(tstz) || '-Q' || quarter(tstz)",
    "year(tstz) || '-' || month(tstz) || '-' || day(tstz)",
    "isoyear(tstz) || '-W' || week(tstz)",
    "year(tstz) || '-' || month(tstz) || '-' || day(tstz) || ' ' || hour(tstz)",
    "dayname(tstz) || ', ' || strftime(tstz, '%Y-%m-%d')",
)
COLUMNS = ("ts", "d", "tstz")

shapes = [named(c, expr.format(c=c), "lab") for c in COLUMNS for expr in LABELS]
shapes += [named(f"{edge} {c}", expr.format(c=c), table) for c in COLUMNS for expr in EDGES
           for edge, table in (("inf", "lab_inf"), ("old", "old"))]

t.statement("SET TimeZone = 'Europe/Berlin';")
t.statement("CREATE TABLE lab AS SELECT TIMESTAMP '2023-11-15 00:00:00' + INTERVAL (i * 173) MINUTE AS ts, "
            "(TIMESTAMP '2023-11-15 00:00:00' + INTERVAL (i * 173) MINUTE)::TIMESTAMPTZ AS tstz, "
            "(DATE '2023-11-15' + (i % 900)::INTEGER)::DATE AS d, i AS v FROM range(0, 6000) t(i);")
t.statement("INSERT INTO lab VALUES (NULL, NULL, NULL, -1);")
t.statement("CREATE TABLE lab_inf AS SELECT * FROM lab;")
t.statement("INSERT INTO lab_inf VALUES (TIMESTAMP 'infinity', TIMESTAMPTZ 'infinity', DATE 'infinity', -2), "
            "(TIMESTAMP '-infinity', TIMESTAMPTZ '-infinity', DATE '-infinity', -3);")
t.statement("CREATE TABLE old AS SELECT TIMESTAMP '1890-01-01 00:00:00' + INTERVAL (i * 977) HOUR AS ts, "
            "(TIMESTAMP '1890-01-01 00:00:00' + INTERVAL (i * 977) HOUR)::TIMESTAMPTZ AS tstz, "
            "(DATE '1890-01-01' + (i * 37)::INTEGER)::DATE AS d, i AS v FROM range(0, 4000) t(i);")
diff(t, "lab", shapes, "compressed_materialization", ["shape", "k", "c", "s"])
t.statement("SET TimeZone = 'Pacific/Kiritimati';")
diff(t, "kir", [named("kiritimati", expr, "lab") for expr in KIRITIMATI], "compressed_materialization",
     ["shape", "k", "c", "s"])
t.statement("SET TimeZone = 'Europe/Berlin';")
t.query("SELECT year(ts) || '-Q' || quarter(ts) AS q, count(*) AS c FROM lab WHERE v < 2000 GROUP BY 1 ORDER BY 1;",
        ["q", "c"], 5)

t.write()
