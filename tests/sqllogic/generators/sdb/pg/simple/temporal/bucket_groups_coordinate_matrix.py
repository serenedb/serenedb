from sqllogic_gen import Test
from temporal_gen import diff, shaped

t = Test("Groupings by several date parts of one column (year and month, ISO "
         "year and week, parts out of order, as text, through aliases, CTEs and "
         "DISTINCT) on timestamps, zoned timestamps and dates: every grouping "
         "returns the same keys, counts and sums with compressed materialization "
         "off and on.")


def calls(*names):
    return tuple(f"{name}({{c}})" for name in names)


COMBOS = (
    calls("year", "month"), calls("month", "year"), calls("year", "quarter"), calls("year", "month", "day"),
    calls("year", "month", "day", "hour"), calls("year", "month", "day", "hour", "minute"),
    calls("isoyear", "week"), calls("isoyear", "week", "isodow"), calls("year", "dayofyear"),
    calls("year", "monthname"), calls("year", "month", "dayname"),
    ("strftime({c}, '%Y')", "month({c})"),
    ("date_part('year', {c})", "date_part('month', {c})"),
    ("extract(year FROM {c})", "extract(month FROM {c})"),
    ("year({c})", "strftime({c}, '%m')"),
    calls("year", "week"), calls("year", "month", "hour"), calls("month", "day"), calls("year", "decade"),
    calls("decade", "year", "month"), calls("year", "quarter", "month"),
    ("year({c})::VARCHAR", "month({c})::VARCHAR"),
    (*calls("year", "month"), "sev"),
)
ALIASED = (
    ("alias", "SELECT y::VARCHAR || '/' || m::VARCHAR AS k, c, s FROM (SELECT year(ts) AS y, month(ts) AS m, "
              "count(*) AS c, sum(v) AS s FROM g GROUP BY y, m)"),
    ("subquery alias", "SELECT y::VARCHAR || '/' || m::VARCHAR AS k, c, s FROM (SELECT y, m, count(*) AS c, "
                       "sum(v) AS s FROM (SELECT year(ts) AS y, month(ts) AS m, v FROM g) GROUP BY y, m)"),
    ("cte alias", "WITH x AS (SELECT year(ts) AS y, month(ts) AS m, v FROM g) SELECT y::VARCHAR || '/' || "
                  "m::VARCHAR AS k, c, s FROM (SELECT y, m, count(*) AS c, sum(v) AS s FROM x GROUP BY y, m)"),
    ("filtered", "SELECT y::VARCHAR || '/' || m::VARCHAR AS k, c, s FROM (SELECT year(ts) AS y, month(ts) AS m, "
                 "count(*) AS c, sum(v) AS s FROM g WHERE v % 3 = 0 GROUP BY y, m)"),
    ("distinct", "SELECT k, 1 AS c, 0 AS s FROM (SELECT DISTINCT year(ts)::VARCHAR || '/' || "
                 "month(ts)::VARCHAR AS k FROM g)"),
    ("distinct pair", "SELECT y::VARCHAR || '/' || m::VARCHAR AS k, 1 AS c, 0 AS s FROM (SELECT DISTINCT "
                      "year(ts) AS y, month(ts) AS m FROM g)"),
)


def coordinates(label, keys, table):
    joined = " || '/' || ".join(f"k{i}::VARCHAR" for i in range(1, len(keys) + 1))
    columns = ", ".join(f"{key} AS k{i}" for i, key in enumerate(keys, 1))
    groups = ", ".join(str(i) for i in range(1, len(keys) + 1))
    return shaped(label, f"SELECT {joined} AS k, c, s FROM (SELECT {columns}, count(*) AS c, sum(v) AS s "
                         f"FROM {table} GROUP BY {groups})")


shapes = []
for column in ("ts", "tstz", "d"):
    for combo in COMBOS:
        keys = [key.format(c=column) for key in combo]
        shapes.append(coordinates(f"{column} {', '.join(keys)}", keys, "g"))
shapes += [shaped(label, inner) for label, inner in ALIASED]
shapes += [coordinates("two inputs", ["year(ts)", "month(d)"], "g"),
           coordinates("mixed with date_trunc", ["year(ts)", "month(ts)", "date_trunc('day', ts)"], "g"),
           shaped("with having", "SELECT y::VARCHAR || '/' || m::VARCHAR AS k, c, s FROM (SELECT year(ts) AS y, "
                                 "month(ts) AS m, count(*) AS c, sum(v) AS s FROM g GROUP BY y, m "
                                 "HAVING count(*) > 100)")]
for label, parts in (("year month", ("year", "month")), ("year month day", ("year", "month", "day")),
                     ("year month day hour", ("year", "month", "day", "hour")), ("isoyear week", ("isoyear", "week"))):
    shapes.append(coordinates(f"dst {label}", [f"{part}(tstz)" for part in parts], "dst"))

t.statement("SET TimeZone = 'Europe/Berlin';")
t.statement("CREATE TABLE g AS SELECT TIMESTAMP '2023-11-28 00:00:00' + INTERVAL (i * 17) MINUTE AS ts, "
            "(TIMESTAMP '2023-11-28 00:00:00' + INTERVAL (i * 17) MINUTE)::TIMESTAMPTZ AS tstz, "
            "(DATE '2023-11-28' + (i % 700)::INTEGER)::DATE AS d, (i % 5)::SMALLINT AS sev, i AS v "
            "FROM range(60000) t(i);")
t.statement("INSERT INTO g VALUES (NULL, NULL, NULL, NULL, -1);")
t.statement("CREATE TABLE dst AS SELECT tstz, v FROM (SELECT TIMESTAMPTZ '2025-03-28 00:00:00+00' + INTERVAL "
            "(i * 41) MINUTE AS tstz, i AS v FROM range(0, 400) t(i) UNION ALL SELECT TIMESTAMPTZ "
            "'2025-10-24 00:00:00+00' + INTERVAL (i * 41) MINUTE, 1000 + i FROM range(0, 400) t(i) UNION ALL "
            "SELECT TIMESTAMPTZ '2024-12-31 12:00:00+00' + INTERVAL (i * 41) MINUTE, 2000 + i FROM range(0, 100) "
            "t(i));")
diff(t, "c", shapes, "compressed_materialization", ["shape", "k", "c", "s"])
t.query("SELECT year(ts) AS y, month(ts) AS m, count(*) AS c FROM g WHERE v BETWEEN 5000 AND 12000 GROUP BY 1, 2 "
        "ORDER BY 1, 2;", ["y", "m", "c"], 4)
for zone in ("Europe/Berlin", "America/Havana", "Asia/Tokyo", "Australia/Lord_Howe", "Pacific/Kiritimati",
             "Africa/Cairo", "America/Sao_Paulo", "Asia/Kolkata", "Europe/Moscow", "America/St_Johns",
             "Pacific/Chatham", "Asia/Kathmandu", "Africa/Monrovia", "Europe/Amsterdam", "Asia/Tehran", "America/Nuuk",
             "Pacific/Apia", "Antarctica/Casey", "America/Santiago", "Australia/Adelaide", "Etc/UTC", "Europe/Berlin",
             "America/Havana"):
    at = "TIMESTAMPTZ '2025-06-15 12:00:00+00'"
    t.statement(f"SET TimeZone = '{zone}';")
    t.query(f"SELECT '{zone}', date_trunc('day', {at})::VARCHAR, ({at})::DATE::VARCHAR, strftime({at}, "
            "'%Y-%m-%d %H:%M %z'), count(*) FROM (SELECT year(tstz), month(tstz), count(*) FROM dst GROUP BY 1, 2);",
            ["?column?", "date_trunc", "text", "strftime", "count"])

t.write()
