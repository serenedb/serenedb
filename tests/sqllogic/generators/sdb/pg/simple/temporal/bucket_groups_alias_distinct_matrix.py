from sqllogic_gen import Test
from temporal_gen import diff, shaped

t = Test("Bucket keys reached through aliases, subqueries, CTEs, DISTINCT and "
         "UNION: every grouping returns the same keys, counts and sums with "
         "compressed materialization off and on.")

KEYS = (
    "date_trunc('hour', ts)", "date_trunc('day', ts)", "date_trunc('month', ts)", "date_trunc('week', ts)::DATE",
    "time_bucket(INTERVAL '15 minutes', ts)", "strftime(ts, '%Y-%m-%d')", "strftime(ts, '%Y-%m')",
    "concat(year(ts), '-Q', quarter(ts))", "date_trunc('hour', tstz)", "date_trunc('day', tstz)",
    "time_bucket(INTERVAL '1 day', tstz, 'Asia/Tokyo')", "floor(price / 100) * 100", "round(price / 50)",
    "CASE WHEN price < 100 THEN 'low' WHEN price < 500 THEN 'mid' ELSE 'high' END",
    "epoch_ms(date_trunc('hour', ts))", "to_timestamp(floor(epoch(ts) / 3600) * 3600)",
    "date_bin(INTERVAL '2 hours', ts, TIMESTAMP '2000-01-01 01:00')", "date_trunc('day', d)", "sev", "ts",
)
ALIASED = (
    ("subquery", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT {e} AS k, v FROM g) GROUP BY k"),
    ("cte", "WITH x AS (SELECT {e} AS k, v FROM g) SELECT k, count(*) AS c, sum(v) AS s FROM x GROUP BY k"),
    ("cte chain", "WITH x AS (SELECT {e} AS k, v FROM g), y AS (SELECT * FROM x) "
                  "SELECT k, count(*) AS c, sum(v) AS s FROM y GROUP BY k"),
    ("filtered subquery", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT {e} AS k, v FROM g) WHERE v > 100 "
                          "GROUP BY k"),
    ("subquery filter inside", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT {e} AS k, v FROM g "
                               "WHERE v % 3 = 0) GROUP BY k"),
    ("multi ref", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT {e} AS k, v FROM g) GROUP BY k "
                  "HAVING min(k) IS NOT NULL OR min(k) IS NULL"),
    ("alias reused", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT {e} AS k, {e} AS k2, v FROM g) GROUP BY k"),
    ("select star", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT *, {e} AS k FROM g) GROUP BY k"),
    ("union all", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT {e} AS k, v FROM g UNION ALL "
                  "SELECT {e}, v FROM g) GROUP BY k"),
    ("distinct", "SELECT k, 1 AS c, 0 AS s FROM (SELECT DISTINCT {e} AS k FROM g)"),
    ("distinct alias", "SELECT k, 1 AS c, 0 AS s FROM (SELECT DISTINCT k FROM (SELECT {e} AS k, v FROM g))"),
    ("distinct filtered", "SELECT k, 1 AS c, 0 AS s FROM (SELECT DISTINCT {e} AS k FROM g WHERE v > 100)"),
    ("distinct ordered", "SELECT k, 1 AS c, 0 AS s FROM (SELECT DISTINCT {e} AS k FROM g ORDER BY k)"),
    ("distinct on", "SELECT k, 1 AS c, 0 AS s FROM (SELECT DISTINCT ON ({e}) {e} AS k FROM g ORDER BY {e})"),
    ("distinct with sev", "SELECT k1::VARCHAR || '/' || k2::VARCHAR AS k, 1 AS c, 0 AS s FROM "
                          "(SELECT DISTINCT {e} AS k1, sev AS k2 FROM g)"),
)
PAIRS = (
    ("date_trunc('day', ts)", "date_trunc('hour', ts)"), ("date_trunc('month', ts)", "date_trunc('day', ts)"),
    ("strftime(ts, '%Y-%m')", "date_trunc('day', ts)"), ("date_trunc('day', tstz)", "date_trunc('hour', tstz)"),
    ("concat(year(ts), '-Q', quarter(ts))", "date_trunc('month', ts)"),
    ("time_bucket(INTERVAL '1 day', ts)", "time_bucket(INTERVAL '15 minutes', ts)"),
    ("date_trunc('day', ts)::DATE", "date_trunc('hour', ts)"),
)
PAIRED = (
    ("alias pair", "SELECT k1::VARCHAR || '/' || k2::VARCHAR AS k, c, s FROM (SELECT k1, k2, count(*) AS c, "
                   "sum(v) AS s FROM (SELECT {k1} AS k1, {k2} AS k2, v FROM g) GROUP BY k1, k2)"),
    ("cte pair", "WITH x AS (SELECT {k1} AS k1, {k2} AS k2, v FROM g) SELECT k1::VARCHAR || '/' || k2::VARCHAR "
                 "AS k, c, s FROM (SELECT k1, k2, count(*) AS c, sum(v) AS s FROM x GROUP BY k1, k2)"),
    ("distinct pair", "SELECT k1::VARCHAR || '/' || k2::VARCHAR AS k, 1 AS c, 0 AS s FROM "
                      "(SELECT DISTINCT {k1} AS k1, {k2} AS k2 FROM g)"),
    ("distinct pair alias", "SELECT k1::VARCHAR || '/' || k2::VARCHAR AS k, 1 AS c, 0 AS s FROM "
                            "(SELECT DISTINCT k1, k2 FROM (SELECT {k1} AS k1, {k2} AS k2 FROM g))"),
    ("pair one aliased", "SELECT k1::VARCHAR || '/' || k2::VARCHAR AS k, c, s FROM (SELECT k1, {k2} AS k2, "
                         "count(*) AS c, sum(v) AS s FROM (SELECT {k1} AS k1, ts, tstz, v FROM g) GROUP BY 1, 2)"),
)
WRAPPED_KEYS = (
    "date_trunc('hour', ts)", "date_trunc('day', ts)", "strftime(ts, '%Y-%m-%d')", "date_trunc('hour', tstz)",
    "time_bucket(INTERVAL '15 minutes', ts)", "floor(price / 100) * 100", "epoch_ms(date_trunc('hour', ts))",
)
WRAPPED = (
    ("wrapped concat", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT b::VARCHAR || '!' AS k, v FROM "
                       "(SELECT {e} AS b, v FROM g)) GROUP BY k"),
    ("wrapped outer", "SELECT b::VARCHAR || '!' AS k, count(*) AS c, sum(v) AS s FROM (SELECT {e} AS b, v FROM g) "
                      "GROUP BY 1"),
    ("wrapped cte", "WITH x AS (SELECT {e} AS b, v FROM g) SELECT concat('[', b, ']') AS k, count(*) AS c, "
                    "sum(v) AS s FROM x GROUP BY 1"),
    ("wrapped multi ref", "SELECT b::VARCHAR || '!' AS k, count(*) AS c, sum(v) + 0 * count(b) AS s FROM "
                          "(SELECT {e} AS b, v FROM g) GROUP BY 1"),
    ("union all", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT {e} AS k, v FROM g UNION ALL "
                  "SELECT {e}, v FROM g WHERE sev < 3) GROUP BY k"),
    ("union all three", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT {e} AS k, v FROM g WHERE sev = 0 "
                        "UNION ALL SELECT {e}, v FROM g WHERE sev = 1 UNION ALL SELECT {e}, v FROM g WHERE sev > 1) "
                        "GROUP BY k"),
    ("union all passthrough", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT k, v FROM (SELECT {e} AS k, v "
                              "FROM g) UNION ALL SELECT k, v FROM (SELECT {e} AS k, v FROM g WHERE v % 2 = 0)) "
                              "GROUP BY k"),
    ("union all extra column", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT {e} AS k, sev, v FROM g "
                               "UNION ALL SELECT {e}, sev, v FROM g) GROUP BY k"),
    ("union distinct", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT {e} AS k, v FROM g UNION "
                       "SELECT {e}, v FROM g) GROUP BY k"),
)
UNIONS = (
    ("union all different shapes", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT date_trunc('hour', ts) AS k, "
                                   "v FROM g UNION ALL SELECT date_trunc('day', ts), v FROM g) GROUP BY k"),
    ("union all shifted branch", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT date_trunc('hour', ts) AS k, v "
                                 "FROM g UNION ALL SELECT date_trunc('hour', ts - INTERVAL '3 hours'), v FROM g) "
                                 "GROUP BY k"),
    ("union all mixed columns", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT date_trunc('hour', ts) AS k, v "
                                "FROM g UNION ALL SELECT date_trunc('hour', tstz::TIMESTAMP), v FROM g) GROUP BY k"),
    ("union all with constant branch", "SELECT k, count(*) AS c, sum(v) AS s FROM (SELECT date_trunc('hour', ts) "
                                       "AS k, v FROM g UNION ALL SELECT TIMESTAMP '2025-01-15 10:00', 7) GROUP BY k"),
    ("union all wrapped", "SELECT k::VARCHAR || '!' AS k, c, s FROM (SELECT k, count(*) AS c, sum(v) AS s FROM "
                          "(SELECT date_trunc('hour', ts) AS k, v FROM g UNION ALL SELECT date_trunc('hour', ts), v "
                          "FROM g) GROUP BY k)"),
    ("union all two keys", "SELECT k1::VARCHAR || '/' || k2::VARCHAR AS k, c, s FROM (SELECT k1, k2, count(*) AS c, "
                           "sum(v) AS s FROM (SELECT date_trunc('day', ts) AS k1, date_trunc('hour', ts) AS k2, v "
                           "FROM g UNION ALL SELECT date_trunc('day', ts), date_trunc('hour', ts), v FROM g) "
                           "GROUP BY k1, k2)"),
)

shapes = [shaped(f"{label} {e}", inner.replace("{e}", e)) for e in KEYS for label, inner in ALIASED]
shapes += [shaped(f"{label} {k1} + {k2}", inner.replace("{k1}", k1).replace("{k2}", k2))
           for k1, k2 in PAIRS for label, inner in PAIRED]
shapes += [shaped(f"{label} {e}", inner.replace("{e}", e)) for e in WRAPPED_KEYS for label, inner in WRAPPED]
shapes += [shaped(label, inner) for label, inner in UNIONS]

t.statement("SET TimeZone = 'Europe/Berlin';")
t.statement("CREATE TABLE g AS SELECT TIMESTAMP '2024-12-28 00:00:00' + INTERVAL (i * 7) MINUTE AS ts, "
            "(TIMESTAMP '2024-12-28 00:00:00' + INTERVAL (i * 7) MINUTE)::TIMESTAMPTZ AS tstz, "
            "(DATE '2024-12-28' + (i % 300)::INTEGER)::DATE AS d, (i % 1000)::DOUBLE AS price, "
            "(i % 5)::SMALLINT AS sev, i AS v FROM range(50000) t(i);")
t.statement("INSERT INTO g VALUES (NULL, NULL, NULL, NULL, NULL, -1);")
diff(t, "a", shapes, "compressed_materialization", ["shape", "k", "c", "s"])
t.query("SELECT k::VARCHAR AS k, c FROM (SELECT k, count(*) AS c FROM (SELECT date_trunc('hour', ts) AS k, v FROM g) "
        "WHERE v BETWEEN 400 AND 430 GROUP BY k) ORDER BY 1;", ["k", "c"], 5)
t.query("SELECT k::VARCHAR AS k FROM (SELECT DISTINCT date_trunc('day', tstz) AS k FROM g WHERE v BETWEEN 400 AND 900) "
        "ORDER BY 1;", ["k"], 4)

t.write()
