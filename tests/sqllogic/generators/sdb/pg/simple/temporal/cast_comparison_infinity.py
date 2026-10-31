from sqllogic_gen import Test
from temporal_gen import predicate

t = Test("Casts of dates and truncated timestamps to TIMESTAMP and TIMESTAMPTZ "
         "compared with infinity, -infinity and finite values, over rows with "
         "infinities and NULL: every predicate counts and sums the same rows with "
         "the expression rewriter off and on.")

FAMILIES = (
    (("last_day(ts)::TIMESTAMP", "d::TIMESTAMP", "date_trunc('month', ts)::DATE::TIMESTAMP"), "TIMESTAMP", ""),
    (("last_day(ts)::TIMESTAMPTZ", "d::TIMESTAMPTZ"), "TIMESTAMPTZ", "+00"),
)
OPERATORS = ("=", "<>", "<", "<=", ">", ">=")
SHAPES = (
    "last_day(ts)::TIMESTAMP BETWEEN TIMESTAMP '-infinity' AND TIMESTAMP 'infinity'",
    "last_day(ts)::TIMESTAMP NOT BETWEEN TIMESTAMP '2025-01-01 12:00:00' AND TIMESTAMP 'infinity'",
    "last_day(ts)::TIMESTAMP IN (TIMESTAMP 'infinity', TIMESTAMP '2025-04-30 00:00:00')",
    "last_day(ts)::TIMESTAMP NOT IN (TIMESTAMP 'infinity', TIMESTAMP '2025-04-30 13:07:13')",
    "NOT (last_day(ts)::TIMESTAMP <= TIMESTAMP 'infinity')",
    "d::TIMESTAMP IS DISTINCT FROM TIMESTAMP 'infinity'",
    "d::TIMESTAMP IS NOT DISTINCT FROM TIMESTAMP '-infinity'",
    "(last_day(ts)::TIMESTAMP > TIMESTAMP '-infinity') AND (v % 3 <> 0)",
)

wheres = []
for exprs, kind, offset in FAMILIES:
    constants = [f"{kind} '{value}'" for value in ("infinity", "-infinity", f"2025-04-15 00:00:00{offset}",
                                                    f"2025-04-15 13:07:13{offset}")]
    wheres += [f"{expr} {op} {constant}" for expr in exprs for op in OPERATORS for constant in constants]
wheres += SHAPES
union = "\nUNION ALL ".join(predicate(where, where, "ci") for where in wheres)

t.statement("SET TimeZone = 'Europe/Berlin';")
t.statement("CREATE TABLE ci AS SELECT i AS v, TIMESTAMP '2025-03-25 00:00:00' + INTERVAL (i * 37) MINUTE AS ts,\n"
            " (TIMESTAMP '2025-03-25 00:00:00' + INTERVAL (i * 37) MINUTE)::DATE AS d FROM range(500) t(i);")
t.statement("INSERT INTO ci VALUES (-1, NULL, NULL), (-2, TIMESTAMP 'infinity', DATE 'infinity'),\n"
            " (-3, TIMESTAMP '-infinity', DATE '-infinity');")
t.statement("SET disabled_optimizers = 'expression_rewriter';")
t.statement(f"CREATE OR REPLACE TABLE ref_ci AS\n{union};")
t.statement("SET disabled_optimizers = '';")
t.statement(f"CREATE OR REPLACE TABLE got_ci AS\n{union};")
t.query("SELECT count(*) AS rows, count(DISTINCT shape) AS shapes,\n"
        "\t(SELECT count(*) FROM (SELECT * FROM ref_ci EXCEPT ALL SELECT * FROM got_ci)) AS lost,\n"
        "\t(SELECT count(*) FROM (SELECT * FROM got_ci EXCEPT ALL SELECT * FROM ref_ci)) AS gained\n"
        "FROM ref_ci;", ["rows", "shapes", "lost", "gained"])
t.query("SELECT shape, c, s FROM got_ci WHERE shape LIKE '%infinity%' ORDER BY shape LIMIT 24;", ["shape", "c", "s"],
        24)

t.write()
