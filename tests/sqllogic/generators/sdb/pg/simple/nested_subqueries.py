from sqllogic_gen import Test

t = Test("Statements nested hundreds of levels deep or hundreds of items wide: "
         "scalar subqueries, derived tables, CTE chains, set operations, IN and "
         "EXISTS lists, aggregates, sort keys and grouping keys. Each plans and "
         "runs without exhausting the stack or taking time quadratic in its size.")


def nest(depth, inner, prefix, suffix):
    return prefix * depth + inner + suffix * depth


def ten(name):
    t.statement(f"CREATE TABLE {name} AS SELECT range AS x FROM range(10)")


def derived(depth, inner):
    return nest(depth, inner, "(SELECT * FROM ", ") t")


def chain(depth, body, source, last):
    ctes = ", ".join(f"c{i} AS {body.format(source if i == 0 else f'c{i - 1}')}" for i in range(depth))
    return f"WITH {ctes} {last.format(f'c{depth - 1}')}"


TOTAL = "SELECT count(*) AS n, sum(x) AS s FROM {}"

t.part()
t.query("SELECT " + nest(160, "1", "(SELECT ", ")") + " AS v", ["v"])
t.query("SELECT 1 IN " + nest(159, "(SELECT 1 WHERE true)", "(SELECT 1 WHERE 1 IN ", ")") + " AS v", ["v"])
inner = derived(799, "(SELECT * FROM (SELECT 1 AS x) t0) t")
t.query("SELECT x FROM " + inner, ["x"])
t.statement("CREATE TABLE ns_t (x INT)")
t.statement("INSERT INTO ns_t SELECT x FROM " + inner)
t.query("SELECT x FROM ns_t", ["x"])
t.statement("DROP TABLE ns_t")
t.query("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n FROM "
        + derived(50, "(SELECT n + 1 AS n FROM r WHERE n < 3) t0") + ") SELECT n FROM r ORDER BY n", ["n"], 3)
t.query("SELECT (SELECT 1) AS a ORDER BY a", ["a"])
t.query("SELECT (SELECT 1) ORDER BY (SELECT 1)", ["?column?"])
t.query("SELECT (SELECT 2) UNION SELECT 1 ORDER BY 1", ["?column?"], 2)
t.query("SELECT (SELECT 2) AS x UNION ALL SELECT 1 ORDER BY x", ["x"], 2)

t.part()
ten("ns_u")
t.query(TOTAL.format("(" + nest(400, "SELECT sum(x) AS x FROM ns_u WHERE x > 5",
                                 "SELECT sum(x) AS x FROM ns_u WHERE x > 5 UNION ALL (", ")") + ") t"), ["n", "s"])
t.statement("DROP TABLE ns_u")
ten("ns_g")
t.query(TOTAL.format(nest(400, "ns_g", "(SELECT DISTINCT x FROM ", ") t")), ["n", "s"])
t.query(TOTAL.format(nest(400, "ns_g", "(SELECT x FROM ", " GROUP BY x) t")), ["n", "s"])
t.statement("DROP TABLE ns_g")
ten("ns_c")
t.query(chain(400, "MATERIALIZED (SELECT x FROM {})", "ns_c", TOTAL), ["n", "s"])
t.statement("DROP TABLE ns_c")
ten("ns_i")
t.query(chain(600, "(SELECT x FROM {})", "ns_i", TOTAL), ["n", "s"])
t.query("SELECT count(*) AS n, sum(x) AS s FROM ns_i t WHERE " + " AND ".join(
    f"t.x IN (SELECT u.x + {i} - {i} FROM ns_i u)" if i % 3 == 0 else
    f"EXISTS (SELECT 1 FROM ns_i u WHERE u.x = {i} % 10)" if i % 3 == 1 else
    f"t.x NOT IN (SELECT u.x + 100 + {i} FROM ns_i u)" for i in range(300)), ["n", "s"])
t.query("SELECT x FROM ns_i t WHERE t.x > 4 AND t.x IN (SELECT u.x FROM ns_i u WHERE u.x % 2 = 0) "
        "AND EXISTS (SELECT 1 FROM ns_i u WHERE u.x = t.x + 1) ORDER BY x", ["x"], 2)
t.statement("DROP TABLE ns_i")

t.part()
ten("ns_w")
t.query(TOTAL.format("(" + "".join(f"WITH c{i} AS (" for i in range(399, -1, -1)) + "SELECT x FROM ns_w"
                     + "".join(f") SELECT x FROM c{i}" for i in range(400)) + ") t"), ["n", "s"])
t.statement("DROP TABLE ns_w")
t.query("WITH " + ", ".join(f"c{i} AS (SELECT {i} AS x)" for i in range(400)) + " " + TOTAL.format(
    "(" + " UNION ALL ".join(f"SELECT x FROM c{i}" for i in range(400) for _ in range(2)) + ") t"), ["n", "s"])
ten("ns_p")
t.query(chain(300, "MATERIALIZED (SELECT x, x + 1 AS y FROM {})", "ns_p",
              "SELECT count(x) AS n, sum(x) AS s FROM {}"), ["n", "s"])
t.statement("DROP TABLE ns_p")
ten("ns_f")
t.query("WITH " + ", ".join("c0 AS MATERIALIZED (SELECT x FROM ns_f)" if i == 0 else
                            f"c{i} AS MATERIALIZED (SELECT x FROM c{i - 1} WHERE x >= {i % 7})"
                            for i in range(400)) + " " + TOTAL.format("c399 WHERE x >= 0"), ["n", "s"])
t.statement("DROP TABLE ns_f")
ten("ns_a")
t.query("SELECT " + " + ".join(f"sum(x + {i})" for i in range(300) for _ in range(2)) + " AS v FROM ns_a", ["v"])
t.statement("DROP TABLE ns_a")

t.part()
ten("ns_o")
t.query("SELECT x FROM ns_o ORDER BY x % 2 DESC, " + ", ".join(f"x + {i}" for i in range(298)) + ", -x",
        ["x"], 10)
t.statement("DROP TABLE ns_o")
ten("ns_s")
t.query(TOTAL.format("(" + nest(200, "SELECT x FROM ns_s", "SELECT x FROM ns_s INTERSECT (", ")") + ") t"),
        ["n", "s"])
t.query(TOTAL.format("(" + nest(201, "SELECT x + 100 AS x FROM ns_s", "SELECT x FROM ns_s EXCEPT (", ")")
                     + ") t"), ["n", "s"])
t.query(TOTAL.format("(" + nest(200, "SELECT x FROM ns_s", "SELECT x FROM ns_s UNION ALL (",
                                " ORDER BY 1 LIMIT 100000)") + ") t"), ["n", "s"])
t.statement("DROP TABLE ns_s")
ten("ns_g")
t.query("SELECT count(*) AS n, sum(g0) AS s FROM (SELECT " + ", ".join(f"x + {i} AS g{i}" for i in range(300))
        + " FROM ns_g GROUP BY " + ", ".join(f"x + {i}" for i in range(300)) + ") t", ["n", "s"])
t.query("SELECT x % 2 AS a, x % 3 AS b, count(*) AS c FROM ns_g GROUP BY ROLLUP (x % 2), x % 3 "
        "ORDER BY a NULLS LAST, b", ["a", "b", "c"], 9)
t.statement("DROP TABLE ns_g")

t.write()
