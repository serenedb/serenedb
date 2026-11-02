from sqllogic_gen import Test

t = Test("Statements far larger than usual: a 3000-term AND chain, 48 EXISTS "
         "subqueries, a 2000-column table and a 4000-branch UNION ALL. Each "
         "plans in linear time; a pass quadratic in the statement's size runs "
         "into the timeout.")

t.part()
t.statement("CREATE TABLE ps_t AS SELECT range::INT AS a, (range % 7)::INT AS b FROM range(10000)")
t.query("SELECT count(*) AS c FROM ps_t WHERE " + " AND ".join(f"a <> {i}" for i in range(3000)), ["c"])
t.query("SELECT count(*) AS c FROM ps_t t WHERE " + " AND ".join(
    f"EXISTS (SELECT 1 FROM ps_t s WHERE s.a = t.a + {k})" for k in range(48)), ["c"])
t.statement("DROP TABLE ps_t")

t.part()
t.statement("CREATE TABLE ps_w(" + ", ".join(f"c{i} INT" for i in range(2000)) + ")")
t.statement("INSERT INTO ps_w SELECT " + ", ".join(f"range + {i}" for i in range(2000)) + " FROM range(3)")
t.query("SELECT sum(" + " + ".join(f"c{i}" for i in range(0, 2000, 10)) + ") AS s FROM ps_w", ["s"])
t.query("SELECT count(*) AS c FROM (SELECT * FROM ps_w WHERE c0 >= 1)", ["c"])
t.statement("DROP TABLE ps_w")

t.part()
t.statement("CREATE TABLE ps_d AS SELECT range::INT AS x FROM range(10)")
t.query("SELECT count(*) AS c FROM (" + " UNION ALL ".join(
    f"SELECT DISTINCT x + {i} AS x FROM ps_d" for i in range(4000)) + ") t", ["c"])
t.statement("DROP TABLE ps_d")

t.write()
