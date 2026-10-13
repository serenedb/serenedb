
SELECT c.c_custkey AS customer,
  (SELECT "ifresult2".*
    FROM (LATERAL
          (SELECT sum("RTE0"."o_totalprice") AS "sum"
           FROM orders AS "RTE0"
           WHERE "RTE0"."o_custkey" = "custkey") AS "let0"("totalbusiness_1")
          LEFT OUTER JOIN
          (LATERAL
           (SELECT "totalbusiness_1" > (1000000) AS "q4_1") AS "let1"("q4_1")
           LEFT OUTER JOIN
           LATERAL
           ((SELECT 'Platinum' AS "result"
             WHERE NOT "q4_1" IS DISTINCT FROM True)
              UNION ALL
            (SELECT "ifresult6".*
             FROM (LATERAL
                   (SELECT "totalbusiness_1" > (500000) AS "q8_2") AS "let5"("q8_2")
                   LEFT OUTER JOIN
                   LATERAL
                   ((SELECT 'Gold' AS "result"
                     WHERE NOT "q8_2" IS DISTINCT FROM True)
                      UNION ALL
                    (SELECT 'Regular' AS "result"
                     WHERE "q8_2" IS DISTINCT FROM True)
                   ) AS "ifresult6"
                   ON True)
             WHERE "q4_1" IS DISTINCT FROM True)
           ) AS "ifresult2"
           ON True)
          ON True)) AS "service level"
FROM   (SELECT *
        FROM   customer
        ORDER BY c_custkey
        LIMIT :invocations) AS c, LATERAL (SELECT c.c_custkey) AS _(custkey);
