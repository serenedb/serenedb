

SELECT c.c_custkey AS "customer", c.c_name AS "name",
(
    SELECT "ifresult4".*
    FROM (LATERAL
          (SELECT count(*) AS "count"
                   FROM lineitem AS "RTE0",
                        orders AS "RTE1"
                   WHERE ("RTE0"."l_orderkey" = "RTE1"."o_orderkey"
                          AND
                          "RTE1"."o_custkey" = "custkey"
                          AND
                          "RTE0"."l_shipmode" = ANY (ARRAY['RAIL',
                                                           'TRUCK'] :: bpchar[]))
          ) AS "let0"("ground_1")
          LEFT OUTER JOIN
          (LATERAL
           (SELECT count(*) AS "count"
                    FROM lineitem AS "RTE2",
                         orders AS "RTE3"
                    WHERE ("RTE2"."l_orderkey" = "RTE3"."o_orderkey"
                           AND
                           "RTE3"."o_custkey" = "custkey"
                           AND
                           "RTE2"."l_shipmode" = ANY (ARRAY['AIR',
                                                            'REG AIR'] :: bpchar[]))
           ) AS "let1"("air_1")
           LEFT OUTER JOIN
           (LATERAL
            (SELECT count(*) AS "count"
                     FROM lineitem AS "RTE4",
                          orders AS "RTE5"
                     WHERE ("RTE4"."l_orderkey" = "RTE5"."o_orderkey"
                            AND
                            "RTE5"."o_custkey" = "custkey"
                            AND
                            "RTE4"."l_shipmode" = 'MAIL')
            ) AS "let2"("mail_1")
            LEFT OUTER JOIN
            (LATERAL
             (SELECT ("ground_1" >= "air_1" AND "ground_1" >= "mail_1") AS "q4_1"
             ) AS "let3"("q4_1")
             LEFT OUTER JOIN
             LATERAL
             ((SELECT 'ground' AS "result"
               WHERE NOT "q4_1" IS DISTINCT FROM True)
                UNION ALL
              (SELECT "ifresult7".*
               FROM (LATERAL
                     (SELECT ("air_1" >= "ground_1" AND "air_1" >= "mail_1") AS "q9_2"
                     ) AS "let6"("q9_2")
                     LEFT OUTER JOIN
                     LATERAL
                     ((SELECT 'air' AS "result"
                       WHERE NOT "q9_2" IS DISTINCT FROM True)
                        UNION ALL
                      (SELECT "ifresult10".*
                       FROM (LATERAL
                             (SELECT ("mail_1" >= "ground_1" AND "mail_1" >= "air_1") AS "q14_3"
                             ) AS "let9"("q14_3")
                             LEFT OUTER JOIN
                             LATERAL
                             ((SELECT 'mail' AS "result"
                               WHERE NOT "q14_3" IS DISTINCT FROM True)
                                UNION ALL
                              (SELECT NULL :: text AS "result"
                               WHERE "q14_3" IS DISTINCT FROM True)
                             ) AS "ifresult10"
                             ON True)
                       WHERE "q9_2" IS DISTINCT FROM True)
                     ) AS "ifresult7"
                     ON True)
               WHERE "q4_1" IS DISTINCT FROM True)
             ) AS "ifresult4"
             ON True)
            ON True)
           ON True)
          ON True)
) AS "preferred ship mode"
FROM   (SELECT *
        FROM   customer
        ORDER BY c_custkey
        LIMIT :invocations) AS c, LATERAL (SELECT c_custkey) AS _(custkey);
