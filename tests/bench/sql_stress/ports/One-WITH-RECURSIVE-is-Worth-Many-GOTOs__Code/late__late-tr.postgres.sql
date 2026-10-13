
SELECT s.s_name, COUNT(*) AS numwait
FROM   supplier AS s, nation AS n, (SELECT *
                                    FROM orders
                                    WHERE o_orderstatus = 'F'
                                    ORDER BY o_orderkey
                                    LIMIT :invocations) AS o, LATERAL (SELECT s.s_suppkey, o.o_orderkey) AS _(suppkey, orderkey)
WHERE  o.o_orderstatus = 'F'
AND    s.s_nationkey = n.n_nationkey AND n.n_name = 'GERMANY'
AND    (WITH :MODE run("rec?",
                           "res",
                           "blame",
                           "counter3",
                           "lArray2",
                           "multi",
                           "orderkey",
                           "q4",
                           "suppkey") AS
          (
              SELECT True,
                      NULL :: bool,
                      FALSE AS "blame_1",
                      1,
                      "lis_1" AS "lArray2_1",
                      FALSE AS "multi_1",
                      "orderkey",
                      array_length("lis_1", 1) AS "q4_1",
                      "suppkey"
               FROM (SELECT array_agg("RTE0") AS "array_agg"
                     FROM lineitem AS "RTE0"
                     WHERE "RTE0"."l_orderkey" = "orderkey") AS "let16"("lis_1")
                UNION ALL
              (SELECT "result".*
               FROM run AS "run"("rec?",
                                 "res",
                                 "blame",
                                 "counter3",
                                 "lArray2",
                                 "multi",
                                 "orderkey",
                                 "q4",
                                 "suppkey"),
                    LATERAL
                    (SELECT "ifresult1".*
                     FROM (LATERAL
                           (SELECT "counter3" <= "q4" AS "pred_2") AS "let0"("pred_2")
                           LEFT OUTER JOIN
                           LATERAL
                           ((SELECT "ifresult6".*
                             FROM (LATERAL
                                   (SELECT ("lArray2")["counter3"] AS "li_3") AS "let2"("li_3")
                                   LEFT OUTER JOIN
                                   (LATERAL
                                    (SELECT "counter3" + 1 AS "counter3_4") AS "let3"("counter3_4")
                                    LEFT OUTER JOIN
                                    (LATERAL
                                     (SELECT ("multi" OR (("li_3" :: lineitem).l_suppkey) <> "suppkey") AS "multi_4"
                                     ) AS "let4"("multi_4")
                                     LEFT OUTER JOIN
                                     (LATERAL
                                      (SELECT (("li_3" :: lineitem).l_receiptdate) > (("li_3" :: lineitem).l_commitdate) AS "q8_3"
                                      ) AS "let5"("q8_3")
                                      LEFT OUTER JOIN
                                      LATERAL
                                      ((SELECT "ifresult8".*
                                        FROM (LATERAL
                                              (SELECT (("li_3" :: lineitem).l_suppkey) <> "suppkey" AS "q12_4"
                                              ) AS "let7"("q12_4")
                                              LEFT OUTER JOIN
                                              LATERAL
                                              ((SELECT False,
                                                       False AS "result",
                                                       "run"."blame",
                                                       "run"."counter3",
                                                       "run"."lArray2",
                                                       "run"."multi",
                                                       "run"."orderkey",
                                                       "run"."q4",
                                                       "run"."suppkey"
                                                WHERE NOT "q12_4" IS DISTINCT FROM True)
                                                 UNION ALL
                                               (SELECT True,
                                                        NULL :: bool,
                                                        True AS "blame_6",
                                                        "counter3_4",
                                                        "lArray2",
                                                        "multi_4",
                                                        "orderkey",
                                                        "q4",
                                                        "suppkey"
                                                WHERE "q12_4" IS DISTINCT FROM True)
                                              ) AS "ifresult8"
                                              ON True)
                                        WHERE NOT "q8_3" IS DISTINCT FROM True)
                                         UNION ALL
                                       (SELECT True,
                                               NULL :: bool,
                                               "blame",
                                               "counter3_4",
                                               "lArray2",
                                               "multi_4",
                                               "orderkey",
                                               "q4",
                                               "suppkey"
                                        WHERE "q8_3" IS DISTINCT FROM True)
                                      ) AS "ifresult6"
                                      ON True)
                                     ON True)
                                    ON True)
                                   ON True)
                             WHERE NOT "pred_2" IS DISTINCT FROM True)
                              UNION ALL
                            (SELECT False,
                                    ("multi" AND "blame") AS "result",
                                    "run"."blame",
                                    "run"."counter3",
                                    "run"."lArray2",
                                    "run"."multi",
                                    "run"."orderkey",
                                    "run"."q4",
                                    "run"."suppkey"
                             WHERE "pred_2" IS DISTINCT FROM True)
                           ) AS "ifresult1"
                           ON True)
                    ) AS "result"
               WHERE "run"."rec?")
          )
          SELECT "run"."res" AS "res"
          FROM run AS "run"
          WHERE NOT "run"."rec?"
      )
GROUP BY s.s_name
ORDER BY numwait DESC, s_name;