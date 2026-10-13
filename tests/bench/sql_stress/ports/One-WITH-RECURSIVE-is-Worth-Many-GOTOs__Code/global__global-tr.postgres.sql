
SELECT l.l_orderkey,
  (
  WITH :MODE run("rec?",
                       "res",
                       "counter3",
                       "lArray2",
                       "q4") AS
    (
        SELECT True,
                NULL :: bool,
                1,
                "regions_1" AS "lArray2_1",
                array_length("regions_1", 1) AS "q4_1"
         FROM (SELECT array_agg("RTE2"."n_regionkey") AS "array_agg"
                        FROM lineitem AS "RTE0",
                             supplier AS "RTE1",
                             nation AS "RTE2"("n_nationkey", "n_name", "n_regionkey", "n_comment")
                        WHERE ("RTE0"."l_orderkey" = "orderkey"
                               AND
                               "RTE0"."l_suppkey" = "RTE1"."s_suppkey"
                               AND
                               "RTE1"."s_nationkey" = "RTE2"."n_nationkey")
               ) AS "let9"("regions_1")
          UNION ALL
        (SELECT "result".*
         FROM run AS "run"("rec?",
                           "res",
                           "counter3",
                           "lArray2",
                           "q4"),
              LATERAL
              (SELECT "ifresult1".*
               FROM (LATERAL
                     (SELECT "counter3" <= "q4" AS "pred_2") AS "let0"("pred_2")
                     LEFT OUTER JOIN
                     LATERAL
                     ((SELECT "ifresult5".*
                       FROM (LATERAL
                               (SELECT ("lArray2")["counter3"] <> (("lArray2")[1]) AS "q8_3") AS "let4"("q8_3")
                               LEFT OUTER JOIN
                               LATERAL
                               ((SELECT False,
                                        True AS "result",
                                        "run"."counter3",
                                        "run"."lArray2",
                                        "run"."q4"
                                 WHERE NOT "q8_3" IS DISTINCT FROM True)
                                  UNION ALL
                                (SELECT True,
                                        NULL :: bool,
                                        "counter3" + 1 AS "counter3_4",
                                        "lArray2",
                                        "q4"
                                 WHERE "q8_3" IS DISTINCT FROM True)
                               ) AS "ifresult5"
                               ON True)
                       WHERE NOT "pred_2" IS DISTINCT FROM True)
                        UNION ALL
                      (SELECT False,
                              False AS "result",
                              "run"."counter3",
                              "run"."lArray2",
                              "run"."q4"
                       WHERE "pred_2" IS DISTINCT FROM True)
                     ) AS "ifresult1"
                     ON True)
              ) AS "result"
         WHERE "run"."rec?")
    )
    SELECT "run"."res" AS "res"
    FROM run AS "run"
    WHERE NOT "run"."rec?"
    ) AS "long_distance?"
FROM   (SELECT *
        FROM   lineitem
        ORDER BY l_orderkey
        LIMIT :invocations) AS l, LATERAL (SELECT l_orderkey) AS _(orderkey)
GROUP  BY l.l_orderkey, orderkey;