

SELECT p.p_partkey, p.p_name, run.res AS margin
FROM   (SELECT * FROM part ORDER BY p_partkey ASC
        LIMIT :invocations) AS p, LATERAL (SELECT p.p_partkey) AS _(partkey), LATERAL
(
    WITH :MODE run("rec?",
                       "res",
                       "buy",
                       "cheapest",
                       "cheapest_order",
                       "margin",
                       "partkey",
                       "profit",
                       "sell",
                       "this_order") AS
    (
        (SELECT True,
                 NULL :: trade,
                 NULL :: int AS "buy_1",
                 NULL :: numeric AS "cheapest_1",
                 NULL :: int4,
                 NULL :: numeric AS "margin_1",
                 "partkey",
                 NULL :: numeric,
                 NULL :: int AS "sell_1",
                 "this_order_1"
         FROM (SELECT (SELECT ("RTE1"."o_orderkey",
                                    "RTE1"."o_orderdate") :: dated_order AS "row"
                            FROM lineitem AS "RTE0",
                                 orders AS "RTE1"
                            WHERE ("RTE0"."l_orderkey" = "RTE1"."o_orderkey"
                                   AND
                                   "RTE0"."l_partkey" = "partkey")
                            ORDER BY ("RTE1"."o_orderdate") ASC
                            LIMIT 1) AS "this_order_1"
                   ) AS "let4"("this_order_1")
                   )
          UNION ALL
        (SELECT "result".*
         FROM run AS "run"("rec?",
                           "res",
                           "buy",
                           "cheapest",
                           "cheapest_order",
                           "margin",
                           "partkey",
                           "profit",
                           "sell",
                           "this_order"),
              LATERAL
              (SELECT "ifresult7".*
               FROM (LATERAL
                     (SELECT "this_order" IS NOT NULL AS "pred_2") AS "let6"("pred_2")
                     LEFT OUTER JOIN
                     LATERAL
                     ((SELECT "ifresult11".*
                       FROM (LATERAL
                             (SELECT (SELECT min(("RTE2"."l_extendedprice"
                                                  *
                                                  ((1) - "RTE2"."l_discount"))
                                                 *
                                                 ((1) + "RTE2"."l_tax")) AS "min"
                                      FROM lineitem AS "RTE2"
                                      WHERE ("RTE2"."l_orderkey"
                                             =
                                             (("this_order").orderkey)
                                             AND
                                             "RTE2"."l_partkey" = "partkey")) AS "price_3"
                             ) AS "let8"("price_3")
                             LEFT OUTER JOIN
                             (LATERAL
                              (SELECT COALESCE("cheapest", "price_3") AS "cheapest_4"
                              ) AS "let9"("cheapest_4")
                              LEFT OUTER JOIN
                              (LATERAL
                               (SELECT "price_3" <= "cheapest_4" AS "q5_3") AS "let10"("q5_3")
                               LEFT OUTER JOIN
                               LATERAL
                               ((SELECT "ifresult17".*
                                 FROM (LATERAL
                                       (SELECT "price_3" AS "cheapest_9",
                                               ("this_order").orderkey AS "cheapest_order_7",
                                               "price_3" - "price_3" AS "profit_4"
                                       ) AS "let12"("cheapest_9")
                                        LEFT OUTER JOIN
                                         (LATERAL
                                          (SELECT COALESCE("margin", "profit_4") AS "margin_5"
                                          ) AS "let15"("margin_5")
                                          LEFT OUTER JOIN
                                          (LATERAL
                                           (SELECT "profit_4" >= "margin_5" AS "q9_4"
                                           ) AS "let16"("q9_4")
                                           LEFT OUTER JOIN
                                           LATERAL
                                           ((SELECT True,
                                                    NULL :: trade,
                                                    "cheapest_order_7" AS "buy_6",
                                                    "cheapest_9",
                                                    "cheapest_order_7",
                                                    "profit_4" AS "margin_7",
                                                    "partkey",
                                                    "profit_4",
                                                    ("this_order").orderkey AS "sell_6",
                                                    (SELECT ("RTE4"."o_orderkey",
                                                                       "RTE4"."o_orderdate") :: dated_order AS "row"
                                                               FROM lineitem AS "RTE3",
                                                                    orders AS "RTE4"
                                                               WHERE ("RTE3"."l_orderkey"
                                                                      =
                                                                      "RTE4"."o_orderkey"
                                                                      AND
                                                                      "RTE3"."l_partkey" = "partkey"
                                                                      AND
                                                                      "RTE4"."o_orderdate"
                                                                      >
                                                                      (("this_order").orderdate))
                                                               ORDER BY ("RTE4"."o_orderdate") ASC
                                                               LIMIT 1) AS "this_order_7"
                                             WHERE "q9_4")
                                              UNION ALL
                                            (SELECT True,
                                                   NULL :: trade,
                                                   "buy",
                                                   "cheapest_9",
                                                   "cheapest_order_7",
                                                   "margin_5",
                                                   "partkey",
                                                   "profit_4",
                                                   "sell",
                                                   (SELECT ("RTE4"."o_orderkey",
                                                                    "RTE4"."o_orderdate") :: dated_order AS "row"
                                                            FROM lineitem AS "RTE3",
                                                                 orders AS "RTE4"
                                                            WHERE ("RTE3"."l_orderkey"
                                                                   =
                                                                   "RTE4"."o_orderkey"
                                                                   AND
                                                                   "RTE3"."l_partkey" = "partkey"
                                                                   AND
                                                                   "RTE4"."o_orderdate"
                                                                   >
                                                                   (("this_order").orderdate))
                                                            ORDER BY ("RTE4"."o_orderdate") ASC
                                                            LIMIT 1) AS "this_order_7"
                                             WHERE NOT "q9_4")
                                           ) AS "ifresult17"
                                         ON True)
                                        ON True)
                                       ON True)
                                 WHERE "q5_3")
                                  UNION ALL
                                (SELECT "ifresult28".*
                                 FROM (LATERAL
                                       (SELECT "price_3" - "cheapest_4" AS "profit_4"
                                       ) AS "let25"("profit_4")
                                       LEFT OUTER JOIN
                                       (LATERAL
                                        (SELECT COALESCE("margin", "profit_4") AS "margin_5"
                                        ) AS "let26"("margin_5")
                                        LEFT OUTER JOIN
                                        (LATERAL
                                         (SELECT "profit_4" >= "margin_5" AS "q9_4"
                                         ) AS "let27"("q9_4")
                                         LEFT OUTER JOIN
                                         LATERAL
                                         ((SELECT True,
                                                  NULL :: trade,
                                                  "cheapest_order" AS "buy_6",
                                                  "cheapest_4",
                                                  "cheapest_order",
                                                  "profit_4" AS "margin_7",
                                                  "partkey",
                                                  "profit_4",
                                                  ("this_order").orderkey AS "sell_6",
                                                  (SELECT ("RTE4"."o_orderkey",
                                                                     "RTE4"."o_orderdate") :: dated_order AS "row"
                                                             FROM lineitem AS "RTE3",
                                                                  orders AS "RTE4"
                                                             WHERE ("RTE3"."l_orderkey"
                                                                    =
                                                                    "RTE4"."o_orderkey"
                                                                    AND
                                                                    "RTE3"."l_partkey" = "partkey"
                                                                    AND
                                                                    "RTE4"."o_orderdate"
                                                                    >
                                                                    (("this_order").orderdate))
                                                             ORDER BY ("RTE4"."o_orderdate") ASC
                                                             LIMIT 1) AS "this_order_7"
                                           WHERE "q9_4")
                                            UNION ALL
                                          (SELECT True,
                                                   NULL :: trade,
                                                   "buy",
                                                   "cheapest_4",
                                                   "cheapest_order",
                                                   "margin_5",
                                                   "partkey",
                                                   "profit_4",
                                                   "sell",
                                                   (SELECT ("RTE4"."o_orderkey",
                                                                  "RTE4"."o_orderdate") :: dated_order AS "row"
                                                          FROM lineitem AS "RTE3",
                                                               orders AS "RTE4"
                                                          WHERE ("RTE3"."l_orderkey"
                                                                 =
                                                                 "RTE4"."o_orderkey"
                                                                 AND
                                                                 "RTE3"."l_partkey" = "partkey"
                                                                 AND
                                                                 "RTE4"."o_orderdate"
                                                                 >
                                                                 (("this_order").orderdate))
                                                          ORDER BY ("RTE4"."o_orderdate") ASC
                                                          LIMIT 1) AS "this_order_7"
                                           WHERE NOT "q9_4")
                                         ) AS "ifresult28"
                                         ON True)
                                        ON True)
                                       ON True)
                                 WHERE NOT "q5_3")
                               ) AS "ifresult11"
                               ON True)
                              ON True)
                             ON True)
                       WHERE "pred_2")
                        UNION ALL
                      (SELECT False,
                              (SELECT ("buy", "sell", "margin") :: trade AS "row") AS "result",
                              "run"."buy",
                              "run"."cheapest",
                              "run"."cheapest_order",
                              "run"."margin",
                              "run"."partkey",
                              "run"."profit",
                              "run"."sell",
                              "run"."this_order"
                       WHERE NOT "pred_2")
                     ) AS "ifresult7"
                     ON True)
              ) AS "result"
         WHERE "run"."rec?" = True)
    )
    SELECT "run"."res" AS "res"
    FROM run AS "run"
    WHERE "run"."rec?" = False
    ) AS run;
