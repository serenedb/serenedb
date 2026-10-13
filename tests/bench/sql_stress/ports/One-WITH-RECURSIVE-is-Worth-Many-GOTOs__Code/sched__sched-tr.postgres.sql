
-- Try to schedule all open orders (and indicate whether we will miss the production deadline)
SELECT o.o_orderkey,
       cardinality(run.res) < (SELECT MAX(l.l_linenumber)
                                              FROM   lineitem AS l
                                              WHERE  l.l_orderkey = o.o_orderkey) AS "miss?",
       run.res AS schedule
FROM   (SELECT *
        FROM orders
        ORDER BY o_orderkey
        LIMIT :invocations) AS o, LATERAL (SELECT o.o_orderkey) AS _(orderkey),
    LATERAL
(
    WITH :MODE run("rec?",
                       "label",
                       "res",
                       "busy",
                       "details",
                       "item_end",
                       "item_start",
                       "lineitem",
                       "order",
                       "orderkey",
                       "prio",
                       "schedule",
                       "schedule_end",
                       "schedule_start") AS
    (
        (SELECT "ifresult14".*
         FROM (LATERAL
               (SELECT "RTE0" AS "order_1"
                        FROM orders AS "RTE0"
                        WHERE "RTE0"."o_orderkey" = "orderkey"
               ) AS "let12"("order_1")
               LEFT OUTER JOIN
               (LATERAL (SELECT "order_1" IS NULL AS "q4_1") AS "let13"("q4_1")
                LEFT OUTER JOIN
                LATERAL
                ((SELECT False,
                         NULL :: text,
                         (SELECT NULL :: scheduled[] AS "?column?") AS "result",
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL
                  WHERE "q4_1")
                   UNION ALL
                 (SELECT True,
                          'fori6_head',
                          NULL :: scheduled[],
                          ARRAY[] :: daterange[] AS "busy_2",
                          "details_2",
                          NULL :: date,
                          NULL :: date,
                          NULL :: lineitem,
                          "order_1",
                          "orderkey",
                          (SELECT 1 AS "?column?"),
                          ARRAY[] :: scheduled[] AS "schedule_2",
                          ("details_2" :: order_details).last_shipdate AS "schedule_end_2",
                          ("order_1" :: orders).o_orderdate AS "schedule_start_2"
                  FROM (SELECT (count(*),
                                 max("RTE1"."l_shipdate")) :: order_details AS "details_2"
                         FROM lineitem AS "RTE1"
                         WHERE "RTE1"."l_orderkey" = "orderkey"
                        ) AS "let16"("details_2")
                  WHERE NOT "q4_1")
                ) AS "ifresult14"
                ON True)
               ON True))
          UNION ALL
        (SELECT "result".*
         FROM run AS "run"("rec?",
                           "label",
                           "res",
                           "busy",
                           "details",
                           "item_end",
                           "item_start",
                           "lineitem",
                           "order",
                           "orderkey",
                           "prio",
                           "schedule",
                           "schedule_end",
                           "schedule_start"),
              LATERAL
              ((SELECT "ifresult2".*
                FROM (LATERAL
                       (SELECT "prio" <= ("details" :: order_details).items AS "pred_3") AS "let1"("pred_3")
                       LEFT OUTER JOIN
                       LATERAL
                       ((SELECT True,
                               'while8_head',
                               NULL :: scheduled[],
                               "busy",
                               "details",
                               least(("lineitem_4" :: lineitem).l_shipdate, "schedule_end") AS "item_end_4",
                               least(("lineitem_4" :: lineitem).l_shipdate, "schedule_end") - (("lineitem_4" :: lineitem).l_quantity) :: int4 AS "item_start_4",
                               "lineitem_4",
                               "order",
                               "orderkey",
                               "prio",
                               "schedule",
                               "schedule_end",
                               "schedule_start"
                         FROM (SELECT "subquery4"."lineitem" AS "lineitem_4"
                                        FROM (SELECT row_number() OVER(ORDER BY ("RTE3"."p_retailprice") DESC RANGE UNBOUNDED PRECEDING) AS "priority",
                                                     "RTE2" AS "lineitem"
                                              FROM lineitem AS "RTE2",
                                                   part AS "RTE3"
                                              WHERE ("RTE2"."l_orderkey" = "orderkey"
                                                     AND
                                                     "RTE2"."l_partkey" = "RTE3"."p_partkey")
                                             ) AS "subquery4"("priority", "lineitem")
                                        WHERE "subquery4"."priority" = "prio"
                               ) AS "let3"("lineitem_4")
                         WHERE "pred_3")
                          UNION ALL
                        (SELECT "ifresult8".*
                         FROM (LATERAL
                               (SELECT (cardinality("schedule")) > 0 AS "q16_10"
                               ) AS "let7"("q16_10")
                               LEFT OUTER JOIN
                               LATERAL
                               ((SELECT False,
                                       NULL :: text,
                                       (SELECT array_agg("RTFunc6"
                                                                  ORDER BY ("RTFunc6"."when") ASC) AS "schedule_13"
                                                FROM unnest("schedule") AS "RTFunc6"("item",
                                                                                     "when")
                                       ) AS "result",
                                       "run"."busy",
                                       "run"."details",
                                       "run"."item_end",
                                       "run"."item_start",
                                       "run"."lineitem",
                                       "run"."order",
                                       "run"."orderkey",
                                       "run"."prio",
                                       "run"."schedule",
                                       "run"."schedule_end",
                                       "run"."schedule_start"
                                 WHERE "q16_10")
                                  UNION ALL
                                (SELECT False,
                                        NULL :: text,
                                        "schedule" AS "result",
                                        "run"."busy",
                                        "run"."details",
                                        "run"."item_end",
                                        "run"."item_start",
                                        "run"."lineitem",
                                        "run"."order",
                                        "run"."orderkey",
                                        "run"."prio",
                                        "run"."schedule",
                                        "run"."schedule_end",
                                        "run"."schedule_start"
                                 WHERE NOT "q16_10")
                               ) AS "ifresult8"
                               ON True)
                         WHERE NOT "pred_3")
                       ) AS "ifresult2"
                       ON True)
                WHERE "run"."label" = 'fori6_head')
                 UNION ALL
               (SELECT "ifresult23".*
                FROM (LATERAL
                      (SELECT (daterange("item_start", "item_end") && ANY("busy") AND "item_start" >= "schedule_start") AS "pred_6"
                      ) AS "let22"("pred_6")
                      LEFT OUTER JOIN
                      LATERAL
                      ((SELECT True,
                               'while8_head',
                               NULL :: scheduled[],
                               "busy",
                               "details",
                               "item_end_7",
                               "item_end_7" - (("lineitem" :: lineitem).l_quantity) :: int4 AS "item_start_7",
                               "lineitem",
                               "order",
                               "orderkey",
                               "prio",
                               "schedule",
                               "schedule_end",
                               "schedule_start"
                        FROM (SELECT lower("RTFunc5"."b") AS "item_end_7"
                                       FROM unnest("busy") AS "RTFunc5"("b")
                                       WHERE (daterange("item_start", "item_end")) && "RTFunc5"."b"
                                       ORDER BY ("RTFunc5"."b") ASC
                                       LIMIT 1
                              ) AS "let24"("item_end_7")
                        WHERE "pred_6")
                         UNION ALL
                       (SELECT "ifresult28".*
                        FROM (LATERAL
                              (SELECT "item_start" >= "schedule_start" AS "q12_7"
                              ) AS "let27"("q12_7")
                              LEFT OUTER JOIN
                              LATERAL
                              ((SELECT True,
                                      'fori6_head',
                                      NULL :: scheduled[],
                                      "busy" || (daterange("item_start", "item_end")) AS "busy_10",
                                      "details",
                                      "item_end",
                                      "item_start",
                                      "lineitem",
                                      "order",
                                      "orderkey",
                                      "prio" + 1 AS "prio_9",
                                      "schedule" || ((("lineitem" :: lineitem).l_linenumber, "item_start") :: scheduled) AS "schedule_10",
                                      "schedule_end",
                                      "schedule_start"
                                WHERE "q12_7")
                                 UNION ALL
                               (SELECT True,
                                        'fori6_head',
                                        NULL :: scheduled[],
                                        "busy",
                                        "details",
                                        "item_end",
                                        "item_start",
                                        "lineitem",
                                        "order",
                                        "orderkey",
                                        "prio" + 1 AS "prio_9",
                                        "schedule",
                                        "schedule_end",
                                        "schedule_start"
                                WHERE NOT "q12_7")
                              ) AS "ifresult28"
                              ON True)
                        WHERE NOT "pred_6")
                      ) AS "ifresult23"
                      ON True)
                WHERE "run"."label" = 'while8_head')
              ) AS "result"("rec?",
                            "label",
                            "res",
                            "busy",
                            "details",
                            "item_end",
                            "item_start",
                            "lineitem",
                            "order",
                            "orderkey",
                            "prio",
                            "schedule",
                            "schedule_end",
                            "schedule_start")
         WHERE "run"."rec?" = True)
    )
    SELECT "run"."res" AS "res"
    FROM run AS "run"
    WHERE "run"."rec?" = False
    ) AS run
WHERE  o.o_orderstatus = 'O';

