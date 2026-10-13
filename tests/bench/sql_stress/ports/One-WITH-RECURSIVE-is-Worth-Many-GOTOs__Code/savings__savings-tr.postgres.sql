
-- Optimize the supply chain for all open orders
SELECT o.o_orderkey, run.res AS "supply chain changes"
FROM   (SELECT *
        FROM orders
        ORDER BY o_orderkey
        LIMIT :invocations) AS o, LATERAL (SELECT o.o_orderkey) AS _(orderkey), LATERAL
(
    WITH :MODE run("rec?",
                   "res",
                   "item",
                   "items",
                   "new_suppliers",
                   "new_supplycost",
                   "total_supplycost") AS
    (
        (SELECT "ifresult21".*
         FROM (LATERAL
               (SELECT "RTE0" AS "order_1"
                        FROM orders AS "RTE0"
                        WHERE "RTE0"."o_orderkey" = "orderkey"
               ) AS "let19"("order_1")
               LEFT OUTER JOIN
               (LATERAL (SELECT "order_1" IS NULL AS "q4_1") AS "let20"("q4_1")
                LEFT OUTER JOIN
                LATERAL
                ((SELECT False,
                         NULL :: savings AS "result",
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL
                  WHERE "q4_1")
                   UNION ALL
                 (SELECT True,
                         NULL :: savings,
                         1,
                         (SELECT count(*) AS "items_2"
                         FROM lineitem AS "RTE1"
                         WHERE "RTE1"."l_orderkey" = "orderkey"
                         ) AS "items_2",
                         ARRAY[] :: supplier_change[] AS "new_suppliers_2",
                         0.0 AS "new_supplycost_2",
                         0.0 AS "total_supplycost_2"
                  WHERE NOT "q4_1")
                ) AS "ifresult21"
                ON True)
               ON True))
          UNION ALL
        (SELECT "result".*
         FROM run AS "run"("rec?",
                           "res",
                           "item",
                           "items",
                           "new_suppliers",
                           "new_supplycost",
                           "total_supplycost"),
              LATERAL
              (SELECT "ifresult2".*
               FROM (LATERAL
                      (SELECT "item" <= "items" AS "pred_3") AS "let1"("pred_3")
                      LEFT OUTER JOIN
                      LATERAL
                      ((SELECT "ifresult8".*
                        FROM (LATERAL
                              (SELECT "RTE2" AS "lineitem_4"
                                       FROM lineitem AS "RTE2"
                                       WHERE ("RTE2"."l_orderkey" = "orderkey"
                                              AND "RTE2"."l_linenumber" = "item")
                              ) AS "let3"("lineitem_4")
                              LEFT OUTER JOIN
                              (LATERAL
                               (SELECT "RTE3" AS "partsupp_4"
                                        FROM partsupp AS "RTE3"
                                        WHERE ((("lineitem_4" :: lineitem).l_partkey)
                                               =
                                               "RTE3"."ps_partkey"
                                               AND
                                               (("lineitem_4" :: lineitem).l_suppkey)
                                               =
                                               "RTE3"."ps_suppkey")
                               ) AS "let4"("partsupp_4")
                               LEFT OUTER JOIN
                               (LATERAL
                                (SELECT (SELECT min("RTE4"."ps_supplycost") AS "min"
                                         FROM partsupp AS "RTE4"
                                         WHERE ("RTE4"."ps_partkey"
                                                =
                                                (("lineitem_4" :: lineitem).l_partkey)
                                                AND
                                                ("RTE4"."ps_availqty")
                                                >=
                                                (("lineitem_4" :: lineitem).l_quantity))) AS "min_supplycost_4"
                                ) AS "let5"("min_supplycost_4")
                                LEFT OUTER JOIN
                                (LATERAL
                                 (SELECT (SELECT min("RTE5"."ps_suppkey") AS "min"
                                          FROM partsupp AS "RTE5"
                                          WHERE ("RTE5"."ps_supplycost"
                                                 =
                                                 "min_supplycost_4"
                                                 AND
                                                 "RTE5"."ps_partkey"
                                                 =
                                                 (("lineitem_4" :: lineitem).l_partkey))) AS "new_supplier_4"
                                 ) AS "let6"("new_supplier_4")
                                 LEFT OUTER JOIN
                                 (LATERAL
                                  (SELECT "new_supplier_4" <> (("partsupp_4" :: partsupp).ps_suppkey) AS "q11_4"
                                  ) AS "let7"("q11_4")
                                  LEFT OUTER JOIN
                                  LATERAL
                                  ((SELECT True,
                                           NULL :: savings,
                                           "item" + 1 AS "item_7",
                                           "items",
                                           ((("lineitem_4" :: lineitem).l_partkey,
                                                    ("partsupp_4" :: partsupp).ps_suppkey,
                                                    "new_supplier_4") :: supplier_change)
                                                  ||
                                                  "new_suppliers" AS "new_suppliers_6",
                                           "new_supplycost"
                                                    +
                                                    ("min_supplycost_4"
                                                     *
                                                     (("lineitem_4" :: lineitem).l_quantity)) AS "new_supplycost_7",
                                           "total_supplycost"
                                                   +
                                                   ((("partsupp_4" :: partsupp).ps_supplycost)
                                                    *
                                                    (("lineitem_4" :: lineitem).l_quantity)) AS "total_supplycost_7"
                                    WHERE "q11_4")
                                     UNION ALL
                                   (SELECT True,
                                            NULL :: savings,
                                            "item" + 1 AS "item_7",
                                            "items",
                                            "new_suppliers",
                                            "new_supplycost"
                                           +
                                           ("min_supplycost_4"
                                            *
                                            (("lineitem_4" :: lineitem).l_quantity)) AS "new_supplycost_7",
                                            "total_supplycost"
                                          +
                                          ((("partsupp_4" :: partsupp).ps_supplycost)
                                           *
                                           (("lineitem_4" :: lineitem).l_quantity)) AS "total_supplycost_7"
                                    WHERE NOT "q11_4")
                                  ) AS "ifresult8"
                                  ON True)
                                 ON True)
                                ON True)
                               ON True)
                              ON True)
                        WHERE "pred_3")
                         UNION ALL
                       (SELECT False,
                               ((1.0 - ("new_supplycost" / "total_supplycost")) * 100.0,
                                        "new_suppliers") :: savings AS "result",
                               "run"."item",
                               "run"."items",
                               "run"."new_suppliers",
                               "run"."new_supplycost",
                               "run"."total_supplycost"
                        WHERE NOT "pred_3")
                      ) AS "ifresult2"
                      ON True)
              ) AS "result"
         WHERE "run"."rec?" = True)
    )
    SELECT "run"."res" AS "res"
    FROM run AS "run"
    WHERE "run"."rec?" = False
) AS run
WHERE  o.o_orderstatus = 'O';
