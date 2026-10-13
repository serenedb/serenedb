
-- maximum size of one pack
\set CAPACITY 60
SELECT o.o_orderkey, run.res AS packs
FROM   (SELECT *
        FROM orders
        ORDER BY o_orderkey
        LIMIT :invocations) AS o, LATERAL (SELECT o.o_orderkey, :CAPACITY) AS _(orderkey, capacity), LATERAL
(
    WITH :MODE run("rec?",
                       "label",
                       "res",
                       "items",
                       "linenumber",
                       "max_size",
                       "max_subset",
                       "n",
                       "pack",
                       "packs",
                       "subset") AS
    (
        (SELECT "ifresult33".*
         FROM (LATERAL
               (SELECT (SELECT count(*) AS "count"
                        FROM lineitem AS "RTE0"
                        WHERE "RTE0"."l_orderkey" = "orderkey") AS "n_1"
               ) AS "let31"("n_1")
               LEFT OUTER JOIN
               (LATERAL (SELECT "n_1" = 0 AS "q4_1") AS "let32"("q4_1")
                LEFT OUTER JOIN
                LATERAL
                ((SELECT False,
                         NULL :: text,
                         (SELECT ARRAY[] :: linenumber[][] AS "array") AS "result",
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
                 (SELECT "ifresult36".*
                  FROM (LATERAL
                        (SELECT "capacity"
                                <
                                ((SELECT max("RTE2"."p_size") AS "max"
                                  FROM lineitem AS "RTE1",
                                       part AS "RTE2"
                                  WHERE ("RTE1"."l_orderkey" = "orderkey"
                                         AND
                                         "RTE1"."l_partkey" = "RTE2"."p_partkey"))) AS "q9_2"
                        ) AS "let35"("q9_2")
                        LEFT OUTER JOIN
                        LATERAL
                        ((SELECT False,
                                 NULL :: text,
                                 (SELECT ARRAY[] :: linenumber[][] AS "array") AS "result",
                                 NULL,
                                 NULL,
                                 NULL,
                                 NULL,
                                 NULL,
                                 NULL,
                                 NULL,
                                 NULL
                          WHERE "q9_2")
                           UNION ALL
                         (SELECT True,
                                 'while11_head',
                                 NULL :: linenumber[][],
                                 ((2) ^ ("n_1")) - (1) AS "items_3",
                                 NULL :: int4,
                                 NULL :: int4,
                                 NULL :: int4,
                                 "n_1",
                                 NULL :: linenumber[],
                                 ARRAY[] :: linenumber[][] AS "packs_3",
                                 NULL :: int4
                          WHERE NOT "q9_2")
                        ) AS "ifresult36"
                        ON True)
                  WHERE NOT "q4_1")
                ) AS "ifresult33"
                ON True)
               ON True))
          UNION ALL
        (SELECT "result".*
         FROM run AS "run"("rec?",
                           "label",
                           "res",
                           "items",
                           "linenumber",
                           "max_size",
                           "max_subset",
                           "n",
                           "pack",
                           "packs",
                           "subset"),
              LATERAL
              ((SELECT "ifresult2".*
                FROM (LATERAL
                       (SELECT "linenumber" <= "n" AS "pred_12") AS "let1"("pred_12")
                       LEFT OUTER JOIN
                       LATERAL
                       ((SELECT "ifresult4".*
                         FROM (LATERAL
                               (SELECT ("max_subset" & (1 << ("linenumber" - 1))) <> 0 AS "q26_12"
                               ) AS "let3"("q26_12")
                               LEFT OUTER JOIN
                               LATERAL
                               ((SELECT True,
                                        'fori21_head',
                                        NULL :: linenumber[][],
                                        "items",
                                        "linenumber" + 1 AS "linenumber_15",
                                        "max_size",
                                        "max_subset",
                                        "n",
                                        array_append("pack", (("linenumber") :: linenumber)) AS "pack_17",
                                        "packs",
                                        "subset" :: int4
                                 WHERE "q26_12")
                                  UNION ALL
                                (SELECT True,
                                        'fori21_head',
                                        NULL :: linenumber[][],
                                        "items",
                                        "linenumber" + 1 AS "linenumber_15",
                                        "max_size",
                                        "max_subset",
                                        "n",
                                        array_append("pack", ((0) :: linenumber)) AS "pack_14",
                                        "packs",
                                        "subset" :: int4
                                 WHERE NOT "q26_12")
                               ) AS "ifresult4"
                               ON True)
                         WHERE "pred_12")
                          UNION ALL
                        (SELECT True,
                                'while11_head',
                                NULL :: linenumber[][],
                                ("items" :: int4) & (~ "max_subset") AS "items_17",
                                "linenumber",
                                "max_size",
                                "max_subset",
                                "n",
                                "pack",
                                "packs" || (ARRAY["pack"] :: linenumber[][]) AS "packs_17",
                                "subset" :: int4
                         WHERE NOT "pred_12")
                       ) AS "ifresult2"
                      ON True)
                WHERE "run"."label" = 'fori21_head')
                 UNION ALL
               ((SELECT "ifresult16".*
                 FROM (LATERAL
                       (SELECT (SELECT sum("RTE4"."p_size") AS "sum"
                                FROM lineitem AS "RTE3",
                                     part AS "RTE4"
                                WHERE ("RTE3"."l_orderkey" = "orderkey"
                                       AND
                                       ("subset" & (1 << ("RTE3"."l_linenumber" - 1))) <> 0
                                       AND
                                       "RTE3"."l_partkey" = "RTE4"."p_partkey")) AS "size_6"
                       ) AS "let14"("size_6")
                       LEFT OUTER JOIN
                       (LATERAL
                        (SELECT ("size_6" <= "capacity" AND "size_6" > "max_size") AS "q16_6"
                        ) AS "let15"("q16_6")
                        LEFT OUTER JOIN
                        LATERAL
                        ((SELECT "ifresult20".*
                          FROM (LATERAL
                                (SELECT "size_6" AS "max_size_8",
                                        "subset" AS "max_subset_8",
                                        "subset" = "items" AS "q20_8") AS "let17"("max_size_8", "max_subset_8", "q20_8")
                                LEFT OUTER JOIN
                                  LATERAL
                                  ((SELECT True,
                                            'fori21_head',
                                            NULL :: linenumber[][],
                                            "items",
                                            (SELECT 1 AS "?column?"),
                                            "max_size_8" :: int4,
                                            "max_subset_8",
                                            "n",
                                            ARRAY[] :: linenumber[] AS "pack_10",
                                            "packs",
                                            "subset" :: int4
                                    WHERE "q20_8")
                                     UNION ALL
                                   (SELECT True,
                                            'loop12_body',
                                            NULL :: linenumber[][],
                                            "items",
                                            "linenumber",
                                            "max_size_8" :: int4,
                                            "max_subset_8",
                                            "n",
                                            "pack",
                                            "packs",
                                            ("items" :: int4) & ("subset" - ("items" :: int4)) AS "subset_10"
                                    WHERE NOT "q20_8")
                                  ) AS "ifresult20"
                                ON True)
                          WHERE "q16_6")
                           UNION ALL
                         (SELECT "ifresult26".*
                          FROM (LATERAL
                                (SELECT "subset" = "items" AS "q20_8") AS "let25"("q20_8")
                                LEFT OUTER JOIN
                                LATERAL
                                ((SELECT True,
                                          'fori21_head',
                                          NULL :: linenumber[][],
                                          "items",
                                          (SELECT 1 AS "?column?"),
                                          "max_size" :: int4,
                                          "max_subset",
                                          "n",
                                          ARRAY[] :: linenumber[] AS "pack_10",
                                          "packs",
                                          "subset" :: int4
                                  WHERE "q20_8")
                                   UNION ALL
                                 (SELECT True,
                                          'loop12_body',
                                          NULL :: linenumber[][],
                                          "items",
                                          "linenumber",
                                          "max_size" :: int4,
                                          "max_subset",
                                          "n",
                                          "pack",
                                          "packs",
                                          ("items" :: int4) & ("subset" - ("items" :: int4)) AS "subset_10"
                                  WHERE NOT "q20_8")
                                ) AS "ifresult26"
                                ON True)
                          WHERE NOT "q16_6")
                        ) AS "ifresult16"
                        ON True)
                       ON True)
                 WHERE "run"."label" = 'loop12_body')
                 UNION ALL
               (SELECT "ifresult42".*
                FROM (LATERAL (SELECT "items" <> 0 AS "pred_4") AS "let41"("pred_4")
                      LEFT OUTER JOIN
                      LATERAL
                      ((SELECT True,
                                'loop12_body',
                                NULL :: linenumber[][],
                                "items",
                                "linenumber",
                                0 AS "max_size_5",
                                0 AS "max_subset_5",
                                "n",
                                "pack",
                                "packs",
                                ("items" :: int4) & (- ("items" :: int4)) AS "subset_5"
                        WHERE "pred_4")
                         UNION ALL
                       (SELECT False,
                               NULL :: text,
                               "packs" AS "result",
                               "run"."items",
                               "run"."linenumber",
                               "run"."max_size" :: int4,
                               "run"."max_subset",
                               "run"."n",
                               "run"."pack",
                               "run"."packs",
                               "run"."subset"
                        WHERE NOT "pred_4")
                      ) AS "ifresult42"
                      ON True)
                WHERE "run"."label" = 'while11_head'))
              ) AS "result"("rec?",
                            "label",
                            "res",
                            "items",
                            "linenumber",
                            "max_size",
                            "max_subset",
                            "n",
                            "pack",
                            "packs",
                            "subset")
         WHERE "run"."rec?" = True)
    )
    SELECT "run"."res" AS "res"
    FROM run AS "run"
    WHERE "run"."rec?" = False
) run
WHERE  o.o_orderstatus = 'F';
