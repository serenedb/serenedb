-- # of sheet evaluations (~ invocations)
\set N :invocations
\set R (1+sqrt(:iterations)) :: int


-- Evaluate entire sheet
SELECT run.res AS value
FROM   generate_series(1, :N) AS __(i), LATERAL (SELECT ('D', :R+1+i-i) :: cell) AS _(c), LATERAL
(
    WITH :MODE run("rec?",
                       "label",
                       "res",
                       "c",
                       "counter20",
                       "counter45",
                       "dep",
                       "deps",
                       "e",
                       "exprs",
                       "intermediates",
                       "lArray19",
                       "lArray44",
                       "open",
                       "q21",
                       "q46",
                       "rpn",
                       "stack") AS
    (
        (SELECT True,
               'while1_head',
               NULL :: float8,
               "c",
               NULL :: int4,
               NULL :: int4,
               NULL :: cell,
               ARRAY["c"] :: cell[] AS "deps_1",
               NULL :: JSONB,
               NULL :: JSONB[],
               NULL :: contents[],
               NULL :: cell[],
               NULL :: JSONB[],
               ARRAY["expr_1"] :: JSONB[] AS "open_1",
               NULL :: int4,
               NULL :: int4,
               NULL :: JSONB[],
               NULL :: float8[]
         FROM (SELECT "RTE0"."formula" AS "expr_1"
                         FROM sheet AS "RTE0"("cell", "formula")
                         WHERE "RTE0"."cell" = "c"
              ) AS "let1"("expr_1"))
          UNION ALL
        (SELECT "result".*
         FROM run AS "run"("rec?",
                           "label",
                           "res",
                           "c",
                           "counter20",
                           "counter45",
                           "dep",
                           "deps",
                           "e",
                           "exprs",
                           "intermediates",
                           "lArray19",
                           "lArray44",
                           "open",
                           "q21",
                           "q46",
                           "rpn",
                           "stack"),
              LATERAL
              ((SELECT "ifresult5".*
                FROM (LATERAL
                      (SELECT "counter20" <= "q21" AS "pred_12") AS "let4"("pred_12")
                      LEFT OUTER JOIN
                      LATERAL
                      ((SELECT "ifresult9".*
                        FROM (LATERAL
                              (SELECT ("lArray19")["counter20"] AS "dep_12",
                                      "counter20" + 1 AS "counter20_13") AS "let6"("dep_12", "counter20_13")
                              LEFT OUTER JOIN
                               (LATERAL
                                (SELECT EXISTS (SELECT 1 AS "?column?"
                                                FROM unnest("intermediates") AS "RTFunc4"("c", "v")
                                                WHERE "RTFunc4"."c" = "dep_12") AS "q25_12"
                                ) AS "let8"("q25_12")
                                LEFT OUTER JOIN
                                LATERAL
                                ((SELECT True,
                                         'foreacha18_head',
                                         NULL :: float8,
                                         "c",
                                         "counter20_13",
                                         "counter45",
                                         "dep_12",
                                         "deps",
                                         "e",
                                         "exprs",
                                         "intermediates",
                                         "lArray19",
                                         "lArray44",
                                         "open",
                                         "q21",
                                         "q46",
                                         "rpn",
                                         "stack"
                                  WHERE "q25_12")
                                   UNION ALL
                                 (SELECT True,
                                          'while26_head',
                                          NULL :: float8,
                                          "c",
                                          "counter20_13",
                                          "counter45",
                                          "dep_12",
                                          "deps",
                                          "e_13",
                                          ARRAY[] :: JSONB[] AS "exprs_13",
                                          "intermediates",
                                          "lArray19",
                                          "lArray44",
                                          "open",
                                          "q21",
                                          "q46",
                                          ARRAY["e_13"] :: JSONB[] AS "rpn_13",
                                          "stack"
                                  FROM (SELECT (SELECT "RTE5"."formula" AS "formula"
                                                 FROM sheet AS "RTE5"("cell", "formula")
                                                 WHERE "RTE5"."cell" = "dep_12") AS "e_13"
                                        ) AS "let11"("e_13")
                                  WHERE NOT "q25_12")
                                ) AS "ifresult9"
                                ON True)
                              ON True)
                        WHERE "pred_12")
                         UNION ALL
                       (SELECT False,
                               NULL :: text,
                               (SELECT ("stack")[1] AS "stack") AS "result",
                               "run"."c",
                               "run"."counter20",
                               "run"."counter45",
                               "run"."dep",
                               "run"."deps",
                               "run"."e",
                               "run"."exprs",
                               "run"."intermediates",
                               "run"."lArray19",
                               "run"."lArray44",
                               "run"."open",
                               "run"."q21",
                               "run"."q46",
                               "run"."rpn",
                               "run"."stack"
                        WHERE NOT "pred_12")
                      ) AS "ifresult5"
                      ON True)
                WHERE "run"."label" = 'foreacha18_head')
                 UNION ALL
               ((SELECT "ifresult17".*
                 FROM (LATERAL
                       (SELECT "counter45" <= "q46" AS "pred_22") AS "let16"("pred_22")
                       LEFT OUTER JOIN
                       LATERAL
                       ((SELECT "ifresult21".*
                         FROM (LATERAL
                               (SELECT ("lArray44")["counter45"] AS "e_21",
                                       "counter45" + 1 AS "counter45_21") AS "let18"("e_21", "counter45_21")
                                LEFT OUTER JOIN
                                LATERAL
                                 ((SELECT True,
                                           'foreacha43_head',
                                           NULL :: float8,
                                           "c",
                                           "counter20",
                                           "counter45_21",
                                           "dep",
                                           "deps",
                                           "e_21",
                                           "exprs",
                                           "intermediates",
                                           "lArray19",
                                           "lArray44",
                                           "open",
                                           "q21",
                                           "q46",
                                           "rpn",
                                           (("e_21" ->> 'num') :: float8) || "stack" AS "stack_41"
                                   WHERE ("e_21" ->> 'entry') = 'num')
                                    UNION ALL
                                  (SELECT "ifresult25".*
                                   FROM ((SELECT "ifresult27".*
                                           FROM ((SELECT True,
                                                           'foreacha43_head',
                                                           NULL :: float8,
                                                           "c",
                                                           "counter20",
                                                           "counter45_21",
                                                           "dep",
                                                           "deps",
                                                           "e_21",
                                                           "exprs",
                                                           "intermediates",
                                                           "lArray19",
                                                           "lArray44",
                                                           "open",
                                                           "q21",
                                                           "q46",
                                                           "rpn",
                                                           ((("stack")[1]) + (("stack")[2]))
                                                                 ||
                                                                 (("stack")[3:]) AS "stack_33"
                                                   WHERE ("e_21" ->> 'op') = '+')
                                                    UNION ALL
                                                  (((SELECT True,
                                                                 'foreacha43_head',
                                                                 NULL :: float8,
                                                                 "c",
                                                                 "counter20",
                                                                 "counter45_21",
                                                                 "dep",
                                                                 "deps",
                                                                 "e_21",
                                                                 "exprs",
                                                                 "intermediates",
                                                                 "lArray19",
                                                                 "lArray44",
                                                                 "open",
                                                                 "q21",
                                                                 "q46",
                                                                 "rpn",
                                                                 ((("stack")[1])
                                                                          -
                                                                          (("stack")[2]))
                                                                         ||
                                                                         (("stack")[3:]) AS "stack_25"
                                                           WHERE ("e_21" ->> 'op') = '-')
                                                            UNION ALL
                                                          (((SELECT True,
                                                                         'foreacha43_head',
                                                                         NULL :: float8,
                                                                         "c",
                                                                         "counter20",
                                                                         "counter45_21",
                                                                         "dep",
                                                                         "deps",
                                                                         "e_21",
                                                                         "exprs",
                                                                         "intermediates",
                                                                         "lArray19",
                                                                         "lArray44",
                                                                         "open",
                                                                         "q21",
                                                                         "q46",
                                                                         "rpn",
                                                                         ((("stack")[1])
                                                                                  *
                                                                                  (("stack")[2]))
                                                                                 ||
                                                                                 (("stack")[3:]) AS "stack_28"
                                                                   WHERE ("e_21" ->> 'op') = '*')
                                                                    UNION ALL
                                                                  (((SELECT True,
                                                                                 'foreacha43_head',
                                                                                 NULL :: float8,
                                                                                 "c",
                                                                                 "counter20",
                                                                                 "counter45_21",
                                                                                 "dep",
                                                                                 "deps",
                                                                                 "e_21",
                                                                                 "exprs",
                                                                                 "intermediates",
                                                                                 "lArray19",
                                                                                 "lArray44",
                                                                                 "open",
                                                                                 "q21",
                                                                                 "q46",
                                                                                 "rpn",
                                                                                 ((("stack")[1])
                                                                                          /
                                                                                          (("stack")[2]))
                                                                                         ||
                                                                                         (("stack")[3:]) AS "stack_31"
                                                                           WHERE ("e_21" ->> 'op') = '/')
                                                                         )
                                                                 ))
                                                         ))
                                                 )) AS "ifresult27"
                                           WHERE ("e_21" ->> 'entry') = 'op')
                                            UNION ALL
                                          (SELECT "ifresult44".*
                                           FROM ((SELECT True,
                                                           'foreacha43_head',
                                                           NULL :: float8,
                                                           "c",
                                                           "counter20",
                                                           "counter45_21",
                                                           "dep",
                                                           "deps",
                                                           "e_21",
                                                           "exprs",
                                                           "intermediates",
                                                           "lArray19",
                                                           "lArray44",
                                                           "open",
                                                           "q21",
                                                           "q46",
                                                           "rpn",
                                                           "stack_36"
                                                   FROM (SELECT ((SELECT CASE "e_21"
                                                                               ->>
                                                                               'agg' WHEN 'sum' THEN sum("RTFunc7"."v")
                                                                                     WHEN 'avg' THEN avg("RTFunc7"."v")
                                                                                     WHEN 'max' THEN max("RTFunc7"."v")
                                                                                     WHEN 'min' THEN min("RTFunc7"."v")
                                                                                     ELSE NULL :: float8
                                                                          END AS "case"
                                                                   FROM unnest("intermediates") AS "RTFunc7"("c",
                                                                                                             "v")
                                                                   WHERE ("RTFunc7"."c"
                                                                          >=
                                                                          (("e_21"
                                                                            ->>
                                                                            'from') :: cell)
                                                                          AND
                                                                          "RTFunc7"."c"
                                                                          <=
                                                                          (("e_21"
                                                                            ->>
                                                                            'to') :: cell))))
                                                                 ||
                                                                 "stack" AS "stack_36"
                                                         ) AS "let45"("stack_36")
                                                   WHERE ("e_21" ->> 'entry') = 'agg')
                                                    UNION ALL
                                                  (((SELECT True,
                                                                 'foreacha43_head',
                                                                 NULL :: float8,
                                                                 "c",
                                                                 "counter20",
                                                                 "counter45_21",
                                                                 "dep",
                                                                 "deps",
                                                                 "e_21",
                                                                 "exprs",
                                                                 "intermediates",
                                                                 "lArray19",
                                                                 "lArray44",
                                                                 "open",
                                                                 "q21",
                                                                 "q46",
                                                                 "rpn",
                                                                 "stack_39"
                                                           FROM (SELECT ((SELECT "RTFunc8"."v" AS "v"
                                                                           FROM unnest("intermediates") AS "RTFunc8"("c",
                                                                                                                     "v")
                                                                           WHERE "RTFunc8"."c"
                                                                                 =
                                                                                 (("e_21"
                                                                                   ->>
                                                                                   'cell') :: cell)))
                                                                         ||
                                                                         "stack" AS "stack_39"
                                                                 ) AS "let49"("stack_39")
                                                           WHERE ("e_21" ->> 'entry') = 'cell')
                                                         )
                                                 )) AS "ifresult44"
                                           )) AS "ifresult25"
                                   )) AS "ifresult21"
                               ON True)
                         WHERE "pred_22")
                          UNION ALL
                        (SELECT True,
                               'foreacha18_head',
                               NULL :: float8,
                               "c",
                               "counter20",
                               "counter45",
                               "dep",
                               "deps",
                               "e",
                               "exprs",
                               "intermediates"
                                       ||
                                       (("dep", ("stack")[1]) :: contents) AS "intermediates_36",
                               "lArray19",
                               "lArray44",
                               "open",
                               "q21",
                               "q46",
                               "rpn",
                               "stack"
                         WHERE NOT "pred_22")
                       ) AS "ifresult17"
                       ON True)
                 WHERE "run"."label" = 'foreacha43_head')
                 UNION ALL
               ((SELECT "ifresult55".*
                 FROM (LATERAL
                       (SELECT (cardinality("open")) > 0 AS "pred_2") AS "let54"("pred_2")
                       LEFT OUTER JOIN
                       LATERAL
                       ((SELECT "ifresult59".*
                         FROM (LATERAL
                               (SELECT ("open")[1] AS "expr_4", ("open")[2:] AS "open_4") AS "let56"("expr_4")
                               LEFT OUTER JOIN
                               LATERAL
                                 ((SELECT True,
                                          'while1_head',
                                          NULL :: float8,
                                          "c",
                                          "counter20",
                                          "counter45",
                                          "dep",
                                          "deps",
                                          "e",
                                          "exprs",
                                          "intermediates",
                                          "lArray19",
                                          "lArray44",
                                          "open_4",
                                          "q21",
                                          "q46",
                                          "rpn",
                                          "stack"
                                   WHERE ("expr_4" ->> 'entry') = 'num')
                                    UNION ALL
                                  (((SELECT True,
                                                  'while1_head',
                                                  NULL :: float8,
                                                  "c",
                                                  "counter20",
                                                  "counter45",
                                                  "dep",
                                                  "deps",
                                                  "e",
                                                  "exprs",
                                                  "intermediates",
                                                  "lArray19",
                                                  "lArray44",
                                                  "open_4" || "formulae_5" AS "open_7",
                                                  "q21",
                                                  "q46",
                                                  "rpn",
                                                  "stack"
                                           FROM (SELECT (SELECT array_agg("RTFunc1"."value") AS "array_agg"
                                                          FROM jsonb_array_elements("expr_4"
                                                                                    ->
                                                                                    'args') AS "RTFunc1"("value")) AS "formulae_5"
                                                 ) AS "let63"("formulae_5")
                                           WHERE ("expr_4" ->> 'entry') = 'op')
                                            UNION ALL
                                          (((SELECT True,
                                                         'while1_head',
                                                         NULL :: float8,
                                                         "c",
                                                         "counter20",
                                                         "counter45",
                                                         "dep",
                                                         (("args_9" :: arguments).cells)
                                                                  ||
                                                                  "deps" AS "deps_11",
                                                         "e",
                                                         "exprs",
                                                         "intermediates",
                                                         "lArray19",
                                                         "lArray44",
                                                         "open_4"
                                                                   ||
                                                                   (("args_9" :: arguments).formulae) AS "open_13",
                                                         "q21",
                                                         "q46",
                                                         "rpn",
                                                         "stack"
                                                   FROM (SELECT (SELECT (array_agg("RTE2"."cell"),
                                                                          array_agg("RTE2"."formula")) :: arguments AS "row"
                                                                  FROM sheet AS "RTE2"("cell",
                                                                                       "formula")
                                                                  WHERE ("RTE2"."cell"
                                                                         >=
                                                                         (("expr_4"
                                                                           ->>
                                                                           'from') :: cell)
                                                                         AND
                                                                         "RTE2"."cell"
                                                                         <=
                                                                         (("expr_4"
                                                                           ->>
                                                                           'to') :: cell))) AS "args_9"
                                                         ) AS "let68"("args_9")
                                                   WHERE ("expr_4" ->> 'entry') = 'agg')
                                                    UNION ALL
                                                  ((SELECT True,
                                                                  'while1_head',
                                                                  NULL :: float8,
                                                                  "c_8",
                                                                  "counter20",
                                                                  "counter45",
                                                                  "dep",
                                                                  "deps_9",
                                                                  "e",
                                                                  "exprs",
                                                                  "intermediates",
                                                                  "lArray19",
                                                                  "lArray44",
                                                                  "open_4" || "expr_10" AS "open_11",
                                                                  "q21",
                                                                  "q46",
                                                                  "rpn",
                                                                  "stack"
                                                           FROM (LATERAL
                                                                 (SELECT ("expr_4" ->> 'cell') :: cell AS "c_8"
                                                                 ) AS "let74"("c_8")
                                                                 LEFT OUTER JOIN
                                                                 (LATERAL
                                                                  (SELECT "c_8" || "deps" AS "deps_9"
                                                                  ) AS "let75"("deps_9")
                                                                  LEFT OUTER JOIN
                                                                  LATERAL
                                                                   (SELECT (SELECT "RTE3"."formula" AS "formula"
                                                                            FROM sheet AS "RTE3"("cell",
                                                                                                 "formula")
                                                                            WHERE "RTE3"."cell"
                                                                                  =
                                                                                  "c_8") AS "expr_10"
                                                                   ) AS "let76"("expr_10")
                                                                  ON True)
                                                                 ON True)
                                                           WHERE ("expr_4" ->> 'entry') = 'cell')
                                                   ))
                                           ))
                                         )) AS "ifresult59"
                               ON True)
                         WHERE "pred_2")
                          UNION ALL
                        (SELECT True,
                                 'foreacha18_head',
                                 NULL :: float8,
                                 "c",
                                 1,
                                 "counter45",
                                 "dep",
                                 "deps",
                                 "e",
                                 "exprs",
                                 ARRAY[] :: contents[] AS "intermediates_10",
                                 "deps" AS "lArray19_10",
                                 "lArray44",
                                 "open",
                                 array_length("deps", 1) AS "q21_10",
                                 "q46",
                                 "rpn",
                                 "stack"
                         WHERE NOT "pred_2")
                       ) AS "ifresult55"
                       ON True)
                 WHERE "run"."label" = 'while1_head')
                 UNION ALL
               (SELECT "ifresult85".*
                FROM (LATERAL
                      (SELECT (cardinality("rpn")) > 0 AS "pred_16") AS "let84"("pred_16")
                      LEFT OUTER JOIN
                      LATERAL
                      ((SELECT "ifresult90".*
                        FROM (LATERAL
                              (SELECT ("rpn")[1] AS "root_15",
                                      ("rpn")[2:] AS "rpn_16") AS "let86"("root_15")
                              LEFT OUTER JOIN
                              (LATERAL
                                (SELECT "root_15" || "exprs" AS "exprs_16") AS "let88"("exprs_16")
                                LEFT OUTER JOIN
                                 LATERAL
                                 ((SELECT True,
                                          'while26_head',
                                          NULL :: float8,
                                          "c",
                                          "counter20",
                                          "counter45",
                                          "dep",
                                          "deps",
                                          "e",
                                          "exprs_16",
                                          "intermediates",
                                          "lArray19",
                                          "lArray44",
                                          "open",
                                          "q21",
                                          "q46",
                                          "rpn_16",
                                          "stack"
                                   WHERE ("root_15" ->> 'entry') = 'num')
                                    UNION ALL
                                  (((SELECT True,
                                           'while26_head',
                                           NULL :: float8,
                                           "c",
                                           "counter20",
                                           "counter45",
                                           "dep",
                                           "deps",
                                           "e",
                                           "exprs_16",
                                           "intermediates",
                                           "lArray19",
                                           "lArray44",
                                           "open",
                                           "q21",
                                           "q46",
                                           ((SELECT array_agg("RTFunc6"."value") AS "array_agg"
                                                   FROM jsonb_array_elements("root_15"
                                                                             ->
                                                                             'args') AS "RTFunc6"("value")))
                                                 ||
                                                 "rpn_16" AS "rpn_19",
                                           "stack"
                                   WHERE ("root_15" ->> 'entry') = 'op')
                                            UNION ALL
                                          (SELECT True,
                                                  'while26_head',
                                                  NULL :: float8,
                                                  "c",
                                                  "counter20",
                                                  "counter45",
                                                  "dep",
                                                  "deps",
                                                  "e",
                                                  "exprs_16",
                                                  "intermediates",
                                                  "lArray19",
                                                  "lArray44",
                                                  "open",
                                                  "q21",
                                                  "q46",
                                                  "rpn_16",
                                                  "stack"
                                           WHERE NOT ("root_15" ->> 'entry') = 'num' AND NOT (("root_15" ->> 'entry') = 'op'))
                                         )
                                 )) AS "ifresult90"
                               ON True)
                              ON True)
                        WHERE "pred_16")
                         UNION ALL
                       (SELECT True,
                                'foreacha43_head',
                                NULL :: float8,
                                "c",
                                "counter20",
                                1,
                                "dep",
                                "deps",
                                "e",
                                "exprs",
                                "intermediates",
                                "lArray19",
                                "exprs" AS "lArray44_18",
                                "open",
                                "q21",
                                array_length("exprs", 1) AS "q46_18",
                                "rpn",
                                ARRAY[] :: float8[] AS "stack_18"
                        WHERE NOT "pred_16")
                      ) AS "ifresult85"
                      ON True)
                WHERE "run"."label" = 'while26_head')))
              ) AS "result"("rec?",
                            "label",
                            "res",
                            "c",
                            "counter20",
                            "counter45",
                            "dep",
                            "deps",
                            "e",
                            "exprs",
                            "intermediates",
                            "lArray19",
                            "lArray44",
                            "open",
                            "q21",
                            "q46",
                            "rpn",
                            "stack")
         WHERE "run"."rec?" = True)
    )
    SELECT "run"."res" AS "res"
    FROM run AS "run"
    WHERE "run"."rec?" = False
    ) AS run;


