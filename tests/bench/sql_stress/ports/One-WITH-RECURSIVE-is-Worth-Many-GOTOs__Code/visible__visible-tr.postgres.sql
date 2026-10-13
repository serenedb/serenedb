\set N :invocations
\set RESOLUTION :iterations
\set X 10
\set Y 10

SELECT setseed(0.42);


SELECT x, run.res AS "visible?"
FROM   generate_series(1, :N) AS i,
       LATERAL (VALUES (random() * (:X - 1) + i - i)) AS _(x),
       LATERAL (SELECT point(x,0), point(x,:Y-1), :X, :Y, :RESOLUTION) AS __(here, there, gridx, gridy, resolution),
       LATERAL
  (
    WITH :MODE run("rec?",
                       "label",
                       "res",
                       "angle",
                       "gridx",
                       "gridy",
                       "here",
                       "hhere",
                       "i",
                       "loc",
                       "max_angle",
                       "resolution",
                       "step",
                       "there") AS
    (
        (SELECT True,
                'fori1_head',
                NULL :: bool,
                NULL :: float8,
                "gridx_1",
                "gridy_1",
                "here",
                "hhere_1",
                1,
                "here" AS "loc_1",
                NULL :: float8 AS "max_angle_1",
                "resolution",
                ("there" - "here") / ("resolution") AS "step_1",
                "there"
         FROM (LATERAL (SELECT "gridx" - 1 AS "gridx_1") AS "let14"("gridx_1")
               LEFT OUTER JOIN
               (LATERAL (SELECT "gridy" - 1 AS "gridy_1") AS "let15"("gridy_1")
                LEFT OUTER JOIN
                LATERAL
                 (SELECT sum((((((((!! "gridx_1")
                                           /
                                           ((!! "subquery2"."x")
                                            *
                                            (!! "gridx_1" - "subquery2"."x")))
                                          *
                                          ("subquery6"."u" ^ ("subquery2"."x")))
                                         *
                                         (((1) - "subquery6"."u") ^ ("gridx_1" - "subquery2"."x")))
                                        *
                                        ((!! "gridy_1")
                                         /
                                         ((!! "subquery2"."y") * (!! "gridy_1" - "subquery2"."y"))))
                                       *
                                       ("subquery6"."v" ^ ("subquery2"."y")))
                                      *
                                      (((1) - "subquery6"."v") ^ ("gridy_1" - "subquery2"."y")))
                                     *
                                     "subquery6"."h") AS "hhere_1"
                          FROM ((SELECT "RTFunc0"."x" AS "x", "RTFunc1"."y" AS "y"
                                 FROM generate_series(0, "gridx_1") AS "RTFunc0"("x"),
                                      generate_series(0, "gridy_1") AS "RTFunc1"("y")
                                ) AS "subquery2"("x", "y")
                                LEFT OUTER JOIN
                                controlp AS "RTE3"("x", "y", "z")
                                ON ("RTE3"."x" = "subquery2"."x"
                                    AND
                                    "RTE3"."y" = "subquery2"."y")),
                               LATERAL
                               (VALUES (((("here")[0]) / ("gridx_1")),
                                       (("here")[1]) / (("gridy_1")),
                                       COALESCE("RTE3"."z", 0))
                               ) AS "subquery6"("u", "v", "h")
                 ) AS "let16"("hhere_1")
                ON True)
               ON True))
          UNION ALL
        (SELECT "result".*
         FROM run AS "run"("rec?",
                           "label",
                           "res",
                           "angle",
                           "gridx",
                           "gridy",
                           "here",
                           "hhere",
                           "i",
                           "loc",
                           "max_angle",
                           "resolution",
                           "step",
                           "there"),
              LATERAL
              (SELECT "ifresult2".*
               FROM (LATERAL (SELECT "resolution" AS "q2_2") AS "let0"("q2_2")
                     LEFT OUTER JOIN
                     (LATERAL (SELECT "i" <= "q2_2" AS "pred_2") AS "let1"("pred_2")
                      LEFT OUTER JOIN
                      LATERAL
                      ((SELECT "ifresult7".*
                        FROM (LATERAL (SELECT "loc" + "step" AS "loc_4") AS "let3"("loc_4")
                              LEFT OUTER JOIN
                              (LATERAL
                               (SELECT sum((((((((!! "gridx")
                                                         /
                                                         ((!! "subquery9"."x")
                                                          *
                                                          (!! "gridx" - "subquery9"."x")))
                                                        *
                                                        ("subquery13"."u" ^ ("subquery9"."x")))
                                                       *
                                                       (((1) - "subquery13"."u")
                                                        ^
                                                        ("gridx" - "subquery9"."x")))
                                                      *
                                                      ((!! "gridy")
                                                       /
                                                       ((!! "subquery9"."y")
                                                        *
                                                        (!! "gridy" - "subquery9"."y"))))
                                                     *
                                                     ("subquery13"."v" ^ ("subquery9"."y")))
                                                    *
                                                    (((1) - "subquery13"."v")
                                                     ^
                                                     ("gridy" - "subquery9"."y")))
                                                   *
                                                   "subquery13"."h") AS "hloc_3"
                                        FROM ((SELECT "RTFunc7"."x" AS "x", "RTFunc8"."y" AS "y"
                                               FROM generate_series(0, "gridx") AS "RTFunc7"("x"),
                                                    generate_series(0, "gridy") AS "RTFunc8"("y")
                                              ) AS "subquery9"("x", "y")
                                              LEFT OUTER JOIN
                                              controlp AS "RTE10"("x", "y", "z")
                                              ON ("RTE10"."x" = "subquery9"."x"
                                                  AND
                                                  "RTE10"."y" = "subquery9"."y")),
                                             LATERAL
                                             (VALUES (((("loc_4")[0]) / ("gridx")),
                                                     ((("loc_4")[1]) / ("gridy")),
                                                     COALESCE("RTE10"."z", 0))
                                             ) AS "subquery13"("u", "v", "h")
                               ) AS "let4"("hloc_3")
                               LEFT OUTER JOIN
                               (LATERAL
                                (SELECT degrees(atan(("hloc_3" - "hhere")
                                                     /
                                                     ("loc_4" <-> "here"))) AS "angle_3"
                                ) AS "let5"("angle_3")
                                LEFT OUTER JOIN
                                (LATERAL
                                 (SELECT ("max_angle" IS NULL OR "angle_3" > "max_angle") AS "q6_3"
                                 ) AS "let6"("q6_3")
                                 LEFT OUTER JOIN
                                 LATERAL
                                 ((SELECT True,
                                          'fori1_head',
                                          NULL :: bool,
                                          "angle_3",
                                          "gridx",
                                          "gridy",
                                          "here",
                                          "hhere",
                                          "i" + 1 AS "i_6",
                                          "loc_4",
                                          "angle_3" AS "max_angle_5",
                                          "resolution",
                                          "step",
                                          "there"
                                   WHERE "q6_3")
                                    UNION ALL
                                  (SELECT True,
                                           'fori1_head',
                                           NULL :: bool,
                                           "angle_3",
                                           "gridx",
                                           "gridy",
                                           "here",
                                           "hhere",
                                           "i" + 1 AS "i_6",
                                           "loc_4",
                                           "max_angle",
                                           "resolution",
                                           "step",
                                           "there"
                                   WHERE NOT "q6_3")
                                 ) AS "ifresult7"
                                 ON True)
                                ON True)
                               ON True)
                              ON True)
                        WHERE "pred_2")
                         UNION ALL
                       (SELECT False,
                               NULL :: text,
                               (SELECT "angle" = "max_angle" AS "?column?") AS "result",
                               "run"."angle",
                               "run"."gridx",
                               "run"."gridy",
                               "run"."here",
                               "run"."hhere",
                               "run"."i",
                               "run"."loc",
                               "run"."max_angle",
                               "run"."resolution",
                               "run"."step",
                               "run"."there"
                        WHERE NOT "pred_2")
                      ) AS "ifresult2"
                      ON True)
                     ON True)
               WHERE "run"."label" = 'fori1_head'
              ) AS "result"
         WHERE "run"."rec?" = True)
    )
    SELECT "run"."res" AS "res"
    FROM run AS "run"
    WHERE "run"."rec?" = False
) AS run;
