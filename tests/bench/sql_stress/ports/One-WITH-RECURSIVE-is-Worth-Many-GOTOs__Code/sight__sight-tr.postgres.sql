
-- # of visibility computations (~ invocations)
\set N :invocations
\set OBSTACLES :iterations
\set WIDTH  (100 * :iterations)
\set HEIGHT (100 * :iterations)

-- Location of light source
\set LIGHT point(:WIDTH / 2, :HEIGHT / 2)

-- Render scene and lighted polygon using SVG

SELECT
(
  WITH :MODE run("rec?",
                     "label",
                     "res",
                     "counter3",
                     "counter7",
                     "intersections",
                     "lArray2",
                     "lArray6",
                     "phi",
                     "q4",
                     "q8") AS
  (
      (SELECT True,
               'foreacha1_head',
               NULL :: polygon,
               1+x-x,
               NULL :: int4,
               ARRAY[] :: point[] AS "intersections_1",
               "points_1" AS "lArray2_1",
               NULL :: point[],
               1.0e-3 AS "phi_1",
               array_length("points_1", 1) AS "q4_1",
               NULL :: int4
       FROM (SELECT array_agg("RTFunc1"."pt") AS "points_1"
             FROM scene AS "RTE0"("id", "color", "poly"),
                  LATERAL
                  unnest(points("RTE0"."poly")) AS "RTFunc1"("pt")
              ) AS "let20"("points_1"))
        UNION ALL
      (SELECT "result".*
       FROM run AS "run"("rec?",
                         "label",
                         "res",
                         "counter3",
                         "counter7",
                         "intersections",
                         "lArray2",
                         "lArray6",
                         "phi",
                         "q4",
                         "q8"),
            LATERAL
            ((SELECT "ifresult1".*
              FROM (LATERAL
                    (SELECT "counter3" <= "q4" AS "pred_2") AS "let0"("pred_2")
                    LEFT OUTER JOIN
                    LATERAL
                    ((SELECT True,
                             'foreacha5_head',
                             NULL :: polygon,
                             "counter3_4",
                             1,
                             "intersections",
                             "lArray2",
                             ARRAY["p0_3", "p1_3", "p2_3"] :: point[] AS "lArray6_3",
                             "phi",
                             "q4",
                             array_length(ARRAY["p0_3", "p1_3", "p2_3"] :: point[], 1) AS "q8_3"
                      FROM (LATERAL
                            (SELECT ("lArray2")["counter3"] AS "p1_3",
                                    "counter3" + 1 AS "counter3_4") AS "let2"("p1_3", "counter3_4")
                            LEFT OUTER JOIN
                            LATERAL
                              (SELECT point(((("light")[0])
                                             +
                                             (((("p1_3")[0]) - (("light")[0])) * (cos("phi"))))
                                            -
                                            (((("p1_3")[1]) - (("light")[1])) * (sin("phi"))),
                                            ((("light")[1])
                                             +
                                             (((("p1_3")[0]) - (("light")[0])) * (sin("phi"))))
                                            +
                                            (((("p1_3")[1]) - (("light")[1]))
                                             *
                                             (cos("phi")))) AS "p0_3",
                                    point(((("light")[0])
                                              +
                                              (((("p1_3")[0]) - (("light")[0])) * (cos(- "phi"))))
                                             -
                                             (((("p1_3")[1]) - (("light")[1])) * (sin(- "phi"))),
                                             ((("light")[1])
                                              +
                                              (((("p1_3")[0]) - (("light")[0])) * (sin(- "phi"))))
                                             +
                                             (((("p1_3")[1]) - (("light")[1]))
                                              *
                                              (cos(- "phi")))) AS "p2_3"
                              ) AS "let4"("p0_3", "p2_3")
                            ON True)
                      WHERE "pred_2")
                       UNION ALL
                     (SELECT False,
                              NULL :: text,
                              polygon("intersections_8") AS "result",
                              "run"."counter3",
                              "run"."counter7",
                              "run"."intersections",
                              "run"."lArray2",
                              "run"."lArray6",
                              "run"."phi",
                              "run"."q4",
                              "run"."q8"
                      FROM (SELECT array_agg("RTFunc5"."i"
                                                       ORDER BY (degrees(atan2((("light")[0])
                                                                               -
                                                                               (("RTFunc5"."i")[0]),
                                                                               (("light")[1])
                                                                               -
                                                                               (("RTFunc5"."i")[1])))) ASC) AS "intersections_8"
                                     FROM unnest("intersections") AS "RTFunc5"("i")
                                     WHERE "RTFunc5"."i" IS NOT NULL
                            ) AS "let9"("intersections_8")
                      WHERE NOT "pred_2")
                    ) AS "ifresult1"
                    ON True)
              WHERE "run"."label" = 'foreacha1_head')
               UNION ALL
             (SELECT "ifresult12".*
              FROM (LATERAL
                    (SELECT "counter7" <= "q8" AS "pred_5") AS "let11"("pred_5")
                    LEFT OUTER JOIN
                    LATERAL
                    ((SELECT True,
                             'foreacha5_head',
                             NULL :: polygon,
                             "counter3",
                             "counter7_6",
                             "intersections" || "ins_5" AS "intersections_6",
                             "lArray2",
                             "lArray6",
                             "phi",
                             "q4",
                             "q8"
                      FROM (LATERAL
                            (SELECT ("lArray6")["counter7"] AS "target_5",
                                    "counter7" + 1 AS "counter7_6") AS "let13"("target_5", "counter7_6")
                            LEFT OUTER JOIN
                             LATERAL
                              (SELECT (ray("light",
                                                   "target_5"))
                                              #
                                              (lseg("RTFunc4"."seg0",
                                                    "RTFunc4"."seg1")) AS "ins_5"
                                       FROM scene AS "RTE2"("id",
                                                            "color",
                                                            "poly"),
                                            LATERAL points("RTE2"."poly") AS "RTFunc3"("pts"),
                                            LATERAL
                                            ROWS FROM (unnest("RTFunc3"."pts"),
                                                       unnest((("RTFunc3"."pts")[2:])
                                                              ||
                                                              (("RTFunc3"."pts")[1]))) AS "RTFunc4"("seg0",
                                                                                                    "seg1")
                                       ORDER BY ("light"
                                                 <->
                                                 ((ray("light", "target_5"))
                                                  #
                                                  (lseg("RTFunc4"."seg0", "RTFunc4"."seg1")))) ASC
                                       LIMIT 1
                              ) AS "let15"("ins_5")
                            ON True)
                      WHERE "pred_5")
                       UNION ALL
                     (SELECT True,
                             'foreacha1_head',
                             NULL :: polygon,
                             "counter3",
                             "counter7",
                             "intersections",
                             "lArray2",
                             "lArray6",
                             "phi",
                             "q4",
                             "q8"
                      WHERE NOT "pred_5")
                    ) AS "ifresult12"
                    ON True)
              WHERE "run"."label" = 'foreacha5_head')
            ) AS "result"("rec?",
                          "label",
                          "res",
                          "counter3",
                          "counter7",
                          "intersections",
                          "lArray2",
                          "lArray6",
                          "phi",
                          "q4",
                          "q8")
       WHERE "run"."rec?" = True)
  )
  SELECT "run"."res" AS "res"
  FROM run AS "run"
  WHERE "run"."rec?" = False
) AS poly
FROM (SELECT :LIGHT, i FROM generate_series(1, :N) AS _(i)) AS _(light, x);



