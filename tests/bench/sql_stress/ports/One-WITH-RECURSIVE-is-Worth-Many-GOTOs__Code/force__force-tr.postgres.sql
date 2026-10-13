
\set area box(point(0,0), point(1000,1000))
\set F :invocations

SELECT setseed(0.42);

SELECT run.res
FROM (SELECT (point(width(:area) * random(), width(:area) * random()), 1.0) :: bodies, 0.5 FROM generate_series(1, :F)) AS _(body, theta),
  LATERAL (
    WITH :MODE run("rec?",
                   "res",
                   "force",
                   "g",
                   "q") AS
    (
        SELECT True,
                NULL :: point,
                point(0, 0) AS "force_1",
                6.67e-11 AS "g_1",
                ARRAY["node_1"] :: barneshut[] AS "q_1"
         FROM (SELECT "RTE0" AS "b"
                FROM barneshut AS "RTE0"("node", "bbox", "parent", "mass", "center")
                WHERE "RTE0"."node" = 0
              ) AS "let2"("node_1")
          UNION ALL
        (SELECT "result".*
         FROM run AS "run"("rec?", "res", "force", "g", "q"),
              LATERAL
              (SELECT "ifresult6".*
               FROM (LATERAL
                     (SELECT (cardinality("q")) > 0 AS "pred_2") AS "let5"("pred_2")
                     LEFT OUTER JOIN
                     LATERAL
                     ((SELECT "ifresult13".*
                       FROM (LATERAL
                             (SELECT ("q")[1] AS "node_4", ("q")[2:] AS "q_4") AS "let7"("node_4", "q_4")
                              LEFT OUTER JOIN
                              (LATERAL
                               (SELECT greatest((("node_4").center)
                                                <->
                                                (("body").pos),
                                                1.0e-10) AS "dist_3",
                                       (("node_4").center)
                                        -
                                        (("body").pos) AS "dir_3",
                                        point(0, 0) AS "grav_3",
                                        NOT EXISTS (SELECT 1 AS "?column?"
                                                      FROM walls AS "RTE1"("wall")
                                                      WHERE ((("body").pos)
                                                             <=
                                                             ((("body").pos)
                                                              ##
                                                              "RTE1"."wall"))
                                                            <>
                                                            ((("node_4").center)
                                                             <=
                                                             ((("node_4").center)
                                                              ##
                                                              "RTE1"."wall"))) AS "q5_3"
                               ) AS "let9"("dist_3", "dir_3", "grav_3", "q5_3")
                                  LEFT OUTER JOIN
                                  LATERAL
                                  ((SELECT "ifresult16".*
                                    FROM (LATERAL
                                          (SELECT ((("g" * (("body").mass))
                                                    *
                                                    (("node_4").mass)) / ("dist_3" ^ (2))) * "dir_3" AS "grav_8",
                                                  (("node_4").node IS NULL
                                                    OR
                                                    ((width(("node_4").bbox)) / "dist_3") < "theta") AS "q9_4"
                                          ) AS "let14"("grav_8", "q9_4")
                                           LEFT OUTER JOIN
                                           LATERAL
                                           ((SELECT True,
                                                    NULL :: point,
                                                    "force" + "grav_8" AS "force_7",
                                                    "g",
                                                    "q_4"
                                             WHERE "q9_4")
                                              UNION ALL
                                            (SELECT True,
                                                    NULL :: point,
                                                    "force",
                                                    "g",
                                                    "q_4" || (SELECT array_agg("RTE2") AS "array_agg"
                                                            FROM barneshut AS "RTE2"
                                                            WHERE "RTE2"."parent" = (("node_4").node)) AS "q_7"
                                             WHERE NOT "q9_4")
                                           ) AS "ifresult16"
                                          ON True)
                                    WHERE "q5_3")
                                     UNION ALL
                                   (SELECT "ifresult23".*
                                    FROM (LATERAL
                                          (SELECT (("node_4").node IS NULL
                                                   OR
                                                   ((width(("node_4").bbox)) / "dist_3") < "theta") AS "q9_4"
                                          ) AS "let22"("q9_4")
                                          LEFT OUTER JOIN
                                          LATERAL
                                          ((SELECT True,
                                                    NULL :: point,
                                                    "force" + "grav_3" AS "force_7",
                                                    "g",
                                                    "q_4"
                                            WHERE "q9_4")
                                             UNION ALL
                                           (SELECT True,
                                                   NULL :: point,
                                                   "force",
                                                   "g",
                                                   "q_4" || (SELECT array_agg("RTE2") AS "array_agg"
                                                             FROM barneshut AS "RTE2"
                                                             WHERE "RTE2"."parent" = (("node_4").node)) AS "q_7"
                                            WHERE NOT "q9_4")
                                          ) AS "ifresult23"
                                          ON True)
                                    WHERE NOT "q5_3")
                                  ) AS "ifresult13"
                                 ON True)
                             ON True)
                       WHERE "pred_2")
                        UNION ALL
                      (SELECT False,
                              "force" AS "result",
                              "run"."force",
                              "run"."g",
                              "run"."q"
                       WHERE NOT "pred_2")
                     ) AS "ifresult6"
                     ON True)
              ) AS "result"
         WHERE "run"."rec?" = True)
    )
    SELECT "run"."res" AS "res"
    FROM run AS "run"
    WHERE "run"."rec?" = False
    ) AS run;
