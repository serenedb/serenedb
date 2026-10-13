
\set N :invocations
\set X :iterations
\set Y :iterations
\set STRIPE 8


SELECT setseed(0.425);

-- Trace the shape's bounding box in the 2D map, starting from varying points
-- on the map's left-hand side within the stripe.  Repeat N times.
SELECT run.res AS res
FROM   generate_series(1, :N) AS i,
       LATERAL (VALUES (floor(    random() * :STRIPE  + i - i),
                        floor(1 + random() * (:Y - 1) + i - i))) AS _(x,y),
       LATERAL (SELECT ((x, y) :: vec2) AS start) AS __(start),
       LATERAL
(
    WITH :MODE run("rec?",
                       "res",
                       "current",
                       "goal",
                       "bbox",
                       "track?") AS
    (
        (SELECT True,
               NULL :: box,
               "start" AS "current_1",
               NULL :: vec2,
               NULL :: box AS "bbox_1",
               False AS "track?_1"
        )
          UNION ALL
        (SELECT "result".*
         FROM run AS "run"("rec?",
                           "res",
                           "current",
                           "goal",
                           "bbox",
                           "track?"),
              LATERAL
              (SELECT "ifresult5".*
               FROM LATERAL
                     ((SELECT "ifresult7".*
                       FROM LATERAL
                             ((SELECT False,
                                      "bbox" AS "result",
                                      "run"."current",
                                      "run"."goal",
                                      "run"."bbox",
                                      "run"."track?"
                               WHERE "run"."track?" AND "current" = "goal")
                                UNION ALL
                              (SELECT "ifresult12".*
                               FROM (LATERAL
                                     (SELECT *
                                              FROM squares AS "RTE0"
                                              WHERE "RTE0"."xy" = "current"
                                     ) AS "square_4"
                                     LEFT OUTER JOIN
                                     (LATERAL
                                      (SELECT "RTE1".*
                                               FROM directions AS "RTE1"
                                               WHERE (("square_4".ll) = "RTE1"."ll"
                                                      AND
                                                      ("square_4".lr) = "RTE1"."lr"
                                                      AND
                                                      ("square_4".ul) = "RTE1"."ul"
                                                      AND
                                                      ("square_4".ur)
                                                      =
                                                      "RTE1"."ur")
                                      ) AS "dir_4"
                                      LEFT OUTER JOIN
                                      LATERAL
                                       ((SELECT True,
                                                NULL :: box,
                                                ((("current").x)
                                                         +
                                                         ((("dir_4").dir).x),
                                                         (("current").y)
                                                         +
                                                         ((("dir_4").dir).y)) :: vec2 AS "current_8",
                                                "current" AS "goal_5",
                                                bound_box(box(point(("current").x,("current").y)), box(point(("current").x, ("current").y))) AS "bbox_9",
                                                True AS "track?_6"
                                         WHERE (NOT "run"."track?" AND ("dir_4")."track?"))
                                          UNION ALL
                                        (((SELECT True,
                                                        NULL :: box,
                                                        ((("current").x)
                                                                 +
                                                                 ((("dir_4").dir).x),
                                                                 (("current").y)
                                                                 +
                                                                 ((("dir_4").dir).y)) :: vec2 AS "current_8",
                                                        "goal",
                                                        bound_box("bbox", box(point(("current").x, ("current").y))) AS "bbox_9",
                                                        "run"."track?"
                                                 WHERE (NOT ((NOT "run"."track?" AND ("dir_4")."track?"))) AND "run"."track?")
                                                  UNION ALL
                                                (SELECT True,
                                                         NULL :: box,
                                                         ((("current").x)
                                                                + ((("dir_4").dir).x),
                                                                (("current").y)
                                                                + ((("dir_4").dir).y)) :: vec2 AS "current_8",
                                                         "goal",
                                                         "bbox",
                                                         "run"."track?"
                                                 WHERE (NOT ((NOT "run"."track?" AND ("dir_4")."track?"))) AND NOT "run"."track?")
                                               )
                                         )
                                       ) AS "ifresult12"
                                      ON True)
                                     ON True)
                               WHERE NOT ("run"."track?" AND "current" = "goal"))
                             ) AS "ifresult7"
                       )
                     ) AS "ifresult5"
              ) AS "result"
         WHERE "run"."rec?" = True)
    )
    SELECT "run"."res" AS "res"
    FROM run AS "run"
    WHERE "run"."rec?" = False
    ) AS run;

