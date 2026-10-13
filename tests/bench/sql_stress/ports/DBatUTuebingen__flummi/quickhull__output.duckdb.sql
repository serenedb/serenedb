WITH RECURSIVE
  "🔄"("#️⃣", "🏷️", "📊.1", "left", "right") AS (
    (SELECT CAST((0) AS INTEGER) AS "#️⃣",
            CAST(('start.1') AS VARCHAR) AS "🏷️",
            CAST((NULL) AS STRUCT(x DOUBLE, y DOUBLE)) AS "📊.1",
            CAST((NULL) AS STRUCT(x DOUBLE, y DOUBLE)) AS "left",
            CAST((NULL) AS STRUCT(x DOUBLE, y DOUBLE)) AS "right")
      UNION ALL
    (WITH
       "start.1"("#️⃣", "⚙️") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.1'
       ),
       "fork.1"("#️⃣", "⚙️", "next") AS (
         SELECT "start.1"."#️⃣" AS "#️⃣",
                "start.1"."⚙️" AS "⚙️",
                CAST(("ℚ"."next") AS STRUCT(x DOUBLE, y DOUBLE)) AS "next"
         FROM   "start.1",
                (SELECT p FROM points AS p) AS "ℚ"("next")
       ),
       "gather.1"("#️⃣", "⚙️", "min", "max") AS (
         SELECT "fork.1"."#️⃣" AS "#️⃣",
                "fork.1"."⚙️" AS "⚙️",
                CAST((arg_min(("fork.1"."next"), ("fork.1"."next").x)) AS STRUCT(x DOUBLE, y DOUBLE)) AS "min",
                CAST((arg_max(("fork.1"."next"), ("fork.1"."next").x)) AS STRUCT(x DOUBLE, y DOUBLE)) AS "max"
         FROM   "fork.1"
         GROUP  BY "fork.1"."#️⃣",
                   "fork.1"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "emit.1"("#️⃣", "⚙️", "📊.1", "min", "max") AS (
         SELECT "gather.1"."#️⃣" AS "#️⃣",
                "gather.1"."⚙️" AS "⚙️",
                "gather.1"."min" AS "📊.1",
                "gather.1"."min" AS "min",
                "gather.1"."max" AS "max"
         FROM   "gather.1"
       ),
       "emit.2"("#️⃣", "⚙️", "📊.1", "min", "max") AS (
         SELECT "emit.1"."#️⃣" AS "#️⃣",
                "emit.1"."⚙️" AS "⚙️",
                "emit.1"."max" AS "📊.1",
                "emit.1"."min" AS "min",
                "emit.1"."max" AS "max"
         FROM   "emit.1"
       ),
       "fork.2"("#️⃣", "⚙️", "min", "max", "flip") AS (
         SELECT "emit.2"."#️⃣" AS "#️⃣",
                "emit.2"."⚙️" AS "⚙️",
                "emit.2"."min" AS "min",
                "emit.2"."max" AS "max",
                CAST(("ℚ"."flip") AS BOOLEAN) AS "flip"
         FROM   "emit.2",
                (VALUES (TRUE), (FALSE)) AS "ℚ"("flip")
       ),
       "assignment.1"("#️⃣", "⚙️", "🔍", "min", "max") AS (
         SELECT "fork.2"."#️⃣" AS "#️⃣",
                "fork.2"."⚙️" AS "⚙️",
                CAST((("fork.2"."flip")) AS BOOLEAN) AS "🔍",
                "fork.2"."min" AS "min",
                "fork.2"."max" AS "max"
         FROM   "fork.2"
       ),
       "start.2"("#️⃣", "⚙️", "left", "right") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️",
                "🔄"."left" AS "left",
                "🔄"."right" AS "right"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.2'
       ),
       "where.1"("#️⃣", "⚙️", "min", "max") AS (
         SELECT "assignment.1"."#️⃣" AS "#️⃣",
                "assignment.1"."⚙️" AS "⚙️",
                "assignment.1"."min" AS "min",
                "assignment.1"."max" AS "max"
         FROM   "assignment.1"
         WHERE  "assignment.1"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "assignment.2"("#️⃣", "⚙️", "left", "right") AS (
         SELECT "where.1"."#️⃣" AS "#️⃣",
                "where.1"."⚙️" AS "⚙️",
                CAST((("where.1"."min")) AS STRUCT(x DOUBLE, y DOUBLE)) AS "left",
                CAST((("where.1"."max")) AS STRUCT(x DOUBLE, y DOUBLE)) AS "right"
         FROM   "where.1"
       ),
       "where.2"("#️⃣", "⚙️", "min", "max") AS (
         SELECT "assignment.1"."#️⃣" AS "#️⃣",
                "assignment.1"."⚙️" AS "⚙️",
                "assignment.1"."min" AS "min",
                "assignment.1"."max" AS "max"
         FROM   "assignment.1"
         WHERE  "assignment.1"."🔍" IS DISTINCT FROM TRUE
       ),
       "assignment.3"("#️⃣", "⚙️", "left", "right") AS (
         SELECT "where.2"."#️⃣" AS "#️⃣",
                "where.2"."⚙️" AS "⚙️",
                CAST((("where.2"."max")) AS STRUCT(x DOUBLE, y DOUBLE)) AS "left",
                CAST((("where.2"."min")) AS STRUCT(x DOUBLE, y DOUBLE)) AS "right"
         FROM   "where.2"
       ),
       "merge.1"("#️⃣", "⚙️", "left", "right") AS (
         (SELECT "assignment.2"."#️⃣" AS "#️⃣",
                 "assignment.2"."⚙️" AS "⚙️",
                 "assignment.2"."left" AS "left",
                 "assignment.2"."right" AS "right"
          FROM   "assignment.2")
           UNION ALL
         (SELECT "assignment.3"."#️⃣" AS "#️⃣",
                 "assignment.3"."⚙️" AS "⚙️",
                 "assignment.3"."left" AS "left",
                 "assignment.3"."right" AS "right"
          FROM   "assignment.3")
       ),
       "merge.2"("#️⃣", "⚙️", "left", "right") AS (
         (SELECT "merge.1"."#️⃣" AS "#️⃣",
                 "merge.1"."⚙️" AS "⚙️",
                 "merge.1"."left" AS "left",
                 "merge.1"."right" AS "right"
          FROM   "merge.1")
           UNION ALL
         (SELECT "start.2"."#️⃣" AS "#️⃣",
                 "start.2"."⚙️" AS "⚙️",
                 "start.2"."left" AS "left",
                 "start.2"."right" AS "right"
          FROM   "start.2")
       ),
       "assignment.4"("#️⃣", "⚙️", "next", "left", "right") AS (
         SELECT "merge.2"."#️⃣" AS "#️⃣",
                "merge.2"."⚙️" AS "⚙️",
                CAST((WITH
                         distances(point, distance) AS (
                           SELECT p,
                                  (
                                    dy * p.x -
                                    dx * p.y +
                                    ("merge.2"."left").x * ("merge.2"."right").y -
                                    ("merge.2"."left").y * ("merge.2"."right").x
                                  ) / sqrt(dx * dx + dy * dy)
                           FROM   points AS p,
                                  (SELECT ("merge.2"."left").y - ("merge.2"."right").y AS dy,
                                          ("merge.2"."left").x - ("merge.2"."right").x AS dx)
                           WHERE  p <> ("merge.2"."right")
                           AND    p <> ("merge.2"."left")
                         )
                       SELECT arg_max(point, distance)
                       FROM   distances
                       WHERE  distance > 0) AS STRUCT(x DOUBLE, y DOUBLE)) AS "next",
                "merge.2"."left" AS "left",
                "merge.2"."right" AS "right"
         FROM   "merge.2"
       ),
       "assignment.5"("#️⃣", "⚙️", "🔍", "next", "left", "right") AS (
         SELECT "assignment.4"."#️⃣" AS "#️⃣",
                "assignment.4"."⚙️" AS "⚙️",
                CAST((("assignment.4"."next") IS NULL) AS BOOLEAN) AS "🔍",
                "assignment.4"."next" AS "next",
                "assignment.4"."left" AS "left",
                "assignment.4"."right" AS "right"
         FROM   "assignment.4"
       ),
       "where.3"("⚙️") AS (
         SELECT "assignment.5"."⚙️" AS "⚙️"
         FROM   "assignment.5"
         WHERE  "assignment.5"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "stop.1"("⚙️") AS (
         SELECT "where.3"."⚙️"
         FROM   "where.3"
         WHERE  FALSE
       ),
       "where.4"("#️⃣", "⚙️", "next", "left", "right") AS (
         SELECT "assignment.5"."#️⃣" AS "#️⃣",
                "assignment.5"."⚙️" AS "⚙️",
                "assignment.5"."next" AS "next",
                "assignment.5"."left" AS "left",
                "assignment.5"."right" AS "right"
         FROM   "assignment.5"
         WHERE  "assignment.5"."🔍" IS DISTINCT FROM TRUE
       ),
       "emit.3"("#️⃣", "⚙️", "📊.1", "next", "left", "right") AS (
         SELECT "where.4"."#️⃣" AS "#️⃣",
                "where.4"."⚙️" AS "⚙️",
                "where.4"."next" AS "📊.1",
                "where.4"."next" AS "next",
                "where.4"."left" AS "left",
                "where.4"."right" AS "right"
         FROM   "where.4"
       ),
       "fork.3"("#️⃣", "⚙️", "next", "flip", "left", "right") AS (
         SELECT "emit.3"."#️⃣" AS "#️⃣",
                "emit.3"."⚙️" AS "⚙️",
                "emit.3"."next" AS "next",
                CAST(("ℚ"."flip") AS BOOLEAN) AS "flip",
                "emit.3"."left" AS "left",
                "emit.3"."right" AS "right"
         FROM   "emit.3",
                (VALUES (TRUE), (FALSE)) AS "ℚ"("flip")
       ),
       "assignment.6"("#️⃣", "⚙️", "🔍", "next", "left", "right") AS (
         SELECT "fork.3"."#️⃣" AS "#️⃣",
                "fork.3"."⚙️" AS "⚙️",
                CAST((("fork.3"."flip")) AS BOOLEAN) AS "🔍",
                "fork.3"."next" AS "next",
                "fork.3"."left" AS "left",
                "fork.3"."right" AS "right"
         FROM   "fork.3"
       ),
       "where.5"("#️⃣", "⚙️", "next", "right") AS (
         SELECT "assignment.6"."#️⃣" AS "#️⃣",
                "assignment.6"."⚙️" AS "⚙️",
                "assignment.6"."next" AS "next",
                "assignment.6"."right" AS "right"
         FROM   "assignment.6"
         WHERE  "assignment.6"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "assignment.7"("#️⃣", "⚙️", "left", "right") AS (
         SELECT "where.5"."#️⃣" AS "#️⃣",
                "where.5"."⚙️" AS "⚙️",
                CAST((("where.5"."next")) AS STRUCT(x DOUBLE, y DOUBLE)) AS "left",
                "where.5"."right" AS "right"
         FROM   "where.5"
       ),
       "jump.1"("#️⃣", "🏷️", "left", "right") AS (
         SELECT "assignment.7"."#️⃣" AS "#️⃣",
                'start.2' AS "🏷️",
                "assignment.7"."left" AS "left",
                "assignment.7"."right" AS "right"
         FROM   "assignment.7"
       ),
       "where.6"("#️⃣", "⚙️", "next", "left") AS (
         SELECT "assignment.6"."#️⃣" AS "#️⃣",
                "assignment.6"."⚙️" AS "⚙️",
                "assignment.6"."next" AS "next",
                "assignment.6"."left" AS "left"
         FROM   "assignment.6"
         WHERE  "assignment.6"."🔍" IS DISTINCT FROM TRUE
       ),
       "assignment.8"("#️⃣", "⚙️", "left", "right") AS (
         SELECT "where.6"."#️⃣" AS "#️⃣",
                "where.6"."⚙️" AS "⚙️",
                "where.6"."left" AS "left",
                CAST((("where.6"."next")) AS STRUCT(x DOUBLE, y DOUBLE)) AS "right"
         FROM   "where.6"
       ),
       "jump.2"("#️⃣", "🏷️", "left", "right") AS (
         SELECT "assignment.8"."#️⃣" AS "#️⃣",
                'start.2' AS "🏷️",
                "assignment.8"."left" AS "left",
                "assignment.8"."right" AS "right"
         FROM   "assignment.8"
       )
     (SELECT CAST(("emit.1"."#️⃣") AS INTEGER) AS "#️⃣",
             CAST((NULL) AS VARCHAR) AS "🏷️",
             CAST(("emit.1"."📊.1") AS STRUCT(x DOUBLE, y DOUBLE)) AS "📊.1",
             CAST((NULL) AS STRUCT(x DOUBLE, y DOUBLE)) AS "left",
             CAST((NULL) AS STRUCT(x DOUBLE, y DOUBLE)) AS "right"
      FROM   "emit.1")
       UNION ALL
     (SELECT CAST(("emit.2"."#️⃣") AS INTEGER) AS "#️⃣",
             CAST((NULL) AS VARCHAR) AS "🏷️",
             CAST(("emit.2"."📊.1") AS STRUCT(x DOUBLE, y DOUBLE)) AS "📊.1",
             CAST((NULL) AS STRUCT(x DOUBLE, y DOUBLE)) AS "left",
             CAST((NULL) AS STRUCT(x DOUBLE, y DOUBLE)) AS "right"
      FROM   "emit.2")
       UNION ALL
     (SELECT CAST(("emit.3"."#️⃣") AS INTEGER) AS "#️⃣",
             CAST((NULL) AS VARCHAR) AS "🏷️",
             CAST(("emit.3"."📊.1") AS STRUCT(x DOUBLE, y DOUBLE)) AS "📊.1",
             CAST((NULL) AS STRUCT(x DOUBLE, y DOUBLE)) AS "left",
             CAST((NULL) AS STRUCT(x DOUBLE, y DOUBLE)) AS "right"
      FROM   "emit.3")
       UNION ALL
     (SELECT CAST(("jump.1"."#️⃣" + 1) AS INTEGER) AS "#️⃣",
             CAST(("jump.1"."🏷️") AS VARCHAR) AS "🏷️",
             CAST((NULL) AS STRUCT(x DOUBLE, y DOUBLE)) AS "📊.1",
             CAST(("jump.1"."left") AS STRUCT(x DOUBLE, y DOUBLE)) AS "left",
             CAST(("jump.1"."right") AS STRUCT(x DOUBLE, y DOUBLE)) AS "right"
      FROM   "jump.1")
       UNION ALL
     (SELECT CAST(("jump.2"."#️⃣" + 1) AS INTEGER) AS "#️⃣",
             CAST(("jump.2"."🏷️") AS VARCHAR) AS "🏷️",
             CAST((NULL) AS STRUCT(x DOUBLE, y DOUBLE)) AS "📊.1",
             CAST(("jump.2"."left") AS STRUCT(x DOUBLE, y DOUBLE)) AS "left",
             CAST(("jump.2"."right") AS STRUCT(x DOUBLE, y DOUBLE)) AS "right"
      FROM   "jump.2"))
  )
SELECT "🔄"."📊.1"
FROM   "🔄"
WHERE  "🔄"."🏷️" IS NULL;