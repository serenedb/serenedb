CREATE SCHEMA IF NOT EXISTS aoc2021;
SET SCHEMA 'aoc2021';
DROP TABLE IF EXISTS dec10;

CREATE TABLE dec10 (
    line_number bigserial NOT NULL,
    value text NOT NULL
);

\COPY dec10 (value) FROM '2021/dec10.input'
VACUUM ANALYZE dec10;

/* FIRST STAR */

WITH RECURSIVE

input (line_number, line) AS (
    SELECT line_number, string_to_array(value, NULL)
    FROM dec10
),

parse (line_number, line, stack, error, points) AS (
    SELECT line_number,
           line,
           CAST(ARRAY[] AS text[]),
           false,
           CAST(NULL AS integer)
    FROM input

    UNION ALL

    SELECT line_number,
           line[2:],

           CASE WHEN line[1] IN (')', ']', '}', '>')
                THEN stack[2:]
                ELSE array_prepend(line[1], stack)
           END,

           new_error,
           CASE WHEN new_error THEN
               CASE line[1]
                   WHEN ')' THEN 3
                   WHEN ']' THEN 57
                   WHEN '}' THEN 1197
                   WHEN '>' THEN 25137
               END
           END
    FROM parse
    CROSS JOIN LATERAL (VALUES (
           error
           OR (line[1] = ')' AND stack[1] <> '(')
           OR (line[1] = ']' AND stack[1] <> '[')
           OR (line[1] = '}' AND stack[1] <> '{')
           OR (line[1] = '>' AND stack[1] <> '<')
    )) AS v (new_error)
    WHERE line > '{}'
      AND NOT error
)

SELECT sum(points)
FROM parse
WHERE error
;

/* SECOND STAR */

WITH RECURSIVE

input (line_number, line) AS (
    SELECT line_number, string_to_array(value, NULL)
    FROM dec10
),

parse (line_number, line, stack, error, points) AS (
    SELECT line_number,
           line,
           CAST(ARRAY[] AS text[]),
           false,
           CAST(NULL AS integer)
    FROM input

    UNION ALL

    SELECT line_number,
           line[2:],

           CASE WHEN line[1] IN (')', ']', '}', '>')
                THEN stack[2:]
                ELSE array_prepend(line[1], stack)
           END,

           new_error,
           CASE WHEN new_error THEN
               CASE line[1]
                   WHEN ')' THEN 3
                   WHEN ']' THEN 57
                   WHEN '}' THEN 1197
                   WHEN '>' THEN 25137
               END
           END
    FROM parse
    CROSS JOIN LATERAL (VALUES (
           error
           OR (line[1] = ')' AND stack[1] <> '(')
           OR (line[1] = ']' AND stack[1] <> '[')
           OR (line[1] = '}' AND stack[1] <> '{')
           OR (line[1] = '>' AND stack[1] <> '<')
    )) AS v (new_error)
    WHERE line > '{}'
      AND NOT error
),

points (line_number, ordinality, points) AS (
    SELECT line_number,
           ordinality,
           CASE bracket
               WHEN '(' THEN 1
               WHEN '[' THEN 2
               WHEN '{' THEN 3
               WHEN '<' THEN 4
           END
    FROM parse
    CROSS JOIN LATERAL unnest(stack) WITH ORDINALITY AS u (bracket)
    WHERE NOT error
      AND line = '{}'
),

scores (line_number, ordinality, score) AS (
    SELECT line_number, ordinality, CAST(points AS bigint)
    FROM points
    WHERE ordinality = 1

    UNION ALL

    SELECT p.line_number,
           p.ordinality,
           5 * s.score + p.points
    FROM scores AS s
    JOIN points AS p ON (p.line_number, p.ordinality) = (s.line_number, s.ordinality + 1)
)

SELECT score AS second_star
FROM (
    SELECT line_number,
           max(score) AS score,
           rank() OVER (ORDER BY max(score) ASC)  AS rank_asc,
           rank() OVER (ORDER BY max(score) DESC) AS rank_desc
    FROM scores
    GROUP BY line_number
) AS _
WHERE rank_asc = rank_desc
;
