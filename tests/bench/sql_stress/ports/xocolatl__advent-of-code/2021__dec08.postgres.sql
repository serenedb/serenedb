CREATE SCHEMA IF NOT EXISTS aoc2021;
SET SCHEMA 'aoc2021';
DROP TABLE IF EXISTS dec08;

CREATE TABLE dec08 (
    line_number bigserial NOT NULL,
    input text NOT NULL,
    output text NOT NULL
);

\COPY dec08 (input, output) FROM '2021/dec08.input' DELIMITER '|'
VACUUM ANALYZE dec08;

/* FIRST STAR */

SELECT count(*)
FROM dec08
CROSS JOIN LATERAL unnest(string_to_array(output, ' ')) AS s
WHERE length(s) IN (2, 3, 4, 7)
;

/* SECOND STAR */

/*
 * Some---but by no means all---of this was inspired by Feike Steenbergen.
 * https://gitlab.com/feike/adventofcode/-/blob/master/2021/day08/08-2.sql
 */

WITH

/* Split all the inputs into arrays */
inputs (id, segments) AS (
    SELECT line_number, string_to_array(segments, NULL)
    FROM dec08
    CROSS JOIN LATERAL unnest(string_to_array(input, ' ')) AS s (segments)
    WHERE segments <> ''
),

/* Do the same for the outputs but here we need to remember the order */
outputs (id, segments, exp) AS (
    SELECT line_number,
           string_to_array(segments, NULL),
           row_number() OVER w - 1
    FROM dec08
    CROSS JOIN LATERAL unnest(string_to_array(trim(output), ' ')) WITH ORDINALITY AS s (segments)
    WHERE segments <> ''
    WINDOW w AS (PARTITION BY line_number ORDER BY ordinality DESC)
),

/* These are the easy ones */
unique_segments (id, segments, number) AS (
    SELECT i.id,
           i.segments,
           CASE cardinality(i.segments)
               WHEN 2 THEN 1
               WHEN 3 THEN 7
               WHEN 4 THEN 4
               WHEN 7 THEN 8
           END
    FROM inputs AS i
    WHERE cardinality(i.segments) IN (2, 3, 4, 7)
),

/*
 * Next look at the numbers with six segments.  They each contain within them a
 * different number of the previous numbers.
 */
six_segments (id, segments, number) AS (
    SELECT i.id,
           i.segments,
           CASE count(*) FILTER (WHERE i.segments @> us.segments)
               WHEN 0 THEN 6 /* Doesn't contain anything */
               WHEN 2 THEN 0 /* Contains 1 and 7 */
               WHEN 3 THEN 9 /* Contains 1, 4, and 7 */
           END
    FROM inputs AS i
    JOIN unique_segments AS us ON us.id = i.id
    WHERE cardinality(i.segments) = 6
    GROUP BY i.id, i.segments
),

/*
 * Finally we can look at the numbers with five segments.  Five is hiding in
 * both 6 and 9, but three is only hiding in 9, so we can distinguish them that
 * way.  The last one must be 2.
 */
five_segments (id, segments, number) AS (
    SELECT i.id,
           i.segments,
           CASE WHEN nine.segments @> i.segments
                THEN CASE WHEN six.segments @> i.segments
                          THEN 5
                          ELSE 3
                     END
                ELSE 2
           END
    FROM inputs AS i
    JOIN six_segments AS nine ON nine.id = i.id
    JOIN six_segments AS six ON six.id = i.id
    WHERE cardinality(i.segments) = 5
      AND nine.number = 9
      AND six.number = 6
),

/* Put them all together... */
digits AS (
    TABLE unique_segments
    UNION ALL
    TABLE six_segments
    UNION ALL
    TABLE five_segments
),

/* ...and use them to convert the outputs */
results (number) AS (
    SELECT sum(d.number * power(10, o.exp))
    FROM outputs AS o
    JOIN digits AS d ON d.id = o.id
                    AND cardinality(d.segments) = cardinality(o.segments)
                    AND d.segments @> o.segments
    GROUP BY d.id
)

/* Add it all up and we're done */
SELECT sum(number) AS second_star
FROM results
;
