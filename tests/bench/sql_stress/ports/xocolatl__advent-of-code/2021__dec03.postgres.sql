CREATE SCHEMA IF NOT EXISTS aoc2021;
SET SCHEMA 'aoc2021';
DROP TABLE IF EXISTS dec03;

CREATE TABLE dec03 (
    line_number bigserial NOT NULL,
    value text NOT NULL
);

\COPY dec03 (value) FROM '2021/dec03.input'
VACUUM ANALYZE dec03;

/* FIRST STAR */

/*
 * The actual work here is getting the mode() for each bit.  We do that by
 * blowing up the number into one digit per row, then group by the bit number
 * and get the mode from that group.
 */

SELECT sum(bit << exp) * sum((1 - bit) << exp) AS first_star
FROM (
    SELECT CAST(mode() WITHIN GROUP (ORDER BY b.bit) AS integer) AS bit,
           CAST(row_number() OVER (ORDER BY b.ordinality DESC) - 1 AS integer) AS exp
    FROM dec03 AS d
    CROSS JOIN LATERAL unnest(string_to_array(d.value, NULL)) WITH ORDINALITY AS b (bit)
    GROUP BY b.ordinality
) AS _
;

/* SECOND STAR */

/*
 * We need to progressively filter rows based on the next bit, so a RECURSIVE
 * query seems appropriate.  We just carry along which bit we're studying and
 * then stop when we have only one.
 *
 * Because we need to choose 1 over 0 if there are an equal number of bits, we
 * calculate the mode() in descending order.
 */

WITH RECURSIVE

oxygen (value, bit, count) AS (
    SELECT value, 1, count(*) OVER ()
    FROM dec03

    UNION ALL

    /*
     * We need to query the work table twice, but PostgreSQL does not allow
     * that, so copy it to some place where we CAN query it multiple times.
     */
    (WITH oxy AS (TABLE oxygen)
     SELECT o.value, o.bit+1, count(*) OVER ()
     FROM oxy AS o
     WHERE SUBSTRING(o.value FROM o.bit FOR 1) = (
            SELECT mode() WITHIN GROUP (ORDER BY SUBSTRING(oxy.value FROM o.bit FOR 1) DESC)
            FROM oxy)
       AND o.count > 1
    )
),

/* All of this is just to convert the oxygen value to decimal. */
oxygen_decimal (value) AS (
    SELECT sum(bit << exp)
    FROM (
        SELECT CAST(bit AS integer),
               CAST(row_number() OVER (ORDER BY ordinality DESC) - 1 AS integer) AS exp
        FROM (SELECT value FROM oxygen WHERE count = 1) AS o
        CROSS JOIN LATERAL unnest(string_to_array(o.value, NULL)) WITH ORDINALITY AS b (bit)
    ) AS _
),

/* This is just the same as for oxygen, except we want the non-mode value */
co2 (value, bit, count) AS (
    SELECT value, 1, count(*) OVER ()
    FROM dec03

    UNION ALL

    (WITH co2 AS (TABLE co2)
     SELECT c.value, c.bit+1, count(*) OVER ()
     FROM co2 AS c
     WHERE SUBSTRING(c.value FROM c.bit FOR 1) <> (
            SELECT mode() WITHIN GROUP (ORDER BY SUBSTRING(co2.value FROM c.bit FOR 1) DESC)
            FROM co2)
       AND c.count > 1
    )
),

/* All of this is just to convert the co2 value to decimal. */
co2_decimal (value) AS (
    SELECT sum(bit << exp)
    FROM (
        SELECT CAST(bit AS integer),
               CAST(row_number() OVER (ORDER BY ordinality DESC) - 1 AS integer) AS exp
        FROM (SELECT value FROM co2 WHERE count = 1) AS o
        CROSS JOIN LATERAL unnest(string_to_array(o.value, NULL)) WITH ORDINALITY AS b (bit)
    ) AS _
)

SELECT o.value * c.value AS second_star
FROM oxygen_decimal AS o
CROSS JOIN co2_decimal AS c
;
