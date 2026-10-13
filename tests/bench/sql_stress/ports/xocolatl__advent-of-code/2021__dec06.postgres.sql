CREATE SCHEMA IF NOT EXISTS aoc2021;
SET SCHEMA 'aoc2021';
DROP TABLE IF EXISTS dec06;

CREATE TABLE dec06 (
    line_number bigserial NOT NULL,
    value text NOT NULL
);

\COPY dec06 (value) FROM '2021/dec06.input'
VACUUM ANALYZE dec06;

/*
 * I tried to come up with a nice formula to calculate the number of fish on
 * any given day, but my math skills aren't what they used to be (if they ever
 * were) so I ended up resorting to naively moving buckets.
 *
 * This solution again uses just one query for both stars.
 */

WITH RECURSIVE

evolve (day, ttl0, ttl1, ttl2, ttl3, ttl4, ttl5, ttl6, ttl7, ttl8) AS (
    SELECT 0,
           count(*) FILTER (WHERE ttl = '0'),
           count(*) FILTER (WHERE ttl = '1'),
           count(*) FILTER (WHERE ttl = '2'),
           count(*) FILTER (WHERE ttl = '3'),
           count(*) FILTER (WHERE ttl = '4'),
           count(*) FILTER (WHERE ttl = '5'),
           count(*) FILTER (WHERE ttl = '6'),
           count(*) FILTER (WHERE ttl = '7'),
           count(*) FILTER (WHERE ttl = '8')
    FROM dec06
    CROSS JOIN LATERAL unnest(string_to_array(value, ',')) AS t (ttl)

    UNION ALL

    SELECT day+1,
           ttl1,
           ttl2,
           ttl3,
           ttl4,
           ttl5,
           ttl6,
           ttl7 + ttl0, /* The new fish entering maturity, and the old fish cycling */
           ttl8,
           ttl0
    FROM evolve
    WHERE day < 256
)

SELECT CASE day WHEN 80 THEN 'first' WHEN 256 THEN 'second' END AS star,
       ttl0 + ttl1 + ttl2 + ttl3 + ttl4 + ttl5 + ttl6 + ttl7 + ttl8 AS answer
FROM evolve
WHERE day IN (80, 256)
ORDER BY day
;
