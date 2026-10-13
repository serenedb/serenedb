CREATE SCHEMA IF NOT EXISTS aoc2022;
SET SCHEMA 'aoc2022';
DROP TABLE IF EXISTS dec03;

CREATE TABLE dec03 (
    line_number bigserial NOT NULL,
    line text
);

\COPY dec03 (line) FROM '2022/dec03.input'
VACUUM ANALYZE dec03;

/* FIRST STAR */

WITH

compartments (bag, first, second) AS (
    SELECT d.line_number,
           SUBSTRING(d.line FROM 1 FOR v.length),
           SUBSTRING(d.line FROM v.length+1 FOR v.length)
   FROM dec03 AS d
   CROSS JOIN LATERAL (VALUES (LENGTH(d.line) / 2)) AS v (length)
),

priorities (bag, item) AS (
    SELECT c.bag, i.item
    FROM compartments AS c
    CROSS JOIN LATERAL unnest(string_to_array(c.first, NULL)) AS i (item)

    INTERSECT DISTINCT

    SELECT c.bag, i.item
    FROM compartments AS c
    CROSS JOIN LATERAL unnest(string_to_array(c.second, NULL)) AS i (item)
)

SELECT SUM(POSITION(p.item IN 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'))
FROM priorities AS p
;

/* SECOND STAR */

WITH

rucksacks (grp, elf, item) AS (
    SELECT (d.line_number - 1) / 3,
           MOD(d.line_number - 1, 3),
           i.item
    FROM dec03 AS d
    CROSS JOIN LATERAL unnest(string_to_array(d.line, NULL)) AS i (item)
),

badges (grp, item) AS (
    SELECT r.grp, r.item
    FROM rucksacks AS r
    WHERE r.elf = 0

    INTERSECT DISTINCT

    SELECT r.grp, r.item
    FROM rucksacks AS r
    WHERE r.elf = 1

    INTERSECT DISTINCT

    SELECT r.grp, r.item
    FROM rucksacks AS r
    WHERE r.elf = 2
)

SELECT SUM(POSITION(b.item IN 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'))
FROM badges AS b
;
