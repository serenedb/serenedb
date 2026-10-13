CREATE SCHEMA IF NOT EXISTS aoc2022;
SET SCHEMA 'aoc2022';
DROP TABLE IF EXISTS dec02;

CREATE TABLE dec02 (
    line_number bigserial NOT NULL,
    line text
);

\COPY dec02 (line) FROM '2022/dec02.input'
VACUUM ANALYZE dec02;

/* FIRST STAR */

WITH

outcomes (play, score) AS (
    VALUES ('A X', 3+1),
           ('A Y', 6+2),
           ('A Z', 0+3),
           ('B X', 0+1),
           ('B Y', 3+2),
           ('B Z', 6+3),
           ('C X', 6+1),
           ('C Y', 0+2),
           ('C Z', 3+3)
)

SELECT SUM(o.score) AS first_star
FROM dec02 AS d
JOIN outcomes AS o ON o.play = d.line
;

/* SECOND STAR */

WITH

outcomes (play, score) AS (
    VALUES ('A X', 0+3),
           ('A Y', 3+1),
           ('A Z', 6+2),
           ('B X', 0+1),
           ('B Y', 3+2),
           ('B Z', 6+3),
           ('C X', 0+2),
           ('C Y', 3+3),
           ('C Z', 6+1)
)

SELECT SUM(o.score) AS second_star
FROM dec02 AS d
JOIN outcomes AS o ON o.play = d.line
;
