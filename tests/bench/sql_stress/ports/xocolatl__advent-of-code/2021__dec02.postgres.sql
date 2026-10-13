CREATE SCHEMA IF NOT EXISTS aoc2021;
SET SCHEMA 'aoc2021';
DROP TABLE IF EXISTS dec02;

CREATE TABLE dec02 (
    line_number bigserial NOT NULL,
    command text NOT NULL,
    amount bigint NOT NULL
);

\COPY dec02 (command, amount) FROM '2021/dec02.input' DELIMITER ' '
VACUUM ANALYZE dec02;

/* FIRST STAR */

/*
 * The instructions for this task are very procedural in nature, but that is
 * not the best way to think about things in SQL.  It doesn't matter if we go
 * down X and up Y, or if we first go up Y and down X, and it doesn't matter in
 * which order we go forward compared to the ups and down.  So the best way to
 * do this in SQL is to aggregate over the data and spit out the result.
 *
 * I think it is a little bit cleaner to separate out the values first and then
 * plug them into the formula, but of course this can be done directly without
 * the subquery.
 */

SELECT forward * (down - up) AS first_star
FROM (
    SELECT sum(amount) FILTER (WHERE command = 'forward') AS forward,
           sum(amount) FILTER (WHERE command = 'down') AS down,
           sum(amount) FILTER (WHERE command = 'up') AS up
    FROM dec02
) AS _
;

/* SECOND STAR */

/*
 * This one is a little harder because the order of operations DOES matter.
 *
 * There are two basic ways to do this.  The first is with a RECURSIVE query,
 * stepping through each command one at a time.  That is easy to write but
 * isn't very idiomatic for something like this.
 *
 * The approach taken here is the second one, which is to use a window function
 * to calculate the aim, and then just aggregate it like in the previous query.
 */

SELECT sum(amount) * sum(amount * aim) AS second_star
FROM (
    SELECT command,
           amount,
           sum(CASE command
                   WHEN 'down' THEN +amount
                   WHEN 'up'   THEN -amount
               END) OVER (ORDER BY line_number) AS aim
    FROM dec02
) AS _
WHERE command = 'forward'
;

