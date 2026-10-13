CREATE SCHEMA IF NOT EXISTS aoc2021;
SET SCHEMA 'aoc2021';
DROP TABLE IF EXISTS dec04;

CREATE TABLE dec04 (
    line_number bigserial NOT NULL,
    value text NOT NULL
);

\COPY dec04 (value) FROM '2021/dec04.input'
VACUUM ANALYZE dec04;

/*
 * The difference today between the first and second stars is just a sort
 * order, and since the challenge (for SQL) is getting the input into a usable
 * state, we combine both stars into one query.
 */

WITH

/*
 * Here we are just generating a list of calls.  It is a "table" of arrays
 * where each row has one more call added to it.
 */
calls (num, calls) AS (
    SELECT ordinality, array_agg(CAST(call AS bigint)) OVER (ORDER BY ordinality)
    FROM dec04
    CROSS JOIN LATERAL unnest(string_to_array(value, ',')) WITH ORDINALITY AS s (call)
    WHERE line_number = 1
),

/* This pulls out the boards into a single array of numbers */
boards (id, board) AS (
    SELECT min(line_number),
           CAST(regexp_split_to_array(trim(
                    string_agg(value, ' ' ORDER BY line_number)), '\s+')
                AS bigint[])
    FROM dec04
    WHERE line_number > 1
    GROUP BY (line_number - 3) / 6
),

/* And this gives us arrays of each winning combination. */
board_wins (id, board, wins) AS (
    /* What a mess this thing is.  There has got to be a better way... */
    SELECT id, board, board[ 1: 5] FROM boards
    UNION ALL
    SELECT id, board, board[ 6:10] FROM boards
    UNION ALL
    SELECT id, board, board[11:15] FROM boards
    UNION ALL
    SELECT id, board, board[16:20] FROM boards
    UNION ALL
    SELECT id, board, board[21:25] FROM boards
    UNION ALL
    SELECT id, board, array_agg(square) FILTER (WHERE mod(ordinality-1, 5) = 0)
    FROM boards
    CROSS JOIN LATERAL unnest(board) WITH ORDINALITY AS u (square)
    GROUP BY id, board
    UNION ALL
    SELECT id, board, array_agg(square) FILTER (WHERE mod(ordinality-1, 5) = 1)
    FROM boards
    CROSS JOIN LATERAL unnest(board) WITH ORDINALITY AS u (square)
    GROUP BY id, board
    UNION ALL
    SELECT id, board, array_agg(square) FILTER (WHERE mod(ordinality-1, 5) = 2)
    FROM boards
    CROSS JOIN LATERAL unnest(board) WITH ORDINALITY AS u (square)
    GROUP BY id, board
    UNION ALL
    SELECT id, board, array_agg(square) FILTER (WHERE mod(ordinality-1, 5) = 3)
    FROM boards
    CROSS JOIN LATERAL unnest(board) WITH ORDINALITY AS u (square)
    GROUP BY id, board
    UNION ALL
    SELECT id, board, array_agg(square) FILTER (WHERE mod(ordinality-1, 5) = 4)
    FROM boards
    CROSS JOIN LATERAL unnest(board) WITH ORDINALITY AS u (square)
    GROUP BY id, board
),

/*
 * Now the problem is simple: we just need to find which call array finishes
 * the win for each board.  We are using Standard SQL here.  A more PostgreSQL
 * way of doing it would be with DISTINCT ON.
 */
terminals (id, board, calls) AS (
    SELECT DISTINCT
        bw.id,
        bw.board,
        first_value(c.calls) OVER (PARTITION BY bw.id ORDER BY c.num)
    FROM board_wins AS bw
    JOIN calls AS c ON c.calls @> bw.wins
),

/* The winner has the fewest calls */
winner (board, calls) AS (
    SELECT board, calls
    FROM terminals
    ORDER BY cardinality(calls) ASC
    FETCH FIRST ROW ONLY
),

/* The loser has the most calls */
loser (board, calls) AS (
    SELECT board, calls
    FROM terminals
    ORDER BY cardinality(calls) DESC
    FETCH FIRST ROW ONLY
),

/* These next two are identical and calculate the scores */
winner_score (first_star) AS (
    SELECT s.sum * w.calls[cardinality(w.calls)]
    FROM winner AS w
    CROSS JOIN LATERAL (
        SELECT sum(u.value)
        FROM unnest(w.board) AS u (value)
        WHERE u.value <> ALL (w.calls)
    ) AS s
),

loser_score (second_star) AS (
    SELECT s.sum * l.calls[cardinality(l.calls)]
    FROM loser AS l
    CROSS JOIN LATERAL (
        SELECT sum(u.value)
        FROM unnest(l.board) AS u (value)
        WHERE u.value <> ALL (l.calls)
    ) AS s
)

/* All the work has been done, we just need to return it. */
VALUES ((TABLE winner_score), (TABLE loser_score))
;
