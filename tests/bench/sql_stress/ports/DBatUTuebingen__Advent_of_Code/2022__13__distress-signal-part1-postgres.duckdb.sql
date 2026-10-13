SET integer_division = true;
DROP TABLE IF EXISTS input;
CREATE TABLE input(row SERIAL, signal jsonb);

COPY input(signal) FROM '2022/13/input.txt' WITH csv DELIMITER '|'; -- -sample2

CREATE OR REPLACE FUNCTION remove_first(jb jsonb) RETURNS jsonb AS $$
    SELECT NULL:: jsonb
    WHERE jsonb_typeof(jb) = 'number'
    UNION
    SELECT jb - 0
    WHERE jsonb_typeof(jb) = 'array' AND jsonb_array_length(jb) > 1
    --    OR jsonb_typeof(jb) = 'array' AND jsonb_array_length(jb) = 1 AND jsonb_typeof(jb - 0) = 'array' -- commenting out this *should* make a difference, but doesn't somehow
    UNION
    SELECT NULL::jsonb
    WHERE jsonb_typeof(jb) = 'array' AND jsonb_array_length(jb) = 0
    --    OR jsonb_typeof(jb) = 'array' AND jsonb_array_length(jb) = 1 AND jsonb_typeof(jb - 0) = 'number' -- commenting out this *should* make a difference, but doesn't somehow
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION prepend(pp jsonb, jb jsonb) RETURNS jsonb AS $$
    SELECT jb WHERE pp IS NULL
    UNION
    SELECT pp WHERE jb IS NULL
    UNION
    SELECT jsonb_build_array(pp) || jb WHERE jb IS NOT NULL
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION true_case(fst jsonb, snd jsonb, f jsonb, s jsonb) RETURNS boolean AS $$
    SELECT f IS NULL
       AND (s IS NOT NULL OR (fst IS NULL AND snd IS NULL))
        OR (f = ('null'::jsonb)
            AND ((s <> ('null'::jsonb) AND s <> ('[]'::jsonb))
              OR (fst IS NULL AND snd IS NULL)))
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION false_case(fst jsonb, snd jsonb, f jsonb, s jsonb) RETURNS boolean AS $$
    SELECT (s IS NULL AND f IS NOT NULL)
        OR (s = ('null'::jsonb) AND (f <> ('null'::jsonb) AND f <> ('[]'::jsonb)))
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION equal_case(fst jsonb, snd jsonb, f jsonb, s jsonb) RETURNS boolean AS $$
SELECT (f IS NULL
    AND s IS NULL
    AND (fst IS NOT NULL OR snd IS NOT NULL))
    OR (f = ('null'::jsonb)
    AND s IS NULL )

$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION equal_js(f jsonb, s jsonb) RETURNS boolean AS $$
    SELECT f in (NULL, ('null'::jsonb), ('[]'::jsonb))
       AND s in (NULL, ('null'::jsonb), ('[]'::jsonb))
$$ LANGUAGE SQL;

-- parse input
WITH RECURSIVE data(index, fst, snd) AS (
        SELECT a.idx,
               CASE WHEN a.packet = 1 THEN a.signal ELSE NULL END AS first,
               CASE WHEN a.packet = 2 THEN a.signal ELSE NULL END AS second
        FROM (
            SELECT p.idx,
                   ROW_NUMBER() OVER (PARTITION BY p.idx ORDER BY p.row) AS packet,
                    p.signal
            FROM (
                SELECT i.row / 3 + 1 AS idx, i.row, i.signal
                FROM input i
                WHERE i.signal IS NOT NULL
                ) AS p
            ) AS a
), clean_data (index, first, second) AS (
    SELECT b.index AS index, a.fst, b.snd
    FROM data AS a, data AS b
    WHERE a.index = b.index AND a.fst IS NOT NULL and b.snd IS NOT NULL
),
worker (level, idx, fst, snd, f, s, c) AS (
    SELECT 1, c.index, NULL::jsonb, NULL::jsonb, c.first, c.second, NULL::boolean
    FROM clean_data AS c
    WHERE jsonb_typeof(c.first) = 'number' AND jsonb_typeof(c.second) = 'number'

    UNION ALL

    SELECT 1, c.index, remove_first(c.first), remove_first(c.second), c.first -> 0, c.second -> 0, NULL::boolean
    FROM clean_data AS c
    WHERE jsonb_typeof(c.first) = 'array' AND jsonb_typeof(c.second) = 'array'

    UNION ALL

    SELECT 1, c.index, remove_first(c.first), NULL, c.first -> 0, c.second, NULL::boolean
    FROM clean_data AS c
    WHERE jsonb_typeof(c.first) = 'array' AND jsonb_typeof(c.second) = 'number'

    UNION ALL

    SELECT 1, c.index, NULL, remove_first(c.second), c.first, c.second -> 0, NULL::boolean
    FROM clean_data AS c
    WHERE jsonb_typeof(c.first) = 'number' AND jsonb_typeof(c.second) = 'array'

    UNION ALL

    (WITH w(level, idx, fst, snd, f, s, c) AS (
        SELECT * FROM worker
    )
    SELECT w.level + 1, w.idx, w.fst, w.snd, w.f, w.s, True as c
    FROM w
    WHERE w.c IS NULL
    AND (true_case(w.fst, w.snd, w.f, w.s)
    OR (w.f < w.s AND jsonb_typeof(w.f) = 'number' AND jsonb_typeof(w.s) = 'number'))

    --------
    UNION ALL

    SELECT w.level + 1, w.idx, w.fst, w.snd, w.f, w.s, False as c
    FROM w
    WHERE w.c IS NULL
    AND (false_case(w.fst, w.snd, w.f, w.s)
    OR (w.s < w.f AND jsonb_typeof(w.f) = 'number' AND jsonb_typeof(w.s) = 'number'))

    UNION ALL

    SELECT w.level + 1, w.idx, prepend(remove_first(w.f), w.fst),
           prepend(remove_first(w.s), w.snd), w.f -> 0, w.s -> 0, NULL::boolean as c
    FROM w
    WHERE w.c IS NULL
    AND jsonb_typeof(w.f) = 'array' AND jsonb_typeof(w.s) = 'array'

    UNION ALL

    SELECT w.level + 1, w.idx, prepend(remove_first(w.f), w.fst),
           prepend('null'::jsonb, w.snd), w.f -> 0, w.s, NULL::boolean as c
    FROM w
    WHERE w.c IS NULL
    AND jsonb_typeof(w.f) = 'array' AND jsonb_typeof(w.s) = 'number'

    UNION ALL

    SELECT w.level + 1, w.idx, prepend('null'::jsonb, w.fst),
           prepend(remove_first(w.s), w.snd), w.f, w.s -> 0, NULL::boolean as c
    FROM w
    WHERE w.c IS NULL
    AND jsonb_typeof(w.f) = 'number' AND jsonb_typeof(w.s) = 'array'

    UNION ALL

    SELECT w.level + 1, w.idx, remove_first(w.fst), remove_first(w.snd),
           w.fst -> 0, w.snd -> 0, NULL::boolean as c
    FROM w
    WHERE w.c IS NULL
    AND ((jsonb_typeof(w.f) = 'number' AND jsonb_typeof(w.s) = 'number' AND w.f = w.s)
        OR equal_js(w.f, w.s))
    )

)
SELECT SUM(w.idx) FROM worker AS w WHERE w.c IS NOT NULL AND w.c;
-- SELECT  d.first, d.second, w.c AS "first<second?", w.idx
-- FROM clean_data as d Join worker as w on d.index = w.idx
-- WHERE w.c IS NOT NULL
-- ORDER BY w.idx;
-- SELECT  d.first, d.second, w.*
-- FROM clean_data as d Join worker as w on d.index = w.idx
-- WHERE w.idx = 1
-- ORDER BY w.level, w.idx;
