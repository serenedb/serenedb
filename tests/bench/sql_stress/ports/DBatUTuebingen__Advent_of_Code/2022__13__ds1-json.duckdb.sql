SET integer_division = true;
-- INSTALL json;
LOAD json;

CREATE OR REPLACE MACRO remove_first(jl) AS
    CASE WHEN json_type(jl) = 'UBIGINT' THEN NULL::json
         WHEN json_type(jl) = 'ARRAY' AND json_array_length(jl) = 0 THEN NULL::json
        --  WHEN json_type(jl) = 'ARRAY' AND json_array_length(jl) = 1 AND json_type(jl->>0) = 'UBIGINT' THEN NULL::json END;
        --  WHEN json_type(jl) = 'ARRAY' AND json_array_length(jl) > 1 THEN jl->range(1, CAST(json_array_length(jl) AS BIGINT)) -- todo: this doesnt work
         WHEN json_type(jl) = 'ARRAY' AND json_array_length(jl) > 1 THEN jl->range(1, 4)
        --  WHEN json_type(jb) = 'ARRAY' AND json_array_length(jb) = 1 AND json_type(jl->>0) = 'ARRAY' THEN jl->range(1, CAST(json_array_length(jl) AS BIGINT))
        WHEN json_type(jl) = 'ARRAY' AND json_array_length(jl) = 1 THEN NULL::json -- todo: bei postgres scheint dieser Fall nicht aufzutreten...
    END;


CREATE OR REPLACE MACRO true_case(fst, snd, f, s) AS
    f IS NULL
       AND (s IS NOT NULL OR (fst IS NULL AND snd IS NULL))
        OR (f = ('null'::json)
            AND ((s <> ('null'::json) AND s <> (json_array()))
              OR (fst IS NULL AND snd IS NULL)));

CREATE OR REPLACE MACRO false_case(fst, snd, f, s) AS (
    SELECT (s IS NULL AND f IS NOT NULL)
        OR (s = ('null'::json) AND (f <> ('null'::json) AND f <> (json_array())))
);

-- read csv
WITH RECURSIVE input(row, signal) AS (
     SELECT ROW_NUMBER() OVER () AS row, s AS signal
     FROM read_csv('2022/13/input-sample.txt',
                   delim='|', header=False, columns={'signal': 'json'}) AS c(s)
),
-- parse input
data(index, first, second) AS (
    SELECT b.idx AS index, ANY_VALUE(b.first) AS first, ANY_VALUE(b.second) AS second
    FROM (
        SELECT i.row // 3 + 1 AS idx,
                ROW_NUMBER() OVER (PARTITION BY idx ORDER BY i.row) AS packet,
                CASE WHEN packet = 1 THEN i.signal ELSE NULL END AS first,
                CASE WHEN packet = 2 THEN i.signal ELSE NULL END AS second
        FROM input i
        WHERE i.signal IS NOT NULL
        ) AS b
    GROUP BY b.idx
),
-- SELECT d.first, d.second, d.first <= d.second, d.first ->>[0], d.second ->>[0], json_type(d.first), json_type(d.second), json_array_length(d.first)
-- FROM data d
-- WHERE json_type(d.first) == json_type(d.second);

-- true WHERE json_type(d.first) == 'ARRAY' AND json_array_length(d.first) == 0
-- comp WHERE json_type(d.first) == 'UBIGINT' AND json_type(d.second) == 'UBIGINT'

---------------------------- works until here ----------------------------

worker(level, idx, fst, snd, f, s, c) AS (
    SELECT 1, d.index, NULL::json, NULL::json, d.first, d.second, NULL::boolean
    FROM data AS d
    WHERE json_type(d.first) = 'UBIGINT' AND json_type(d.second) = 'UBIGINT'

    UNION ALL

    SELECT 1, d.index, remove_first(d.first), remove_first(d.second), d.first ->> 0, d.second ->> 0, NULL::boolean
    FROM data AS d
    WHERE json_type(d.first) = 'ARRAY' AND json_type(d.second) = 'ARRAY'

    UNION ALL

    SELECT 1, d.index, remove_first(d.first), NULL, d.first ->> 0, d.second, NULL::boolean
    FROM data AS d
    WHERE json_type(d.first) = 'ARRAY' AND json_type(d.second) = 'UBIGINT'

    UNION ALL

    SELECT 1, d.index, NULL, remove_first(d.second), d.first, d.second ->> 0, NULL::boolean
    FROM data AS d
    WHERE json_type(d.first) = 'UBIGINT' AND json_type(d.second) = 'ARRAY'

    UNION ALL

    (
    SELECT w.level + 1, w.idx, w.fst, w.snd, w.f, w.s, True as c
    FROM worker AS w
    WHERE w.c IS NULL
    AND (true_case(w.fst, w.snd, w.f, w.s)
     OR (w.f < w.s AND json_type(w.f) = 'UBIGINT' AND json_type(w.s) = 'UBIGINT'))

    UNION ALL

    SELECT w.level + 1, w.idx, w.fst, w.snd, w.f, w.s, False as c
    FROM worker AS w
    WHERE w.c IS NULL
    AND (false_case(w.fst, w.snd, w.f, w.s)
    OR (w.s < w.f AND json_type(w.f) = 'UBIGINT' AND json_type(w.s) = 'UBIGINT'))
    )
)
-- SELECT w.*, d.first, d.second FROM worker w, data d WHERE w.idx = d.index ORDER BY w.level, w.idx;
-- issue fehlermeldung bei Rec CTEs!

table data;
-- json Vergleiche sind ziemlich kaputt -> diese base cases sollte nicht so kompliziert sein
-- w.first = to_json([]) OR (w.first <= w.second AND w.second <> to_json([]))     WHERE json_array_length(w.first) = 0 and json_array_length(w.second) = 0
-- w.first = to_json([]) OR (w.first <= json_extract(w.second, '$[0]'))           WHERE json_array_length(w.first) = 0 and json_array_length(w.second) <> 0 AND json_array_length(json_extract(w.second, '$[0]')) = 0

-- first experiments!
    -- worker (level, first, second, wf, sf, c) AS (
    --     SELECT 1 as level, d.first, d.second, d.first as wf, d.second as sf, NULL
    --     FROM data d

    --     UNION ALL

    --     SELECT w.level + 1, w.first, w.second, w.wf, w.sf, FALSE
    --     FROM worker w
    --     WHERE json_array_length(w.first) = 1 AND  json_array_length(w.second) = 0 AND w.second = to_json([])
    --     AND w.level < 2
    -- )
    -- SELECT w.level, w.first, w.second, w.c FROM worker as w;

-- SELECT d.first, d.second, d.first <= d.second
-- FROM data d;
-- WHERE d.index = 6;

-- SELECT d.index, json_extract(d.first, '$[0]') as f, json_extract(d.second, '$[0]') as g, f<=g as 'f<=g?',
-- json_array_length(f) AS 'len(f)', json_array_length(g) AS 'len(g)'
-- FROM data d;