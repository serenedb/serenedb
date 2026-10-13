SET integer_division = true;

-- FIXME: this is hardcoded: start sequence at some number > input length
CREATE SEQUENCE serial START 1000;

-- compare two numbers
CREATE MACRO comp(fst, snd) AS (
SELECT -1 WHERE fst < snd
UNION ALL
SELECT 0 WHERE fst == snd
UNION ALL
SELECT 1 WHERE fst > snd
);

-- read csv
WITH RECURSIVE input(row, signal) AS (
     SELECT ROW_NUMBER() OVER () AS row, s AS signal
     FROM read_csv('2022/13/input.txt',
                   delim='|', header=False, columns={'signal': 'char[]'}) AS c(s)
),
-- parse input into sensible schema
data(idx, fst, snd) AS (
    SELECT b.idx AS idx, ANY_VALUE(b.fst) AS fst, ANY_VALUE(b.snd) AS snd
    FROM (
        SELECT i.row // 3 + 1 AS idx,
                ROW_NUMBER() OVER (PARTITION BY idx ORDER BY i.row) AS packet,
                CASE WHEN packet = 1 THEN i.signal ELSE NULL END AS fst,
                CASE WHEN packet = 2 THEN i.signal ELSE NULL END AS snd
        FROM input i
        WHERE i.signal IS NOT NULL
        ) AS b
    GROUP BY b.idx
),
-- compares the corresponding lists and integers
-- this is a very unefficient implementation: it compares all corresponding items (breadth first search)
-- without cutting of early (because actually a depth first search is required)
worker(i, idx, parent, id, ids, fst, snd, rec, res) AS (
    SELECT 1, idx, idx, nextval('serial') as id, [idx] as ids, unnest(fst), unnest(snd), true AS rec, NULL::Int
    FROM data

    UNION ALL
    (
    -- both are numbers: compare the numbers
    SELECT * REPLACE(i+1 AS i, false AS rec, comp(fst::Int, snd::Int) AS res)
    FROM worker
    WHERE rec AND NOT fst ^@ '[' AND NOT snd ^@ '['

    UNION ALL

    -- fst is number, snd is list: convert the number into a list
    SELECT * REPLACE(i+1 AS i, '[' || fst || ']' AS fst)
    FROM worker
    WHERE rec AND NOT fst ^@ '[' AND snd ^@ '['

    UNION ALL

    -- fst is number, snd is list: convert the number into a list
    SELECT * REPLACE(i+1 AS i, '[' || snd || ']' AS snd)
    FROM worker
    WHERE rec AND fst ^@ '[' AND NOT snd ^@ '['

    UNION ALL

    -- both are lists: recurse with further unnesting
    SELECT * REPLACE(i+1 AS i,
                     id AS parent,                 -- remembering the tree structure - maybe we need
                     list_prepend(id, ids) AS ids, -- more sophisticated info to make tree traversal
                                                   -- in the collector easier
                     nextval('serial') AS id,
                     unnest(fst::char[]) AS fst,   -- unnest both lists and compare those
                     unnest(snd::char[]) AS snd
    )
    FROM worker
    WHERE rec AND fst ^@ '[' AND snd ^@ '['

    UNION ALL

    -- fst is null (right list is shorter---aka fst<snd)
    SELECT * REPLACE(i+1 AS i, false AS rec, -1 AS res)
    FROM worker
    WHERE rec AND fst IS NULL AND snd IS NOT NULL

    UNION ALL

    -- snd is null (left list is shorter---aka fst>snd)
    SELECT * REPLACE(i+1 AS i, false AS rec, 1 AS res)
    FROM worker
    WHERE rec AND fst IS NOT NULL AND snd IS NULL
    )
),
-- since the worker did a bfs not caring about when to cut off, the collector now needs to traverse
-- the results of the worker in dfs manner to obtain the correct result per index/list comparison:
-- node=0: continue dfs, node=1: overall result=false, node=-1: overall result=true
collector(i, idx, parent, id, ids, fst, snd, rec, res, finished, last_iter) AS (
    -- copy the result rows of the worker + reset (of i and rec) + two more booleans for the logic
    SELECT * REPLACE(1 AS i, true AS rec), false AS finished, false AS last_iter
    FROM worker
    WHERE NOT rec

    UNION ALL

    (WITH last_iteration AS (
        SELECT * FROM collector WHERE last_iter
    ),
    -- identify next row to collect in dfs manner
    rank1 AS (
        -- collect result rows following the tree structure via dfs (aka leftmost deepest child first)
        -- we're always searching the leftmost deepest child as we simply "forget" all rows concerning
        -- the subtrees to the left that we've already seen
        (WITH RECURSIVE
        -- following the tree structure down to siblings/children
        to_sibling_or_children AS (
            SELECT c.*
            FROM collector c, last_iteration l
            WHERE c.rec AND c.idx = l.idx AND (list_contains(c.ids, l.id+1) OR l.id + 1 = c.id)
        ), idxs(idx) AS (
            SELECT idx FROM last_iteration WHERE idx NOT in (SELECT idx FROM to_sibling_or_children)
        ),
        -- following the tree structure up (because the current subtree is explored)
        to_parent(x) AS (
            SELECT 0::int AS x, NULL::int AS i, idx AS idx, NULL::int AS parent, NULL::int AS id,
                   NULL::int[] AS ids, NULL::varchar AS fst, NULL::varchar AS snd, NULL::boolean AS rec,
                   NULL::int AS res, NULL::boolean AS finished, NULL::boolean AS last_iter
            FROM idxs

            UNION ALL

            (
            -- this ominous row is just there to keep looping until a result is found
            SELECT x+1, NULL, p.idx, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM to_parent p, last_iteration l
            WHERE p.idx = l.idx AND l.ids[x+1] IS NOT NULL

            UNION ALL

            SELECT x+1, c.*
            FROM collector c, last_iteration l, to_parent p
            WHERE c.rec AND c.idx = l.idx AND c.idx = p.idx
            AND (l.ids[x+1]+1 = c.id OR list_contains(c.ids, l.ids[x+1]+1))
            -- go up just to next node, not second next as well
            AND NOT c.idx IN (SELECT idx FROM to_parent WHERE i IS NOT NULL)
            )
        )
        -- when finding the correct subtree to follow:
        -- (a) on first iteration: root
        -- (b) if the subtree is not explored: direct children or siblings/nieces/nephews
        -- (c) if the subtree is explored: search ancestors for next subtree
        -- pick the leftmost deepest child (aka the one with the min id (of direkt children, not overall!!!))
        , leftmost_child AS (
            -- collect result rows that are leftmost deepest child on first iteration
            SELECT -1 AS x, false AS fin, c.*
            FROM collector c
            WHERE (SELECT COUNT(*) FROM last_iteration) = 0

            UNION ALL

            SELECT -1 AS x, false AS fin, tp.* EXCLUDE(x)
            FROM to_parent tp
            WHERE i IS NOT NULL

            UNION ALL

            SELECT -1 AS x, false AS fin, tsc.*
            FROM to_sibling_or_children tsc

            UNION ALL

            (WITH minIds(minId, idx) AS (
                SELECT MIN(id), idx
                FROM leftmost_child
                WHERE NOT fin AND ids[x-1] IS NULL
                GROUP BY idx

                UNION ALL

                SELECT MIN(ids[x-1]), idx
                FROM leftmost_child
                WHERE NOT fin
                GROUP BY idx
            ), m(minId, idx) AS (
                SELECT MIN(minId), idx
                FROM minIds
                GROUP BY idx
            )
            SELECT lc.* REPLACE(x-1 AS x)
            FROM leftmost_child lc, m
            WHERE lc.idx = m.idx AND list_contains(lc.ids, m.minId)

            UNION ALL

            SELECT lc.* REPLACE(x-1 AS x, true AS fin)
            FROM leftmost_child lc, m
            WHERE lc.idx = m.idx AND lc.id = m.minId
            )
        )
        SELECT * EXCLUDE(x, fin)
        FROM leftmost_child
        WHERE fin
        )
    ),
    -- result indicates recursion
    -- we remember these rows with the flag last_iter=True to know our current position in the tree.
    to_recurse AS (
        SELECT * REPLACE (false AS rec, true AS last_iter) FROM rank1 WHERE res = 0
    ),
    -- rows to finish: rows where we find -1/1 as a result. We can cut off computation here.
    to_finish AS (
        SELECT * REPLACE(false AS rec, true AS res)
        FROM rank1
        WHERE res = -1

        UNION ALL

        SELECT * REPLACE(false AS rec, false AS res)
        FROM rank1
        WHERE res = 1
    ),
    -- copy the finished rows to keep track of what we've seen and know now
    -- this might be a point where we can optimise and use the finished flag more efficiently
    -- atm all finished comparisons are copied each iteration until the last comparison finishes
    copy_finished AS (
        SELECT * FROM collector WHERE NOT rec AND NOT finished AND NOT last_iter AND (SELECT COUNT(*) FROM rank1) > 0
        UNION  ALL
        SELECT * REPLACE(true AS finished) FROM collector WHERE NOT rec AND NOT finished AND (SELECT COUNT(*) FROM rank1) = 0
    ),
    -- copy the worker results we need for further recursion, don't copy our current rows (in rank1)
    -- and all rows relating to our now finished comparisons (early cut-off)
    to_copy AS (
        SELECT * FROM collector
        WHERE rec AND id NOT IN (SELECT id FROM rank1) AND idx NOT IN (SELECT idx FROM to_finish)
    )
    -- collect all the results/copy rows etc
    SELECT * REPLACE(i+1 AS i) FROM to_recurse
    UNION ALL
    SELECT * REPLACE(i+1 AS i) FROM to_finish
    UNION ALL
    SELECT * REPLACE(i+1 AS i) FROM copy_finished
    UNION ALL
    SELECT * REPLACE(i+1 AS i) FROM to_copy
    )
)
SELECT SUM(idx) AS result, list_sort(list(idx)) AS indices
FROM collector
WHERE finished and res;
