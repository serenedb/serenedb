WITH map AS (
    SELECT
        string_split_regex(column0, '') AS arr,
        row_number() OVER () AS y
    FROM read_csv_auto('04/example.csv', header = false, delim = '')
),
expanded_map AS (
    SELECT
        y,
        unnest(arr) as cell,
        generate_subscripts(arr, 1) AS x
    FROM map
),
directions AS (
    SELECT 1 dx, 0 dy UNION ALL
    SELECT -1, 0 UNION ALL
    SELECT 0, 1 UNION ALL
    SELECT 0, -1 UNION ALL
    SELECT 1, 1 UNION ALL
    SELECT 1, -1 UNION ALL
    SELECT -1, 1 UNION ALL
    SELECT -1, -1
),
nodes AS (
  SELECT x, y, cell
  FROM expanded_map
  WHERE cell = '@'
),
joined_directions AS (
    SELECT
        em.x,
        em.y,
        em.cell AS cell,
        m.x AS nx,
        m.y AS ny,
        m.cell AS neighbor
    FROM nodes em
    CROSS JOIN directions o
    JOIN nodes m
    ON m.x = em.x + o.dx
   AND m.y = em.y + o.dy
),
combinations AS (
    SELECT
        x,
        y,
        SUM(CASE WHEN neighbor = '@' THEN 1 ELSE 0 END) AS at_neighbor_count,
    FROM joined_directions
    GROUP BY x, y
),
remove_rolls AS (
    WITH RECURSIVE recurse_remove (
        x,
        y,
        cell,
        RecursionStep,
        count,
        changed
    ) AS (
        -- Base case
        SELECT
            n.x,
            n.y,
            CASE WHEN if(at_neighbor_count IS null or at_neighbor_count < 4, TRUE, FALSE) THEN 'X' ELSE n.cell END AS cell,
            1 AS RecursionStep,
            at_neighbor_count as count,
            CASE WHEN at_neighbor_count < 4 THEN 1 ELSE 0 END AS changed
        FROM nodes n
        LEFT JOIN combinations c ON c.x = n.x and c.y = n.y

        UNION ALL

        SELECT
            r.x,
            r.y,
            CASE WHEN new_count < 4 THEN 'X' ELSE r.cell END AS cell,
            RecursionStep + 1,
            new_count as count,
            CASE WHEN new_cell != r.cell THEN 1 ELSE 0 END AS changed
        FROM (
            SELECT
                cur.x,
                cur.y,
                cur.cell,
                SUM(CASE WHEN nbr.cell = '@' THEN 1 ELSE 0 END) AS new_count,
                cur.RecursionStep,
                CASE WHEN SUM(CASE WHEN nbr.cell = '@' THEN 1 ELSE 0 END) < 4
                     THEN 'X' ELSE cur.cell END AS new_cell
            FROM recurse_remove cur
            JOIN directions d ON TRUE
            JOIN recurse_remove nbr
                ON nbr.x = cur.x + d.dx
               AND nbr.y = cur.y + d.dy
               AND nbr.RecursionStep = cur.RecursionStep
             WHERE cur.RecursionStep = (
                 SELECT MAX(RecursionStep) FROM recurse_remove
            )
            GROUP BY cur.x, cur.y, cur.cell, cur.RecursionStep
        ) AS r
         WHERE EXISTS (
            SELECT 1
            FROM recurse_remove r2
            WHERE r2.RecursionStep = r.RecursionStep AND r2.changed = 1
        )
    )
    SELECT * FROM recurse_remove
),
final_removed AS (
    SELECT
        x,
        y,
        MIN(RecursionStep) AS step_removed
    FROM remove_rolls
    WHERE cell = 'X'
    GROUP BY x, y
),
part1 AS (
    SELECT COUNT(*) FROM combinations WHERE at_neighbor_count < 4
)
SELECT count(*) FROM final_removed
