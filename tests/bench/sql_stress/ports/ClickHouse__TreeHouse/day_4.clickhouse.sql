CREATE OR REPLACE MACRO arrayFlatten(a) AS flatten(a);
CREATE OR REPLACE MACRO arrayEnumerate(a) AS range(1, len(a) + 1);
CREATE OR REPLACE MACRO splitByChar(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO ngrams(s, n) AS string_split(s, NULL);
CREATE OR REPLACE MACRO countIf(c) AS count_if(c);
CREATE OR REPLACE MACRO input() AS '..@@.@@@@.
@@@.@.@.@@
@@@@@.@.@@
@.@@@@..@.
@@.@@@@.@@
.@@@@@@@.@
.@.@.@.@@@
@.@@@.@@@@
.@@@@@@@@.
@.@.@@@.@.';
WITH RECURSIVE 
    -- Split input into lines
input_lines AS (
    SELECT splitByChar(E'\n', input()) AS lines
),
    -- Find all '@' symbol positions in the grid
grid_points AS (
    SELECT unnest(
        list_transform(arrayFlatten(
            list_transform(
                list_transform(
                    lines,
                    (line, line_num) -> (ngrams(line, 1), line_num)
                ),
                line_tuple -> 
                    list_transform(
                        list_filter(
                            arrayEnumerate(line_tuple[1]),
                            pos -> line_tuple[1][pos] = '@'
                        ),
                        x_pos -> (x_pos, line_tuple[2])
                    )
            )
        ), t -> (t[1]::UTINYINT, t[2]::UTINYINT))
    ) AS point
    FROM input_lines
),
    -- Expand points into separate columns
initial_points AS (
    SELECT 
        point[1] AS x,
        point[2] AS y
    FROM grid_points
),
    -- Recursive CTE: Keep only points with 4+ neighbors at each iteration
recursive_convergence AS (
    -- Base case: all initial points at depth 1
    SELECT
        x,
        y,
        1 AS depth
    FROM initial_points

    UNION ALL

    -- Recursive case: keep points with at least 4 neighbors
    SELECT
        p.x,
        p.y,
        p.depth + 1 AS depth
    FROM recursive_convergence AS p
    CROSS JOIN recursive_convergence AS q
    WHERE p.depth < 256  -- Maximum recursion depth
    GROUP BY p.x, p.y, p.depth
    HAVING countIf(
        q.x BETWEEN p.x - 1 AND p.x + 1
        AND q.y BETWEEN p.y - 1 AND p.y + 1
        AND NOT (p.x = q.x AND p.y = q.y)
    ) >= 4
),
    -- Track point counts at each depth level
depth_statistics AS (
    SELECT 
        depth,
        count() AS point_count,
        lag(point_count, 1, 0) OVER (ORDER BY depth) AS previous_count
    FROM recursive_convergence
    GROUP BY depth
    ORDER BY depth
),
    -- Find the depth where the count stabilizes (no more points removed)
stabilization_analysis AS (
    SELECT 
        min(depth) AS stabilization_depth,
        arg_min(point_count, depth) AS stabilized_count
    FROM depth_statistics
    WHERE point_count = previous_count 
        AND point_count > 0
),
    -- Part 1: Points removed after first iteration (depth 2)
part1_solution AS (
    SELECT
        (SELECT count() FROM initial_points) - 
        (SELECT point_count FROM depth_statistics WHERE depth = 2 LIMIT 1) AS solution
),
    -- Part 2: Points removed when convergence stabilizes
part2_solution AS (
    SELECT
        (SELECT count() FROM initial_points) - stabilized_count AS solution
    FROM stabilization_analysis
),
    -- Combine results from both parts (necessary to prevent a bug with recursive CTE/external UNIONs)
combined_solutions as (
SELECT
    'Part 1' AS part,
    solution -- Should be 1604 with my input
FROM part1_solution

UNION ALL

SELECT
    'Part 2' AS part,
    solution -- Should be 9397 with my input
FROM part2_solution)
select * from combined_solutions