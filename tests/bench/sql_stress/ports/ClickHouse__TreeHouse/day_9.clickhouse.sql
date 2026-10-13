CREATE OR REPLACE MACRO arrayPushBack(a, x) AS list_append(a, x);
CREATE OR REPLACE MACRO ch_format_csv_xy(s) AS TABLE SELECT CAST(f[1] AS DOUBLE) AS x, CAST(f[2] AS DOUBLE) AS y FROM (SELECT string_split(line, ',') AS f FROM unnest(string_split(s, chr(10))) AS l(line));
CREATE OR REPLACE MACRO polygonAreaCartesian(r) AS abs(list_sum(list_transform(range(1, len(r)), i -> r[i][1] * r[i + 1][2] - r[i + 1][1] * r[i][2]))) / 2;
CREATE OR REPLACE MACRO ch_point_in_ring(p, r) AS list_sum(list_transform(range(1, len(r)), i -> CASE WHEN (r[i][2] > p[2]) <> (r[i + 1][2] > p[2]) AND p[1] < (r[i + 1][1] - r[i][1]) * (p[2] - r[i][2]) / (r[i + 1][2] - r[i][2]) + r[i][1] THEN 1 ELSE 0 END)) % 2 = 1;
CREATE OR REPLACE MACRO ch_orient(a, b, c) AS (b[1] - a[1]) * (c[2] - a[2]) - (b[2] - a[2]) * (c[1] - a[1]);
CREATE OR REPLACE MACRO ch_segments_cross(a, b, c, d) AS ch_orient(a, b, c) * ch_orient(a, b, d) < 0 AND ch_orient(c, d, a) * ch_orient(c, d, b) < 0;
CREATE OR REPLACE MACRO polygonsWithinCartesian(a, b) AS CAST(list_bool_and(list_transform(a, p -> ch_point_in_ring(p, b))) AND NOT list_bool_or(flatten(list_transform(range(1, len(a)), i -> list_transform(range(1, len(b)), j -> ch_segments_cross(a[i], a[i + 1], b[j], b[j + 1]))))) AS UTINYINT);
CREATE OR REPLACE MACRO input() AS '7,1
11,1
11,7
9,7
9,5
2,5
2,3
7,3';
-- Define puzzle input 
WITH 
    -- Parse 2D coordinate points
parsed_points AS (
    SELECT *
    FROM ch_format_csv_xy(input())
),
    -- Generate all unique pairs of points
point_pairs AS (
    SELECT
        c1.x AS x1,
        c1.y AS y1,
        c2.x AS x2,
        c2.y AS y2
    FROM parsed_points AS c1
    CROSS JOIN parsed_points AS c2
    WHERE (c1.x, c1.y) < (c2.x, c2.y)
),
    -- Create bounding box polygons for each pair
bounding_boxes AS (
    SELECT
        x1,
        y1,
        x2,
        y2,
        
        -- Exact bounding box (corners at point coordinates)
        [
            (least(x1, x2), least(y1, y2)),        -- bottom-left
            (least(x1, x2), greatest(y1, y2)),     -- top-left
            (greatest(x1, x2), greatest(y1, y2)),  -- top-right
            (greatest(x1, x2), least(y1, y2)),     -- bottom-right
            (least(x1, x2), least(y1, y2))         -- close the ring
        ]::TUPLE(DOUBLE, DOUBLE)[] AS exact_bounds,
        
        -- Expanded bounding box (extends 0.5 units beyond points)
        [
            (least(x1, x2) - 0.5, least(y1, y2) - 0.5),        -- bottom-left
            (least(x1, x2) - 0.5, greatest(y1, y2) + 0.5),     -- top-left
            (greatest(x1, x2) + 0.5, greatest(y1, y2) + 0.5),  -- top-right
            (greatest(x1, x2) + 0.5, least(y1, y2) - 0.5),     -- bottom-right
            (least(x1, x2) - 0.5, least(y1, y2) - 0.5)         -- close the ring
        ]::TUPLE(DOUBLE, DOUBLE)[] AS expanded_bounds
    FROM point_pairs
),
    -- Create polygon containing all points (for Part 2 containment test)
all_points_array AS (
    SELECT list((x, y)) AS points_array
    FROM parsed_points
),
    all_points_polygon AS (
    SELECT arrayPushBack(points_array, points_array[1])::TUPLE(DOUBLE, DOUBLE)[] AS ring
    FROM all_points_array
),
    -- Part 1: Find largest bounding box by area
part1_candidates AS (
    SELECT
        x1,
        y1,
        x2,
        y2,
        exact_bounds,
        expanded_bounds,
        polygonAreaCartesian(expanded_bounds) AS area
    FROM bounding_boxes
    ORDER BY area DESC
    LIMIT 1
),
    part1_solution AS (
    SELECT area AS solution
    FROM part1_candidates
),
    -- Part 2: Find largest bounding box that contains all points
part2_candidates AS (
    SELECT
        bb.x1,
        bb.y1,
        bb.x2,
        bb.y2,
        
        -- Create slightly inset test bounds (0.01 units inside)
        (least(x1, x2) + 0.01, least(y1, y2) + 0.01) AS bottom_left,
        (least(x1, x2) + 0.01, greatest(y1, y2) - 0.01) AS top_left,
        (greatest(x1, x2) - 0.01, greatest(y1, y2) - 0.01) AS top_right,
        (greatest(x1, x2) - 0.01, least(y1, y2) + 0.01) AS bottom_right,
        
        -- Create test bounds polygon
        [
            bottom_left,
            top_left,
            top_right,
            bottom_right,
            bottom_left
        ]::TUPLE(DOUBLE, DOUBLE)[] AS test_bounds,
        
        -- Check if all points are within test bounds
        polygonsWithinCartesian(test_bounds, app.ring) AS all_points_contained,
        
        polygonAreaCartesian(bb.expanded_bounds) AS area
    FROM bounding_boxes AS bb
    CROSS JOIN all_points_polygon AS app
    WHERE all_points_contained != 0
    ORDER BY area DESC
    LIMIT 1
),
    part2_solution AS (
    SELECT area AS solution
    FROM part2_candidates
)

-- Combine results from both parts
SELECT
    'Part 1' AS part,
    solution AS area -- 4739623064 with my input
FROM part1_solution

UNION ALL

SELECT
    'Part 2' AS part,
    solution AS area -- 1654141440 with my input
FROM part2_solution
