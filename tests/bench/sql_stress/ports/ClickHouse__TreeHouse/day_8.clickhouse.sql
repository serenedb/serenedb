CREATE OR REPLACE MACRO arrayProduct(a) AS list_product(a);
CREATE OR REPLACE MACRO arrayReverseSort(a) AS list_reverse_sort(a);
CREATE OR REPLACE MACRO arrayDistinct(a) AS list_distinct(a);
CREATE OR REPLACE MACRO arrayFlatten(a) AS flatten(a);
CREATE OR REPLACE MACRO arrayConcat(a, b) AS list_concat(a, b);
CREATE OR REPLACE MACRO hasAny(a, b) AS list_has_any(a, b);
CREATE OR REPLACE MACRO arraySlice(a, o) AS a[o:], (a, o, l) AS a[o:o + l - 1];
CREATE OR REPLACE MACRO ch_format_csv_xyz(s) AS TABLE SELECT CAST(f[1] AS UINTEGER) AS x, CAST(f[2] AS UINTEGER) AS y, CAST(f[3] AS UINTEGER) AS z FROM (SELECT string_split(line, ',') AS f FROM unnest(string_split(s, chr(10))) AS l(line));
CREATE OR REPLACE MACRO L2Distance(a, b) AS list_distance(a, b);
CREATE OR REPLACE MACRO input() AS '162,817,812
57,618,57
906,360,560
592,479,940
352,342,300
466,668,158
542,29,236
431,825,988
739,650,466
52,470,668
216,146,977
819,987,18
117,168,530
805,96,715
346,949,466
970,615,88
941,993,340
862,61,35
984,92,344
425,690,689';
-- Define puzzle input 
WITH 
    -- Parse 3D coordinate points
parsed_points AS (
    SELECT (x, y, z) AS point
    FROM ch_format_csv_xyz(input())
),
    -- Generate all point pairs with L2 distances, sorted by distance
point_pairs_by_distance AS (
    SELECT
        t1.point AS point1,
        t2.point AS point2,
        L2Distance(
            [point1[1], point1[2], point1[3]],
            [point2[1], point2[2], point2[3]]
        ) AS distance
    FROM parsed_points AS t1
    CROSS JOIN parsed_points AS t2
    WHERE point1 < point2
    ORDER BY distance ASC
),
    -- Take the 1000 closest pairs
closest_pairs AS (
    SELECT list([point1, point2]) AS pairs
    FROM (
        SELECT point1, point2
        FROM point_pairs_by_distance
        ORDER BY distance ASC
        LIMIT 1000
    )
),
    -- Part 1: Build connected components from closest pairs
connected_components AS (
    SELECT
        pairs,
        
        -- Fold through pairs to merge into connected components
        list_reduce(pairs, (accumulator, pair) -> if(
                -- Check if any existing components contain points from current pair
                length(
                    list_filter(accumulator, component -> hasAny(component, pair))
                ) > 0,
                
                -- Merge matching components with current pair
                arrayConcat(
                    -- Keep non-matching components unchanged
                    list_filter(accumulator, component -> NOT hasAny(component, pair)),
                    -- Add merged component
                    [
                        arrayDistinct(
                            arrayFlatten(
                                arrayConcat(
                                    list_filter(accumulator, component -> hasAny(component, pair)),
                                    [pair]
                                )
                            )
                        )
                    ]
                ),
                
                -- No matches found, add pair as new component
                arrayConcat(accumulator, [pair])
            ), []::TUPLE(UINTEGER, UINTEGER, UINTEGER)[][]) AS components
    FROM closest_pairs
),
    component_analysis AS (
    SELECT
        components,
        list_transform(components, x -> length(x)) AS component_sizes
    FROM connected_components
),
    part1_solution AS (
    SELECT arrayProduct(
        arraySlice(
            arrayReverseSort(component_sizes),
            1,
            3
        )
    ) AS solution
    FROM component_analysis
),
    -- Part 2: Find first pair where 1000 unique points have been seen
point_pair_states AS (
    SELECT
        point1,
        point2,
        distance,
        [point1, point2] AS points_state
    FROM point_pairs_by_distance
),
    part2_solution AS (
    SELECT
        point1,
        point2,
        distance,
        len(list_distinct(flatten(list(points_state) OVER (ORDER BY distance ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)))) AS unique_points_seen,
        point1[1] * point2[1] AS solution
    FROM point_pair_states
    QUALIFY unique_points_seen >= 1000
    ORDER BY distance ASC
    LIMIT 1
)

-- Combine results from both parts
SELECT
    'Part 1' AS part,
    solution::UBIGINT -- 135169920 with my input
FROM part1_solution

UNION ALL

SELECT
    'Part 2' AS part,
    solution::UBIGINT -- 302133440 with my input
FROM part2_solution

