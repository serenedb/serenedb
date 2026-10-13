CREATE OR REPLACE MACRO toUInt64(x) AS CAST(x AS UBIGINT);
CREATE OR REPLACE MACRO splitByChar(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO splitByString(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO intervalLengthSum(a, b) AS list_reduce(list_sort(list([a, b])), (acc, x) -> CASE WHEN x[2] <= acc[1] THEN acc ELSE [x[2], acc[2] + x[2] - greatest(x[1], acc[1])] END, [0::UBIGINT, 0::UBIGINT])[2];
CREATE OR REPLACE MACRO input() AS '3-5
10-14
16-20
12-18

1
5
8
11
17
32';
-- Define puzzle input
WITH 
    -- Split input into two sections
input_sections AS (
    SELECT
        splitByString(E'\n\n', input())[1] AS ranges_section,
        splitByString(E'\n\n', input())[2] AS ids_section
),
    -- Parse ranges from first section (format: "min-max" per line)
parsed_ranges AS (
    SELECT list_transform(
        splitByChar(E'\n', ranges_section),
        x -> (
            toUInt64(splitByChar('-', x)[1]),
            toUInt64(splitByChar('-', x)[2]) + 1 -- Make max half-open
        )
    ) AS ranges
    FROM input_sections
),
    -- Parse IDs from second section (one ID per line)
parsed_ids AS (
    SELECT list_transform(
        splitByChar(E'\n', ids_section),
        x -> toUInt64(x)
    ) AS ids
    FROM input_sections
),
    -- Explode ranges into individual rows for Part 2 interval calculation
exploded_ranges AS (
    SELECT unnest(ranges) AS range_tuple
    FROM parsed_ranges
),
    -- Part 1: Count how many IDs fall within any range
part1_solution AS (
    SELECT
        length(
            list_filter(
                ids,
                id -> list_bool_or(list_transform(
                    ranges,
                    range -> id BETWEEN range[1] AND range[2]
                ))
            )
        ) AS solution
    FROM parsed_ranges, parsed_ids
),
    -- Part 2: Calculate total interval length (union of all ranges)
part2_solution AS (
    SELECT
        intervalLengthSum(range_tuple[1], range_tuple[2]) AS solution
    FROM exploded_ranges
)

-- Combine results from both parts
SELECT
    'Part 1' AS part,
    solution -- Should be 707 with my input
FROM part1_solution

UNION ALL

SELECT
    'Part 2' AS part,
    solution -- Should be 361615643045059 with my input
FROM part2_solution
