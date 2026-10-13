CREATE OR REPLACE MACRO arrayEnumerate(a) AS range(1, len(a) + 1);
CREATE OR REPLACE MACRO arraySum(a) AS list_sum(a);
CREATE OR REPLACE MACRO splitByChar(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO has(a, x) AS list_contains(a, x);
CREATE OR REPLACE MACRO arraySlice(a, o) AS a[o:], (a, o, l) AS a[o:o + l - 1];
CREATE OR REPLACE MACRO ngrams(s, n) AS string_split(s, NULL);
CREATE OR REPLACE MACRO mapContains(m, k) AS map_contains(m, k);
CREATE OR REPLACE MACRO mapValues(m) AS map_values(m);
CREATE OR REPLACE MACRO mapUpdate(a, b) AS map_concat(a, b);
CREATE OR REPLACE MACRO mapSort(m) AS map_from_entries(list_sort(map_entries(m)));
CREATE OR REPLACE MACRO sumMap_array(maps) AS map_from_entries(list_transform(list_sort(list_distinct(list_transform(flatten(list_transform(maps, m -> map_entries(m))), e -> e.key))), k -> {'key': k, 'value': list_sum(list_transform(list_filter(flatten(list_transform(maps, m -> map_entries(m))), e -> e.key = k), e -> e.value))::UBIGINT}));
CREATE OR REPLACE MACRO input() AS '.......S.......
...............
.......^.......
...............
......^.^......
...............
.....^.^.^.....
...............
....^.^...^....
...............
...^.^...^.^...
...............
..^...^.....^..
...............
.^.^.^.^.^...^.
...............';
-- Define puzzle input 
WITH 
    -- Parse input into character grid
parsed_grid AS (
    SELECT list_transform(splitByChar(E'\n', input()), x -> ngrams(x, 1)) AS rows
),
    -- Find starting position in first row
initial_state AS (
    SELECT
        coalesce(list_position(list_transform(rows[1], x -> x = 'S'), true), 0)::UTINYINT AS start_position,
        MAP {
            coalesce(list_position(list_transform(rows[1], x -> x = 'S'), true), 0)::UTINYINT:
            1::UBIGINT
        }::MAP(UTINYINT, UBIGINT) AS initial_worlds
    FROM parsed_grid
),
    -- Filter to only rows with '^' markers (active rows)
active_rows AS (
    SELECT list_filter(rows, x -> has(x, '^')) AS filtered_rows
    FROM parsed_grid
),
    -- Main iteration: propagate world counts through rows
world_propagation AS (
    SELECT
        start_position,
        initial_worlds,
        filtered_rows,
        
        -- Fold through each row, updating state
        list_reduce(filtered_rows, (accumulator, current_row) -> (
                -- Update left boundary (shrink inward)
                (accumulator[1] - 1)::UTINYINT,
                
                -- Update right boundary (shrink inward)
                (accumulator[2] + 1)::UTINYINT,
                
                -- Update world map: propagate counts based on '^' positions
                mapSort(
                    mapUpdate(
                        accumulator[3],
                        sumMap_array(
                            list_transform(-- Only process positions within current boundaries
                                arraySlice(
                                    arrayEnumerate(current_row),
                                    accumulator[1],
                                    (accumulator[2] - accumulator[1]) + 1
                                ), position -> if(
                                    -- Check if position has '^' and exists in current worlds
                                    current_row[position] = '^'
                                    AND mapContains(accumulator[3], position),
                                    
                                    -- Propagate world count to adjacent positions
                                    MAP {
                                        -- Left neighbor gets count (unless blocked by another '^')
                                        (position - 1)::UTINYINT:
                                        (
                                            accumulator[3][position] + if(
                                                current_row[greatest(0, position - 2)] = '^',
                                                0,
                                                coalesce(accumulator[3][position - 1], 0)
                                            )
                                        )::UBIGINT,
                                        
                                        -- Current position resets to 0
                                        (position)::UTINYINT:
                                        0::UBIGINT,
                                        
                                        -- Right neighbor gets count
                                        (position + 1)::UTINYINT:
                                        (coalesce(accumulator[3][position + 1], 0) + accumulator[3][position])::UBIGINT
                                    },
                                    
                                    -- No propagation if conditions not met
                                    MAP {}::MAP(UTINYINT, UBIGINT)
                                ))
                        )
                    )
                ),
                
                -- Part 1 counter: count '^' positions with non-zero worlds
                accumulator[4] + len(list_filter(
                    arraySlice(
                        arrayEnumerate(current_row),
                        accumulator[1],
                        (accumulator[2] - accumulator[1]) + 1
                    ),
                    position -> 
                        current_row[position] = '^'
                        AND mapContains(accumulator[3], position)
                        AND accumulator[3][position] > 0
                ))
            ), -- Initial accumulator state:
            -- (left_boundary, right_boundary, worlds_map, part1_counter)
            (
                start_position,
                start_position,
                initial_worlds,
                0::UBIGINT
            )) AS final_state
    FROM initial_state, active_rows
),
    -- Part 1: Count of '^' positions encountered with non-zero worlds
part1_solution AS (
    SELECT final_state[4] AS solution
    FROM world_propagation
),
    -- Part 2: Sum of all world counts across all positions
part2_solution AS (
    SELECT arraySum(mapValues(final_state[3])) AS solution
    FROM world_propagation
)

-- Combine results from both parts
SELECT
    'Part 1' AS part,
    solution -- 1633 with my input
FROM part1_solution

UNION ALL

SELECT
    'Part 2' AS part,
    solution -- 34339203133559 with my input
FROM part2_solution
