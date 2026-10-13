CREATE OR REPLACE MACRO toUInt8(x) AS CAST(trunc(CAST(x AS DOUBLE)) AS UTINYINT);
CREATE OR REPLACE MACRO toUInt16(x) AS CAST(trunc(CAST(x AS DOUBLE)) AS USMALLINT);
CREATE OR REPLACE MACRO toUInt32(x) AS CAST(trunc(CAST(x AS DOUBLE)) AS UINTEGER);
CREATE OR REPLACE MACRO toUInt64(x) AS CAST(trunc(x) AS UBIGINT);
CREATE OR REPLACE MACRO intDiv(a, b) AS a // b;
CREATE OR REPLACE MACRO splitByChar(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO replaceRegexpAll(s, re, r) AS regexp_replace(s, re, r, 'g');
CREATE OR REPLACE MACRO has(a, x) AS list_contains(a, x);
CREATE OR REPLACE MACRO arraySlice(a, o) AS a[o:], (a, o, l) AS a[o:o + l - 1];
CREATE OR REPLACE MACRO ngrams(s, n) AS string_split(s, NULL);
CREATE OR REPLACE MACRO bitCount(x) AS bit_count(x);
CREATE OR REPLACE MACRO bitTest(a, i) AS ((a >> i) & 1) = 1;
CREATE OR REPLACE MACRO modulo(a, b) AS a % b;
CREATE OR REPLACE MACRO ch_format_lines(s) AS TABLE SELECT line AS raw FROM unnest(string_split(s, chr(10))) AS l(line);
CREATE OR REPLACE MACRO input() AS '[.##.] (3) (1,3) (2) (2,3) (0,2) (0,1) {3,5,4,7}
[...#.] (0,2,3,4) (2,3) (0,4) (0,1,2) (1,2,3,4) {7,5,12,7,2}
[.###.#] (0,1,2,3,4) (0,3,4) (0,1,2,4,5) (1,2) {10,11,11,5,10,5}';
WITH RECURSIVE 
    -- Parse raw input into structured format
raw_split AS (
    SELECT
        row_number() OVER () AS puzzle_id,
        splitByChar(' ', raw) AS components
    FROM ch_format_lines(input())
),
    parsed_puzzles AS (
    SELECT
        puzzle_id,
        
        -- Parse diagram: '#' becomes 1, '.' becomes 0
        list_transform(ngrams(replaceRegexpAll(components[1], '[\[\]]', ''), 1), x -> if(x = '#', 1, 0)) AS target_diagram,
        
        -- Parse buttons: each button affects specific positions
        list_transform(arraySlice(components, 2, length(components) - 2), button_str -> list_transform(splitByChar(',', replaceRegexpAll(button_str, '[\(\)]', '')), pos_str -> (toUInt16(pos_str) + 1))) AS button_effects,
        
        -- Parse joltages: target values for Part 2
        list_transform(splitByChar(',', replaceRegexpAll(components[-1], '[\{\}]', '')), x -> toUInt32(x)) AS target_joltages
    FROM raw_split
),
    puzzle_metadata AS (
    SELECT
        puzzle_id,
        target_diagram,
        button_effects,
        target_joltages,
        length(button_effects) AS num_buttons,
        length(target_joltages) AS num_positions
    FROM parsed_puzzles
),
    -- PART 1: Brute force - enumerate all button combinations
part1_button_combinations AS (
    SELECT
        p.puzzle_id,
        p.target_diagram,
        p.button_effects,
        p.num_buttons,
        p.num_positions,
        combination_id,
        toUInt32(bitCount(combination_id)) AS button_presses,
        
        -- Calculate resulting diagram from this combination
        list_transform(range(1, num_positions + 1), position -> toUInt8(
                modulo(
                    list_aggregate(list_transform(range(0, num_buttons), button_index -> if(
                                bitTest(combination_id, button_index)
                                AND has(button_effects[button_index + 1], position),
                                1,
                                0
                            )), 'sum'),
                    2
                )
            )) AS resulting_diagram
    FROM puzzle_metadata p
    CROSS JOIN unnest(range(0, toUInt32(pow(2, num_buttons)))) AS aj(combination_id)
),
    part1_minimum_solutions AS (
    SELECT
        puzzle_id,
        min(button_presses) AS minimum_presses
    FROM part1_button_combinations
    WHERE target_diagram = resulting_diagram
    GROUP BY puzzle_id
),
    -- PART 2: Pre-compute button combination patterns for recursive algorithm
button_combination_patterns AS (
    SELECT
        p.puzzle_id,
        p.button_effects,
        p.num_buttons,
        p.num_positions,
        combination_id,
        toUInt32(bitCount(combination_id)) AS pattern_cost,
        
        -- Pattern: numeric effect on each position
        list_transform(range(1, num_positions + 1), position -> toUInt32(
                list_aggregate(list_transform(range(0, num_buttons), button_index -> if(
                            bitTest(combination_id, button_index)
                            AND has(button_effects[button_index + 1], position),
                            1,
                            0
                        )), 'sum')
            )) AS effect_pattern,
        
        -- Parity pattern: XOR constraint (mod 2)
        list_transform(range(1, num_positions + 1), position -> toUInt8(
                modulo(
                    list_aggregate(list_transform(range(0, num_buttons), button_index -> if(
                                bitTest(combination_id, button_index)
                                AND has(button_effects[button_index + 1], position),
                                1,
                                0
                            )), 'sum'),
                    2
                )
            )) AS parity_pattern
    FROM puzzle_metadata p
    CROSS JOIN unnest(range(0, toUInt32(pow(2, num_buttons)))) AS aj(combination_id)
),
    -- Group patterns by parity for efficient lookup
patterns_grouped_by_parity AS (
    SELECT
        puzzle_id,
        button_effects,
        num_buttons,
        num_positions,
        parity_pattern,
        list(row(effect_pattern, pattern_cost)) AS available_patterns
    FROM button_combination_patterns
    GROUP BY puzzle_id, button_effects, num_buttons, num_positions, parity_pattern
),
    -- Recursive halving algorithm: iteratively reduce joltages to zero
recursive_halving_solver AS (
    -- Base case: start with target joltages
    SELECT
        puzzle_id,
        button_effects,
        num_buttons,
        num_positions,
        target_joltages AS current_goal,
        toUInt64(0) AS accumulated_cost,
        0 AS recursion_depth
    FROM puzzle_metadata
    
    UNION ALL
    
    -- Recursive case: apply pattern, subtract, halve, and continue
    SELECT
        puzzle_id,
        button_effects,
        num_buttons,
        num_positions,
        current_goal,
        min(accumulated_cost) AS accumulated_cost,
        min(recursion_depth) AS recursion_depth
    FROM (
        SELECT
            solver.puzzle_id,
            solver.button_effects,
            solver.num_buttons,
            solver.num_positions,
            
            -- New goal: (current - pattern) / 2
            list_transform(range(1, solver.num_positions + 1), i -> intDiv(
                    solver.current_goal[i] - pattern_tuple[1][i],
                    2
                )) AS current_goal,
            
            -- Accumulate cost: pattern_cost * 2^depth
            solver.accumulated_cost + 
                toUInt64(pattern_tuple[2]) * toUInt64(pow(2, solver.recursion_depth)) AS accumulated_cost,
            
            solver.recursion_depth + 1 AS recursion_depth
        FROM recursive_halving_solver solver
        INNER JOIN patterns_grouped_by_parity patterns
            ON patterns.puzzle_id = solver.puzzle_id
            AND patterns.parity_pattern = list_transform(solver.current_goal, x -> if(x % 2 = 0, toUInt8(0), toUInt8(1)))
        CROSS JOIN unnest(patterns.available_patterns) AS aj(pattern_tuple)
        WHERE
            solver.recursion_depth < 100
            AND NOT list_bool_and(list_transform(solver.current_goal, x -> x = 0))
            -- Ensure pattern doesn't overshoot (feasibility constraint)
            AND list_bool_and(list_transform(range(1, solver.num_positions + 1), i -> pattern_tuple[1][i] <= solver.current_goal[i]))
    )
    GROUP BY puzzle_id, button_effects, num_buttons, num_positions, current_goal
),
    part2_minimum_solutions AS (
    SELECT
        puzzle_id,
        min(accumulated_cost) AS minimum_cost
    FROM recursive_halving_solver
    WHERE list_bool_and(list_transform(current_goal, x -> x = 0))
    GROUP BY puzzle_id
),
    -- Aggregate final solutions
combined_solutions AS (
    SELECT 'Part 1' AS part, sum(minimum_presses) AS solution -- 527 with my input
    FROM part1_minimum_solutions
    
    UNION ALL

    SELECT 'Part 2' AS part, sum(minimum_cost) AS solution -- 19810 with my input
    FROM part2_minimum_solutions
)

-- Combine results from both parts
SELECT * FROM combined_solutions;