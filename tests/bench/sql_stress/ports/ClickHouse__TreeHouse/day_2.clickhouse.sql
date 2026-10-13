CREATE OR REPLACE MACRO toString(x) AS CAST(x AS VARCHAR);
CREATE OR REPLACE MACRO splitByChar(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO input() AS '11-22,95-115,998-1012,1188511880-1188511890,222220-222224,1698522-1698528,446443-446449,38593856-38593862,565653-565659,824824821-824824827,2121212118-2121212124';
-- Define puzzle input
WITH
    -- Parse range bounds from input string
range_bounds AS (
    SELECT list_transform(
        splitByChar('-', unnest(splitByChar(',', input()))),
        x -> x::UBIGINT
    ) AS bounds
),
    -- Expand ranges into individual numbers
expanded_numbers AS (
    SELECT
        unnest(range(bounds[1]::BIGINT, (bounds[2] + 1)::BIGINT)) AS number,
        toString(number) AS number_string,
        length(number_string) AS string_length
    FROM range_bounds
),
    -- Analyze each number for repeating patterns
repeating_analysis AS (
    SELECT
        number_string,
        CAST(number_string AS UBIGINT) AS number,
        
        -- Part 2: Check if string is made of any repeating pattern
        -- (e.g., "123123" = "123" repeated, "1111" = "1" repeated)
        (len(list_filter(
            range(1, string_length),
            x -> (string_length % x = 0)
                AND (
                    repeat(
                        substring(number_string, 1, x),
                        (string_length / x)::UINTEGER
                    ) = number_string
                )
        )) > 0)::INT AS has_pattern_repeat,
        
        -- Part 1: Check if second half equals first half
        -- (e.g., "1212" -> "12" = "12", "123123" -> "123" = "123")
        if(
            string_length % 2 = 0 
            AND substring(number_string, (string_length // 2) + 1, string_length // 2) 
                = substring(number_string, 1, string_length // 2),
            1,
            0
        ) AS has_half_repeat
    FROM expanded_numbers
    WHERE
        has_pattern_repeat != 0 
        OR has_half_repeat != 0
    ORDER BY number ASC
)

-- Calculate final solutions
SELECT 
    sum(number) FILTER (WHERE has_half_repeat = 1) AS part_1_solution, -- Should be 24043483400 with my input
    sum(number) FILTER (WHERE has_pattern_repeat = 1) AS part_2_solution -- Should be 38262920235 with my input
FROM repeating_analysis