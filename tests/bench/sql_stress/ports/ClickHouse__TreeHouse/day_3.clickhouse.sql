CREATE OR REPLACE MACRO toUInt8(x) AS CAST(trunc(x) AS UTINYINT);
CREATE OR REPLACE MACRO intExp10(x) AS CAST(pow(10, x) AS BIGINT);
CREATE OR REPLACE MACRO arrayMax(a) AS list_max(a);
CREATE OR REPLACE MACRO splitByChar(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO arraySlice(a, o) AS a[o:], (a, o, l) AS a[o:o + l - 1];
CREATE OR REPLACE MACRO ngrams(s, n) AS string_split(s, NULL);
CREATE OR REPLACE MACRO input() AS '987654321111111
811111111111119
234234234234278
818181911112111';
CREATE OR REPLACE MACRO num_digits_needed() AS 12;
-- Define puzzle input
WITH
    -- Convert input to array of digit values for Part 2
digit_array AS (
    SELECT
        list_transform(ngrams(unnest(splitByChar(E'\n', input())), 1), x -> CAST(x AS UTINYINT)) AS digits,
        length(digits) AS total_digits
),
    -- Part 1: Find largest two-digit number from each line
part1_largest_pairs AS (
    SELECT
        ngrams(unnest(splitByChar(E'\n', input())), 1) AS chars,
        arraySlice(chars, 1, length(chars) - 1) AS chars_without_last,
        
        -- Find first max digit, then find max digit after it
        concat(
            arrayMax(chars_without_last),
            arrayMax(
                arraySlice(
                    chars,
                    coalesce(list_position(list_transform(chars, x -> x = arrayMax(chars_without_last)), true), 0) + 1
                )
            )
        )::SMALLINT AS largest_two_digit
),
    -- Part 2: Build largest N-digit number by greedily selecting max digits
part2_greedy_selection AS (
    SELECT
        digits,
        
        -- Iteratively build number by selecting maximum available digit
        list_reduce(digits, (accumulator, current_element) -> (
                -- Decrement remaining digits counter
                greatest(accumulator[1] - 1, 0)::BIGINT,
                
                -- Update position: find where max digit is in remaining slice
                accumulator[2] + (
                    coalesce(list_position(list_transform(arraySlice(
                            digits,
                            accumulator[2] + 1,
                            total_digits - accumulator[1] - accumulator[2] + 1
                        ), x -> x = arrayMax(
                            arraySlice(
                                digits,
                                accumulator[2] + 1,
                                total_digits - accumulator[1] - accumulator[2] + 1
                            )
                        )), true), 0)
                )::UBIGINT,
                
                -- Accumulate joltage: add max digit * 10^(remaining-1)
                accumulator[3] + if(
                    accumulator[1] = 0,
                    0::UBIGINT,
                    arrayMax(
                        arraySlice(
                            digits,
                            accumulator[2] + 1,
                            total_digits - accumulator[1] - accumulator[2] + 1
                        )
                    ) * intExp10(greatest(0, accumulator[1] - 1))
                )
            ), -- Initial accumulator state:
            -- (digits_remaining, current_position, accumulated_value)
            (num_digits_needed()::BIGINT, 0::UBIGINT, 0::UBIGINT))[3] AS joltage  -- Extract the accumulated value (3rd element)
    
    FROM digit_array
)

-- Combine results from both parts
SELECT
    'Part 1' AS part,
    sum(largest_two_digit)::UBIGINT AS solution -- Should be 17263 with my input
FROM part1_largest_pairs

UNION ALL

SELECT
    'Part 2' AS part,
    sum(joltage) AS solution -- Should be 170731717900423 with my input
FROM part2_greedy_selection