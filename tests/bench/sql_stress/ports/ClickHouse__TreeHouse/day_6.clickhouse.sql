CREATE OR REPLACE MACRO toInt64(x) AS CAST(trunc(CAST(x AS DOUBLE)) AS BIGINT);
CREATE OR REPLACE MACRO arrayProduct(a) AS list_product(a);
CREATE OR REPLACE MACRO arrayFlatten(a) AS flatten(a);
CREATE OR REPLACE MACRO arrayStringConcat(a) AS array_to_string(a, ''), (a, sep) AS array_to_string(a, sep);
CREATE OR REPLACE MACRO arraySum(a) AS list_sum(a);
CREATE OR REPLACE MACRO splitByChar(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO splitByWhitespace(s) AS list_filter(regexp_split_to_array(s, '\s+'), x -> x <> '');
CREATE OR REPLACE MACRO replaceAll(s, a, b) AS replace(s, a, b);
CREATE OR REPLACE MACRO has(a, x) AS list_contains(a, x);
CREATE OR REPLACE MACRO hasAny(a, b) AS list_has_any(a, b);
CREATE OR REPLACE MACRO arraySlice(a, o) AS a[o:], (a, o, l) AS a[o:o + l - 1];
CREATE OR REPLACE MACRO ngrams(s, n) AS string_split(s, NULL);
CREATE OR REPLACE MACRO arraySplit_at(starts, a) AS list_transform(range(1, len(starts) + 1), k -> a[starts[k]:coalesce(starts[k + 1] - 1, len(a))]);
CREATE OR REPLACE MACRO arraySplit(flags, a) AS arraySplit_at(list_filter(range(1, len(a) + 1), i -> i = 1 OR flags[i]), a);
CREATE OR REPLACE MACRO input() AS '123 328  51 64 
 45 64  387 23 
  6 98  215 314
*   +   *   +  ';
-- Define puzzle input
WITH 
    -- Part 1: Parse input into columns and apply operations
part1_parsed_rows AS (
    SELECT list_transform(splitByChar(E'\n', input()), x -> splitByWhitespace(x)) AS rows
),
    part1_columns AS (
    SELECT list_transform(range(1, length(rows[1]) + 1), column_index -> list_transform(rows, row -> row[column_index])) AS columns
    FROM part1_parsed_rows
),
    part1_solution AS (
    SELECT arraySum(
        list_transform(columns, "column" -> if(
                -- Check if last element is multiplication operator
                list_filter("column", x -> 1)[-1] = '*',
                
                -- Multiply all numbers in column
                toInt64(arrayProduct(
                    list_transform(arraySlice("column", 1, length("column") - 1), x -> toInt64(x))
                )),
                
                -- Otherwise, add all numbers in column
                toInt64(arraySum(
                    list_transform(arraySlice("column", 1, length("column") - 1), x -> toInt64(x))
                ))
            ))
    ) AS solution
    FROM part1_columns
),
    -- Part 2: Parse with character-level precision to handle multi-digit numbers
part2_parsed_chars AS (
    SELECT list_transform(splitByChar(E'\n', input()), x -> ngrams(x, 1)) AS char_rows
),
    part2_columns_raw AS (
    SELECT list_transform(range(1, length(char_rows[1]) + 1), column_index -> list_transform(char_rows, row -> row[column_index])) AS columns
    FROM part2_parsed_chars
),
    part2_columns_filtered AS (
    SELECT list_filter(columns, x -> NOT list_bool_and(list_transform(x, y -> y = ' '))) AS non_empty_columns
    FROM part2_columns_raw
),
    part2_numbers_extracted AS (
    SELECT list_transform(non_empty_columns, "column" -> replaceAll(
            arrayStringConcat(
                arraySlice("column", 1, length("column") - 1)
            ),
            ' ',
            ''
        )) AS number_strings
    FROM part2_columns_filtered
),
    part2_numbers_grouped AS (
    SELECT
        number_strings,
        non_empty_columns,
        
        -- Split numbers by operator positions
        arraySplit(
            list_transform(list_zip(
            number_strings,
            list_transform(non_empty_columns, "column" -> hasAny("column", ['+', '*']))), t -> t[2]),
            number_strings
        ) AS number_groups
    FROM part2_numbers_extracted, part2_columns_filtered
),
    part2_operations AS (
    SELECT list_zip(
        -- Extract operators from columns
        list_filter(arrayFlatten(non_empty_columns), x -> has(['+', '*'], x)),
        -- Pair with corresponding number groups
        number_groups
    ) AS operations_with_numbers
    FROM part2_numbers_grouped
),
    part2_solution AS (
    SELECT arraySum(
        list_transform(operations_with_numbers, operation -> if(
                -- Check operator type
                operation[1] = '*',
                
                -- Multiply all numbers in group
                toInt64(arrayProduct(
                    list_transform(operation[2], x -> toInt64(x))
                )),
                
                -- Otherwise, add all numbers in group
                toInt64(arraySum(
                    list_transform(operation[2], x -> toInt64(x))
                ))
            ))
    ) AS solution
    FROM part2_operations
)

-- Combine results from both parts
SELECT
    'Part 1' AS part,
    solution -- 5782351442566 with my input
FROM part1_solution

UNION ALL

SELECT
    'Part 2' AS part,
    solution -- 10194584711842 with my input
FROM part2_solution
