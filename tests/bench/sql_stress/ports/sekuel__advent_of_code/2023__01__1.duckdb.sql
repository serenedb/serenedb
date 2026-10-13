WITH input AS (
    SELECT val 
    FROM read_csv_auto('./2023/01/input.csv', columns = { val: text }, header=false)  
), extracted AS (
    SELECT regexp_extract_all(val, '\d') AS extracted_value FROM input
), replaced AS (
    SELECT 
        (val)
        .replace('twone', 'twoone')
        .replace('eighthree', 'eightthree')
        .replace('oneight', 'oneeight')
        .replace('threeight', 'threeeight')
        .replace('fiveight', 'fiveeight')
        .replace('nineight', 'nineeight')
        .replace('eightwo', 'eighttwo')
        .replace('sevenine', 'sevennine') AS replaced_val
    FROM input
), extracted_2 AS (
    SELECT 
        replaced_val, 
        list_transform(
            regexp_extract_all(replaced_val, 'one|eight|three|four|five|six|seven|two|nine|\d'), 
                x ->
                    CASE x 
                        WHEN 'one' THEN 1 
                        WHEN 'two' THEN 2
                        WHEN 'three' THEN 3
                        WHEN 'four' THEN 4
                        WHEN 'five' THEN 5
                        WHEN 'six' THEN 6
                        WHEN 'seven' THEN 7
                        WHEN 'eight' THEN 8
                        WHEN 'nine' THEN 9
                        ELSE x::int 
                    END
            )  AS extracted_value 
    FROM replaced
)
SELECT 'Part I' AS part, sum((extracted_value[1] || extracted_value[-1])::int) AS answer FROM extracted
UNION ALL
SELECT 'Part II' AS part, sum((extracted_value[1] || extracted_value[-1])::int) AS answer FROM extracted_2;