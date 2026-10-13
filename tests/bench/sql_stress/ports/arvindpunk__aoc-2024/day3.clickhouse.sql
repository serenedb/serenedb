CREATE OR REPLACE MACRO toInt64(x) AS CAST(trunc(CAST(x AS DOUBLE)) AS BIGINT);
CREATE OR REPLACE MACRO toInt64OrZero(x) AS coalesce(TRY_CAST(x AS BIGINT), 0);
-- part a
WITH data AS (
    SELECT
        unnest(
            list_transform(regexp_extract_all(line, 'mul\((\d{1,3}),(\d{1,3})\)'), m -> [regexp_extract(m, 'mul\((\d{1,3}),(\d{1,3})\)', 1), regexp_extract(m, 'mul\((\d{1,3}),(\d{1,3})\)', 2)])
        ) AS pairs
    FROM
        "table"
)
SELECT
    sum(toInt64(pairs [1]) * toInt64(pairs [2]))
FROM
    data;

-- part b
WITH data AS (
    SELECT
        list_transform(regexp_extract_all(
            list_reduce(
                list(line),
                (acc, curr) -> concat(acc, curr),
                ''
            ),
            '(?:(do)\(\)|(don)''t\(\)|mul\((\d{1,3}),(\d{1,3})\))''?'
        ), m -> [regexp_extract(m, '(?:(do)\(\)|(don)''t\(\)|mul\((\d{1,3}),(\d{1,3})\))''?', 1), regexp_extract(m, '(?:(do)\(\)|(don)''t\(\)|mul\((\d{1,3}),(\d{1,3})\))''?', 2), regexp_extract(m, '(?:(do)\(\)|(don)''t\(\)|mul\((\d{1,3}),(\d{1,3})\))''?', 3), regexp_extract(m, '(?:(do)\(\)|(don)''t\(\)|mul\((\d{1,3}),(\d{1,3})\))''?', 4)]) AS pairs
    FROM
        "table"
)
SELECT
    list_reduce(
        pairs,
        (acc, curr) -> (
            acc[1] + acc[2] * (
                toInt64OrZero(curr [3]) * toInt64OrZero(curr [4])
            ),
            CASE WHEN curr [1] = 'do' THEN 1 WHEN curr [2] = 'don' THEN 0 ELSE acc[2] END
        ),
        (0 :: BIGINT, 1 :: BIGINT)
    )[1]
FROM
    data;