WITH data AS (
    SELECT regexp_split_to_table(column0, ',') as split
    FROM read_csv_auto('02/example.csv', header = false, delim = '')
),
split AS (
    SELECT
        unnest(range(CAST(string_split(split, '-')[1] AS BIGINT), CAST(string_split(split, '-')[2] AS BIGINT)+ 1) ) AS productID
    FROM data
),
possibleIds AS(
    SELECT CAST(productID AS string) AS productId, LEN(CAST(productID AS string)) as len
    FROM split
),
part1 AS (
    SELECT SUM(CAST(productId AS BIGINT)) as part1
    FROM possibleIds
    WHERE left(productId, len//2) = right(productId, len//2)
        AND LEN(CAST(productID AS string)) % 2 = 0
),
candidates AS (
    SELECT
        productId,
        UNNEST(generate_series(1, len // 2)) AS k
    FROM possibleIds
),
part2 AS (
    SELECT
        SUM(DISTINCT CAST(productId AS BIGINT)) as part2
    FROM candidates
    WHERE repeat(left(productId, k), length(productId) // k) = productId
)
FROM part1 CROSS JOIN part2

