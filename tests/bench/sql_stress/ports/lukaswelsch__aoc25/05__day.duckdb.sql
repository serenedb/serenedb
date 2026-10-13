WITH data AS (
    SELECT regexp_split_to_table(column0, ',') as split
    FROM read_csv_auto('05/example.csv', header = false, delim = '')
),
idranges AS (
    SELECT
        CAST(string_split(split, '-')[1] AS HUGEINT) AS productID_start,
        CAST(string_split(split, '-')[2] AS HUGEINT) AS productID_end
    FROM data
    WHERE split like '%-%'
),
idsToCheck AS (
    SELECT CAST(split AS BIGINT) as productIdToCheck
    FROM data
    WHERE split not like '%-%'
),
part1 AS (
    SELECT DISTINCT productIdToCheck
    FROM idranges r
          JOIN idsToCheck c
                 ON c.productIdToCheck > r.productID_start
                 AND c.productIdToCheck <= r.productID_end
),
intervals AS (
    -- sortiert nach productID_start, schaue ob es eine productID_ende gibt das den start inkludiert
    SELECT
        productID_start,
        productID_end,
        productID_start <= LAG(productID_end) OVER (ORDER BY productID_start, productID_end) AS grp
    FROM idranges
),
sum_ordered AS (
    -- gib jedem Start eine eigene Gruppe
    SELECT
        productID_start,
        productID_end,
        SUM(CASE WHEN grp
            THEN 0
            ELSE 1
        END) OVER (ORDER BY productID_start, productID_end) AS grp
    FROM intervals
),
intermediate_ranges AS (
    -- pro Gruppe finde den min anfangs und max end wert
    SELECT
        MIN(productID_start) as min_start,
        MAX(productID_end) + 1 as max_start
    FROM sum_ordered
    GROUP BY grp
),
temp AS (
    SELECT max_start - min_start as s
    FROM intermediate_ranges
),
doublecounted AS (
    -- falls end = start dann müssten eigentlich beide in einer gruppe sein,
    -- würde durch das max + 1 das doppelt gezählt werden
    SELECT -COUNT(*) AS s
    from sum_ordered a
    JOIN sum_ordered b
    ON a.productID_end = b.productID_start
    AND a.grp != b.grp
)
SELECT SUM(s) as result, 'part2' as challenge
FROM (
    SELECT s
    FROM temp
    UNION ALL
    SELECT * FROM doublecounted
 )
UNION ALL
SELECT COUNT(*), 'part1'
FROM part1
ORDER BY challenge