WITH data AS (
    SELECT string_split_regex(column0, '\s+') as arr
    FROM read_csv_auto('06/example.csv', header = false, delim='')
),
unnested AS (
	SELECT
		row_number() OVER () AS r,
		unnest(arr) as val,
		generate_subscripts(arr, 1) AS x
	FROM data
),
type AS (
	SELECT * FROM unnested WHERE val IN ('*', '+')
),
numbers AS (
	SELECT val::INTEGER AS number, x FROM unnested WHERE val NOT IN ('*', '+')
),
homework AS (
	SELECT
	CASE WHEN val = '*' THEN CAST(exp(sum(ln (number))) AS BIGINT)
	ELSE sum(number)
	END AS quick_math
	FROM numbers
	JOIN type USING(x)
	GROUP BY x, val
),
part1 AS (
	SELECT SUM(quick_math) FROM homework
)
SELECT * FROM part1
