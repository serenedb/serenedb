WITH data AS (
    SELECT   string_split_regex(column0, '') as arr
    FROM read_csv_auto('06/example.csv', header = false, delim='')
),
unnested AS (
	SELECT
	        row_number() OVER () AS r,
	        unnest(arr) as val,
	        generate_subscripts(arr, 1) AS idx
	FROM data
),
rotated AS (
	SELECT
	    idx,
	    string_agg(val, '' ORDER BY r) AS col_str
	FROM unnested
	GROUP BY idx
	ORDER BY idx
),
grouped AS (
	SELECT
	 SUM(CASE WHEN regexp_full_match(col_str, '\s+')
	            THEN 1
	            ELSE 0
	        END) OVER (ORDER BY idx) AS grp, *
	FROM rotated
),
type AS (
	SELECT if('*' in col_str, '*', '+') AS type, grp FROM grouped WHERE col_str LIKE '%*%' OR col_str LIKE '%+%'
),
cleaned AS (
	SELECT regexp_replace(replace(replace(col_str, '*', ''), '+',''), '\s+', '')::BIGINT AS number, grp FROM grouped WHERE NOT regexp_full_match(col_str, '\s+')
),
homework AS (
	SELECT
	CASE WHEN type = '*' THEN CAST(exp(sum(ln (number))) AS BIGINT)
	ELSE sum(number)
	END AS quick_math
	FROM cleaned
	JOIN type USING(grp)
	GROUP BY grp, type
)
SELECT SUM(quick_math) FROM homework



