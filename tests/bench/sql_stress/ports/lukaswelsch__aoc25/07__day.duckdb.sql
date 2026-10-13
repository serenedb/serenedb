WITH data AS (
    SELECT
        string_split_regex(column0, '') AS arr,
        row_number() OVER () AS y
    FROM read_csv_auto('07/example.csv', header = false, delim = '')
),
grid AS (
	SELECT UNNEST(arr) AS val, generate_subscripts(arr, 1) as x,
	row_number() OVER () AS y
	FROM data
	WHERE NOT arr <@ ['.']
),
start AS (
    SELECT x, y, val
    FROM grid
    WHERE val = 'S'
),
t AS (
	WITH RECURSIVE beams (
		x,
		y,
		cnt
	) AS (
	    SELECT
	        s.x,
	        s.y + 1 AS y,
	        1::BIGINT AS cnt
	    FROM start s

	    UNION ALL

	    SELECT
	        CASE
	            WHEN g.val = '.' THEN b.x
	            WHEN g.val = '^' AND lateral_step.side = 'L' THEN b.x - 1
	            WHEN g.val = '^' AND lateral_step.side = 'R' THEN b.x + 1
	        END AS nx,
	        b.y + 1 AS ny,
	        SUM(b.cnt) as cnt
		FROM beams b
		JOIN grid g ON g.x = b.x AND g.y = b.y
		CROSS JOIN (SELECT 'L' AS side UNION ALL SELECT 'R') AS lateral_step
		WHERE (g.val = '.' AND lateral_step.side = 'L')
		   OR (g.val = '^')
		GROUP BY nx, ny
	)
	SELECT *
	FROM beams
),
part1 AS (
	SELECT COUNT(*) as result
	 FROM t
	JOIN grid USING(x,y)
	WHERE grid.val = '^'

),
part2 AS (
-- + 1 because we miss the count of the first S
SELECT SUM(cnt) + 1 as result FROM t
JOIN grid USING(x,y)
WHERE grid.val = '^'
)
SELECT result, 'part1' as part FROM part1
UNION ALL
SELECT result, 'part2' as part FROM part2


