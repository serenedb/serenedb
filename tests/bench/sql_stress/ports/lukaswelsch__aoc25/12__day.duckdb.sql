WITH shapes AS (
    SELECT * as col, row_number() OVER () AS id
    FROM read_csv_auto('12/example_shape.csv', header = false)
),
block_starts AS (
	SELECT
	    t.*,
	     if(LAG(col) OVER (ORDER BY id) IS NULL, 1, 0) AS block_start
	FROM shapes t
	ORDER BY id
),
grouped AS (
	SELECT
		len(replace(col, '.', '')) as val,
		id,
		SUM(block_start) OVER(ORDER BY id) as grp,
		col
	FROM block_starts b
),
counted AS (
	SELECT
		grp,
		SUM(val) as shape_val
	FROM grouped
	WHERE col not like '%:%'
	GROUP BY grp
),
rectangles AS (
    SELECT
        list_reduce(regexp_split_to_array(string_split(column0, ': ')[1], 'x')::INTEGER[], lambda x, y: x*y) as src,
        regexp_split_to_array(string_split(column0, ': ')[2], ' ') as arr,
        row_number() OVER () AS shape_id
    FROM read_csv_auto('12/example.csv', header = false)
),
rectangle_shapes AS (
	SELECT
		generate_subscripts(arr, 1) as id,
		unnest(arr)::INTEGER as count,
		*
	FROM rectangles r
)
SELECT
	src,
	shape_id,
	SUM(shape_val * count) as cnt
FROM rectangle_shapes r
 LEFT JOIN counted c  ON r.id = c.grp
 GROUP BY shape_id, src
 HAVING cnt < src