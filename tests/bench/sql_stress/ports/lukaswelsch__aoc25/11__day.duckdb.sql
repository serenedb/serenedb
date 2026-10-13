WITH edges AS (
    SELECT string_split(column0, ': ')[1] as src, regexp_split_to_table(string_split(column0, ': ')[2], ' ') as dst
    FROM read_csv_auto('11/example.csv', header = false)
),
nodes AS (
	SELECT DISTINCT src as node
	FROM edges
),
bfs AS (
	WITH RECURSIVE walks(node, path) AS (
		SELECT 'you', ARRAY['you'] AS path
		UNION ALL
		SELECT e.dst as node, list_append(path, e.dst) as path
		FROM walks
		JOIN edges e ON walks.node = e.src
	)
	SELECT *
	FROM walks
	WHERE node='out'
)
SELECT COUNT(*) FROM bfs
