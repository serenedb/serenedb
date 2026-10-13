WITH edges AS (
    SELECT string_split(column0, ': ')[1] as src, regexp_split_to_table(string_split(column0, ': ')[2], ' ') as dst
    FROM read_csv_auto('11/example2.csv', header = false)
),
nodes AS (
	SELECT DISTINCT src as node
	FROM edges
),
svr_fft AS (
	WITH RECURSIVE walks(node, num_paths) AS (
	    SELECT 'svr' AS node, 1::BIGINT AS num_paths
	    UNION ALL
	    SELECT e.dst AS node, SUM(walks.num_paths) AS num_paths
	    FROM walks
	    JOIN edges e ON walks.node = e.src
	    GROUP BY e.dst
	)
	SELECT node, num_paths FROM walks
	WHERE node = 'fft'
),
fft_dac AS (
	WITH RECURSIVE walks(node, num_paths) AS (
	    SELECT 'fft' AS node, 1::BIGINT AS num_paths
	    UNION ALL
	    SELECT e.dst AS node, SUM(walks.num_paths) AS num_paths
	    FROM walks
	    JOIN edges e ON walks.node = e.src
	    GROUP BY e.dst
	)
	SELECT node, num_paths FROM walks
	WHERE node = 'dac'
),
dac_out AS (
	WITH RECURSIVE walks(node, num_paths) AS (
	    SELECT 'dac' AS node, 1::BIGINT AS num_paths
	    UNION ALL
	    SELECT e.dst AS node, SUM(walks.num_paths) AS num_paths
	    FROM walks 
	    JOIN edges e ON walks.node = e.src
	    GROUP BY e.dst
	)
	SELECT node, num_paths FROM walks
	WHERE node = 'out'
),
combined AS (
	SELECT SUM(num_paths) as num_paths FROM svr_fft
	UNION ALL
	SELECT SUM(num_paths) as num_paths FROM fft_dac
	UNION ALL
	SELECT SUM(num_paths) as num_paths FROM dac_out
)
SELECT list_reduce(list(num_paths), lambda x, y: x*y) as part2
FROM combined
