CREATE TEMP TABLE avs_raw AS
SELECT * FROM read_csv('@RAW@', header = true, columns = {
  'shape': 'VARCHAR', 'query': 'VARCHAR', 'tbl': 'VARCHAR', 'mode': 'VARCHAR',
  'round': 'INTEGER', 'ms': 'DOUBLE', 'result': 'VARCHAR'});

CREATE TEMP TABLE avs_summary AS
SELECT shape, query, mode, tbl, count(*) AS n,
       round(median(ms), 3) AS median_ms, round(min(ms), 3) AS min_ms,
       round(max(ms), 3) AS max_ms
FROM avs_raw GROUP BY ALL;

COPY (SELECT * FROM avs_summary ORDER BY shape, query, mode, tbl)
TO '@OUT@/summary.csv' (HEADER);

SELECT d.shape, d.query, d.mode,
       d.median_ms || ' [' || d.min_ms || '..' || d.max_ms || ']' AS duck_art_ms,
       s.median_ms || ' [' || s.min_ms || '..' || s.max_ms || ']' AS search_ms,
       round(s.median_ms / d.median_ms, 2) AS search_over_duck
FROM avs_summary d JOIN avs_summary s USING (shape, query, mode)
WHERE d.tbl = 'duck' AND s.tbl = 'search'
ORDER BY d.shape, d.mode,
         array_position(['hit', 'miss', 'in100', 'in1000', 'probe'], d.query);

SELECT 'results differ' AS error, shape, query,
       string_agg(DISTINCT tbl || ':' || result, ' ') AS results
FROM avs_raw WHERE query <> 'probe'
GROUP BY shape, query HAVING count(DISTINCT result) > 1;
