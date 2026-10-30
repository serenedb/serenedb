CREATE TEMP TABLE avs_pick AS
SELECT q, j,
       CASE WHEN q = 'probe' THEN @ROWS@ + 1000000 + j
            WHEN q = 'hit' OR (q <> 'miss' AND j % 2 = 0)
            THEN (j * 2654435761) % @ROWS@
            ELSE @ROWS@ + j END AS i
FROM (VALUES ('hit', 1), ('miss', 1), ('in100', 100), ('in1000', 1000),
             ('probe', 1)) s(q, n)
JOIN range(1, 1001) r(j) ON j <= n;

SELECT 'hex', q, string_agg('''' || md5(i::VARCHAR) || '''', ', ' ORDER BY j)
FROM avs_pick GROUP BY q ORDER BY q;

SELECT 'int', q, string_agg(((hash(i) >> 1)::BIGINT)::VARCHAR, ', ' ORDER BY j)
FROM avs_pick GROUP BY q ORDER BY q;

DROP TABLE avs_pick;
