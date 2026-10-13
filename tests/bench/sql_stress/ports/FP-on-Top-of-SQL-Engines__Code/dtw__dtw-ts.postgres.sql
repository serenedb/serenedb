DROP TYPE IF EXISTS K CASCADE;
CREATE TYPE K AS (i int, j int, d1 double precision, d2 double precision, ref int);

DROP FUNCTION IF EXISTS dtwTS(int, int);

CREATE FUNCTION dtwTS(l1 int, l2 int) RETURNS double precision AS
$$
WITH RECURSIVE tramp(fn, i, j, x, k) AS (
  SELECT 'DTW', l1, l2, 0 :: double precision, ARRAY[] :: K[]
    UNION ALL
  SELECT _.*
  FROM   tramp AS t, LATERAL
          (SELECT 'Apply', t.i, t.j, 0, t.k
          WHERE t.fn = 'DTW' AND t.i = 0 AND t.j = 0
            UNION ALL
          SELECT 'Apply', t.i, t.j, 'Infinity' :: double precision, t.k
          WHERE t.fn = 'DTW' AND t.i <> 0 AND t.j = 0
            UNION ALL
          SELECT 'Apply', t.i, t.j, 'Infinity' :: double precision, t.k
          WHERE t.fn = 'DTW' AND t.i = 0 AND t.j <> 0
            UNION ALL
          SELECT 'DTW', t.i-1, t.j-1, t.x, array_prepend((t.i, t.j, 0, 0, 1) :: K, t.k)
          WHERE t.fn = 'DTW' AND t.i <> 0 AND t.j <> 0
            UNION ALL
          SELECT 'Finish', t.i, t.j, t.x, t.k
          WHERE t.fn = 'Apply' AND CARDINALITY(t.k) = 0
            UNION ALL
          SELECT 'DTW', t.k[1].i-1, t.k[1].j, t.x,
                 array_prepend((t.k[1].i, t.k[1].j, t.x, 0, 2) :: K, t.k[2:])
          WHERE t.fn = 'Apply' AND t.k[1].ref = 1
            UNION ALL
          SELECT 'DTW', t.k[1].i, t.k[1].j-1, t.x,
                 array_prepend((t.k[1].i, t.k[1].j, t.k[1].d1, t.x, 3) :: K, t.k[2:])
          WHERE t.fn = 'Apply' AND t.k[1].ref = 2
            UNION ALL
          SELECT 'Apply', t.i, t.j, ABS(X.x-Y.y) + LEAST(t.k[1].d1, t.k[1].d2, t.x), t.k[2:]
          FROM X, Y
          WHERE t.fn = 'Apply' AND t.k[1].ref = 3 AND (X.t, Y.t) = (t.k[1].i, t.k[1].j)
          ) AS _
) SELECT t.x FROM tramp AS t WHERE t.fn = 'Finish';
$$ LANGUAGE SQL STABLE STRICT;

SELECT setseed(0.42);
SELECT dtwTS(x, x)
FROM generate_series(1, (SELECT COUNT(*) :: int FROM X)) AS _(x)
LIMIT :iterations;
