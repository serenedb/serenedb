-----------------------------------------------------------------------
-- Translation into a UDF using trampolined style
-- SQL UDF in functional style: expression interpreter

DROP TYPE IF EXISTS K;
CREATE TYPE K AS (arg2 int, e1 numeric, ref int);

DROP FUNCTION IF EXISTS evalTS(expression);

CREATE FUNCTION evalTS(e expression) RETURNS numeric AS
$$
WITH RECURSIVE tramp(fn, e, x, k) AS (
  SELECT 'eval', e, 0 :: numeric, ARRAY[] :: K[]
    UNION ALL
  SELECT _.*
  FROM   tramp AS t, LATERAL
          (SELECT 'apply', t.e, (t.e :: expression).lit, t.k
          WHERE t.fn = 'eval' AND (t.e :: expression).op = 'ℓ'
            UNION ALL
          SELECT 'eval', (SELECT e1 FROM expression AS e1 WHERE e1.node = (t.e :: expression).arg1),
                 t.x, ((t.e :: expression).arg2, 0, 1) :: K || t.k
          WHERE t.fn = 'eval' AND (t.e :: expression).op = '+'
            UNION ALL
          SELECT 'eval', (SELECT e1 FROM expression AS e1 WHERE e1.node = (t.e :: expression).arg1),
                 t.x, ((t.e :: expression).arg2, 0, 2) :: K || t.k
          WHERE t.fn = 'eval' AND (t.e :: expression).op = '*'
            UNION ALL
          SELECT 'finish', t.e, t.x, t.k
          WHERE t.fn = 'apply' AND CARDINALITY(t.k) = 0
            UNION ALL
          SELECT 'eval', (SELECT e2 FROM expression AS e2 WHERE e2.node = t.k[1].arg2),
                 t.x, array_prepend((t.k[1].arg2, t.x, 3) :: K, t.k[2:])
          WHERE t.fn = 'apply' AND t.k[1].ref = 1
            UNION ALL
          SELECT 'eval', (SELECT e2 FROM expression AS e2 WHERE e2.node = t.k[1].arg2),
                 t.x, array_prepend((t.k[1].arg2, t.x, 4) :: K, t.k[2:])
          WHERE t.fn = 'apply' AND t.k[1].ref = 2
            UNION ALL
          SELECT 'apply', t.e, t.k[1].e1 + t.x, t.k[2:]
          WHERE t.fn = 'apply' AND t.k[1].ref = 3
            UNION ALL
          SELECT 'apply', t.e, t.k[1].e1 * t.x, t.k[2:]
          WHERE t.fn = 'apply' AND t.k[1].ref = 4
          ) AS _
) SELECT t.x FROM tramp AS t WHERE t.fn = 'finish';
$$ LANGUAGE SQL;

-----------------------------------------------------------------------
-- Perform expression evaluation of root expression

SELECT setseed(0.42);
SELECT e.node AS node, evalTS(e) AS result
FROM expression AS e
LIMIT :iterations;
