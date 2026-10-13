\set iterations 500
-- Recursive interpreter for artihmetic expressions,
-- walks an expression DAG over +/* operators and numeric literals
--
-- Note: Function eval() defines a standard top-down evaluation, but
--       the evaluation AFTER COMPILATION to WITH RECURSIVE will perform
--       *parallel* bottom-up evaluation:
--       *all* literal leaves will be evaluated in one iteration,
--       in the next iteration *all* operators referring to these leaves
--       will be evaluated, etc.

\set n 13

DROP TABLE IF EXISTS expression CASCADE;

-- Expression representation (DAG)
CREATE TABLE expression(
  node int PRIMARY KEY,   -- expression node ID
  op   char(1),           -- operator ∊ {+,*} or ℓ (≡ literal)
  arg1 int,               -- left and
  arg2 int,               --   right arguments of operator (node = arg1 = arg2 if literal)
  lit  numeric,           -- literal value (0 if operator)
  FOREIGN KEY (arg1) REFERENCES expression(node),
  FOREIGN KEY (arg2) REFERENCES expression(node));

-- Make the expression row type hashable and comparable
--
-- Make the expression rowtype hashable and comparable
--
DROP FUNCTION IF EXISTS expression_hash(expression) CASCADE;
CREATE FUNCTION expression_hash(e expression) RETURNS int AS
$$
  SELECT e.node;
$$ LANGUAGE SQL IMMUTABLE;

DROP OPERATOR IF EXISTS =(expression, expression) CASCADE;
DROP FUNCTION IF EXISTS expression_eq(expression, expression) CASCADE;
CREATE FUNCTION expression_eq(e1 expression, e2 expression) RETURNS boolean AS
$$
  SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) IS NOT DISTINCT FROM (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit);
$$ LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR =(leftarg = expression, rightarg = expression, procedure = expression_eq, commutator = =, HASHES, MERGES);

DROP OPERATOR IF EXISTS <=(expression, expression) CASCADE;
DROP FUNCTION IF EXISTS expression_leq(expression, expression) CASCADE;
CREATE FUNCTION expression_leq(e1 expression, e2 expression) RETURNS boolean AS
$$
  SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) <= (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR <=(leftarg = expression, rightarg = expression, procedure = expression_leq, commutator = >=);

DROP OPERATOR IF EXISTS <(expression, expression) CASCADE;
DROP FUNCTION IF EXISTS expression_lt(expression, expression) CASCADE;
CREATE FUNCTION expression_lt(e1 expression, e2 expression) RETURNS boolean AS
$$
  SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) < (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR <(leftarg = expression, rightarg = expression, procedure = expression_lt, commutator = >);

DROP OPERATOR IF EXISTS >=(expression, expression) CASCADE;
DROP FUNCTION IF EXISTS expression_geq(expression, expression) CASCADE;
CREATE FUNCTION expression_geq(e1 expression, e2 expression) RETURNS boolean AS
$$
  SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) >= (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR >=(leftarg = expression, rightarg = expression, procedure = expression_geq, commutator = <=);

DROP OPERATOR IF EXISTS >(expression, expression) CASCADE;
DROP FUNCTION IF EXISTS expression_gt(expression, expression) CASCADE;
CREATE FUNCTION expression_gt(e1 expression, e2 expression) RETURNS boolean AS
$$
  SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) > (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR >(leftarg = expression, rightarg = expression, procedure = expression_gt, commutator = <);

DROP FUNCTION IF EXISTS expression_cmp(box, box) CASCADE;
CREATE FUNCTION expression_cmp(e1 expression, e2 expression) RETURNS int AS
$$
  SELECT CASE WHEN e1 = e2 THEN 0
              WHEN e1 < e2 THEN -1
              ELSE  1
         END;
$$
LANGUAGE SQL IMMUTABLE;

DROP OPERATOR CLASS IF EXISTS expression_hash_ops USING hash CASCADE;
CREATE OPERATOR CLASS expression_hash_ops DEFAULT FOR TYPE expression USING hash AS
  operator 1 =,
  function 1 expression_hash(expression);

DROP OPERATOR CLASS IF EXISTS expression_btree_ops USING btree CASCADE;
CREATE OPERATOR CLASS expression_btree_ops DEFAULT FOR TYPE expression USING btree AS
  operator 1 <,
  operator 2 <=,
  operator 3 =,
  operator 4 >=,
  operator 5 >,
  function 1 expression_cmp(expression, expression);

-----------------------------------------------------------------------
-- Random expression generation

SELECT setseed(0.42);

-- Randomly select an operator
DROP FUNCTION IF EXISTS oper();
CREATE FUNCTION oper() RETURNS char(1) AS
$$
  SELECT (array['+','*'])[1 + random()];
$$ LANGUAGE SQL;


INSERT INTO expression(node, op, arg1, arg2, lit)
  WITH RECURSIVE
  -- (1) construct inner operation nodes of expression tree
  operations(n, node, op, arg1, arg2, lit) AS (
    SELECT :n AS n, 0 AS node, oper() AS op, 0 * 2 + 1 AS arg1, 0 * 2 + 2 AS arg2, 0::numeric AS lit

      UNION ALL

    SELECT e.n - 1 AS n, n.node, n.op, n.arg1, n.arg2, n.lit
    FROM   operations AS e,
           LATERAL (VALUES (e.node * 2 + 1, oper(), (e.node * 2 + 1) * 2 + 1, (e.node * 2 + 1) * 2 + 2, 0::numeric),
                           (e.node * 2 + 2, oper(), (e.node * 2 + 2) * 2 + 1, (e.node * 2 + 2) * 2 + 2, 0::numeric)) AS n(node,op,arg1,arg2,lit)
    WHERE  e.n > 0
  ),
  -- (2) at the leaf level add literal expressions
  expr(node, op, arg1, arg2, lit) AS (
    SELECT  e.node, e.op, e.arg1, e.arg2, e.lit
    FROM    operations AS e
    WHERE   e.n > 0

      UNION

    SELECT  e.node, 'ℓ' AS op, e.node AS arg1, e.node AS arg2,
            100 * random() AS lit
    FROM    operations AS e
    WHERE   e.n = 0
  ),
  -- (3) introduce distortion and sharing into the expression, we end up with a DAG
  shuffle(node, op, arg1, arg2, lit) AS (
    SELECT e.node, e.op,
           CASE WHEN random() < 0.1 THEN (SELECT e1.node FROM expr AS e1 WHERE e1.node > e.node ORDER BY random() LIMIT 1)
                                    ELSE e.arg1
           END AS arg1,
           CASE WHEN random() < 0.1 THEN (SELECT e1.node FROM expr AS e1 WHERE e1.node > e.node ORDER BY random() LIMIT 1)
                                    ELSE e.arg2
           END AS arg2,
           e.lit
    FROM   expr AS e
    WHERE  e.op <> 'ℓ'

      UNION

    SELECT e.*
    FROM   expr AS e
    WHERE  e.op = 'ℓ'
  ),
  maintree AS (
    SELECT *
    FROM   shuffle AS s
    WHERE  s.node = 0
      UNION
    SELECT s.*
    FROM   maintree AS m,
           LATERAL (SELECT *
                    FROM   shuffle AS s
                    WHERE  m.arg1 = s.node
                      UNION ALL
                    SELECT *
                    FROM   shuffle AS s
                    WHERE  m.arg2 = s.node) AS s
  )
  TABLE maintree;

analyze expression;