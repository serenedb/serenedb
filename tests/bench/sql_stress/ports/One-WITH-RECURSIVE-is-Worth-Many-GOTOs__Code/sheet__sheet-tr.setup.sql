\set MODE RECURSIVE
\set invocations 128
\set iterations 128
-- Simple spreadsheet
--
-- Formulæ in cells may refer to each other.  The code below finds
-- dependencies between cells and iteratively evaluates the individual
-- cell formulæ in proper dependency order.

-- # of sheet evaluations (~ invocations)
\set N :invocations
\set R (1+sqrt(:iterations)) :: int

-- Valid formulæ:
-- 1. float literal (e.g. 4.2)
-- 2. cell reference (e.g., A3)
-- 3. n-ary operator (+, -, *, /) with n argument formulæ
-- 4. aggregate (sum, avg, max, min) over rectangular cell range (e.g. sum(A2:D5))

-- The spreadsheet is stored in table sheet, one cell per row.

-- A cell address (‹col›,‹row›), ('A',3) to represent A3
DROP TYPE IF EXISTS cell CASCADE;
CREATE TYPE cell AS (
  col   text, -- column (A..)
  "row" int   -- row (1..)
);

-- Intermediate cell contents during evaluation
DROP TYPE IF EXISTS contents CASCADE;
CREATE TYPE contents AS (
  cell  cell,
  value float
);

-- All argument cells (and their formulae) processed by an aggregate function
DROP TYPE IF EXISTS arguments CASCADE;
CREATE TYPE arguments AS (
  cells    cell[],
  formulae jsonb[]
);

-- The spreadsheet
DROP TABLE IF EXISTS sheet;
CREATE TABLE sheet (
  cell    cell PRIMARY KEY,  -- cell address
  formula jsonb              -- contained formula (JSON representation, see below)
);

-- JSON representation of formulæ:
-- 1. float literal:  {"entry":"num",  "num":4.2}
-- 2. cell reference: {"entry":"cell", "cell":"(A,3)"}
-- 3. operator:       {"entry":"op",   "op":"+", args:[‹formula›,‹formula›]}
-- 4. aggregate:      {"entry":"agg",  "agg":"sum", "from":"(A,2)", "to":"(D,5)"}


-- A sample spreadsheet instance (also see slides)
--
--   |     A        B        C         D
-- --+-------------------------------------
-- 1 |     1       3.50    A1×B1      0.88
-- 2 |     2       6.10    A2×B2
-- 3 |     2       0.98    A3×B3
-- 4 | SUM(A1:A3)        SUM(C1:C3)  D1×C4
--
-- INSERT INTO sheet(cell, formula) VALUES
--   (('A',1), '{"entry":"num", "num":1}'),
--   (('A',2), '{"entry":"num", "num":2}'),
--   (('A',3), '{"entry":"num", "num":2}'),
--   (('A',4), '{"entry":"agg", "agg":"sum", "from":"(A,1)", "to":"(A,3)"}'),
--   (('B',1), '{"entry":"num", "num":3.50}'),
--   (('B',2), '{"entry":"num", "num":6.10}'),
--   (('B',3), '{"entry":"num", "num":0.98}'),
--   (('C',1), '{"entry":"op" , "op" :"*", "args":[{"entry":"cell", "cell":"(A,1)"}, {"entry":"cell", "cell":"(B,1)"}]}'),
--   (('C',2), '{"entry":"op" , "op" :"*", "args":[{"entry":"cell", "cell":"(A,2)"}, {"entry":"cell", "cell":"(B,2)"}]}'),
--   (('C',3), '{"entry":"op" , "op" :"*", "args":[{"entry":"cell", "cell":"(A,3)"}, {"entry":"cell", "cell":"(B,3)"}]}'),
--   (('C',4), '{"entry":"agg", "agg":"sum", "from":"(C,1)", "to":"(C,3)"}'),
--   (('D',1), '{"entry":"num", "num":0.88}'),
--   (('D',4), '{"entry":"op" , "op" :"*", "args":[{"entry":"cell", "cell":"(D,1)"}, {"entry":"cell", "cell":"(C,4)"}]}');


-- Build a spreadsheet of the form
--
--     |     A        B        C         D
-- ----+-------------------------------------
--   1 |     1        1      A1+B1      0.88
--   2 |    C1        2      A2+B2
--   3 |    C2        3      A3+B3
--   ⋮ |
--   i |    C(i-1)    i      Ai+Bi
--   ⋮ |
--   N |    C(N-1)    N      AN+BN
-- N+1 |                  SUM(C1:CN)  D1×C(N+1)


INSERT INTO sheet(cell, formula) VALUES
  (('A',1), jsonb_build_object('entry', 'num', 'num', 1)),
  (('B',1), jsonb_build_object('entry', 'num', 'num', 1)),
  (('C',1), jsonb_build_object('entry', 'op', 'op', '+', 'args', array[jsonb_build_object('entry', 'cell', 'cell', ('A',1) :: text),
                                                                       jsonb_build_object('entry', 'cell', 'cell', ('B',1) :: text)]));

INSERT INTO sheet(cell, formula)
  SELECT cell.*
  FROM   generate_series(2, :R) AS i, LATERAL
         (VALUES (('A',i) :: cell, jsonb_build_object('entry', 'cell', 'cell', ('C',i-1) :: text)),
                 (('B',i) :: cell, jsonb_build_object('entry', 'num', 'num', i)),
                 (('C',i) :: cell, jsonb_build_object('entry', 'op', 'op', '+', 'args', array[jsonb_build_object('entry', 'cell', 'cell', ('A',i) :: text),
                                                                                              jsonb_build_object('entry', 'cell', 'cell', ('B',i) :: text)]))) AS cell;

INSERT INTO sheet(cell, formula) VALUES
  (('C',:R+1), jsonb_build_object('entry', 'agg', 'agg', 'sum', 'from', ('C',1) :: text, 'to', ('C',:R) :: text)),
  (('D',1),    jsonb_build_object('entry', 'num', 'num', 0.88)),
  (('D',:R+1), jsonb_build_object('entry', 'op', 'op', '*', 'args', array[jsonb_build_object('entry', 'cell', 'cell', ('D',1) :: text),
                                                                          jsonb_build_object('entry', 'cell', 'cell', ('C',:R+1) :: text)]));

