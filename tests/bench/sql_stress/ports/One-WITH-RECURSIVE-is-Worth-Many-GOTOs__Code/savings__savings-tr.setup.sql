\set MODE RECURSIVE
\set invocations 1024
CALL dbgen(sf => 0.01);
-- PL/SQL function to change the suppliers for a given TPC-H order
-- such that the supply cost for all parts are minimal.  Return savings
-- in % as well as an array listing the required supply chain changes
-- (‹part›, ‹old supplier›, ‹new supplier›).

-- representation of a supplier change
DROP TYPE IF EXISTS supplier_change CASCADE;
CREATE TYPE supplier_change AS (
  part int,  -- part for which supplier changed
  old  int,  -- old supplier
  new  int   -- new supplier
);

-- representation of supply chain changes
DROP TYPE IF EXISTS savings CASCADE;
CREATE TYPE savings AS (
  savings          numeric,     -- saved supply cost (in %)
  supplier_changes supplier_change[] -- supplier changes required to achieve savings
);


