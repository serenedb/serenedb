\set MODE RECURSIVE
\set invocations 1024
CALL dbgen(sf => 0.01);
-- Input: o_orderkey
-- Output: scheduling order of lineitems of order that maximizes profit (≡ l_extendedprice) of
--         items produced before their l_shipdate, first scheduling date is o_orderdate,
--         each part of a lineitem needs one day to produce (i.e., we l_quantity days to produce one lineitem)

DROP TYPE IF EXISTS order_details CASCADE;
CREATE TYPE order_details AS (
  items         int,  -- # of lineitems in order
  last_shipdate date  -- last possible ship date (of all items)
);

DROP TYPE IF EXISTS scheduled CASCADE;
CREATE TYPE scheduled AS (
  item int,    -- linenumber of scheduled lineitem
  "when" date  -- when production of lineitem starts
);

