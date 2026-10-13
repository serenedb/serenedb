\set MODE RECURSIVE
\set invocations 1024
CALL dbgen(sf => 0.01);
-- Find buy and sell orders for a given part such that
-- we can maximize the margin for that part.

-- \c tpch

-- index that can help queries ➊ and ➌
DROP INDEX IF EXISTS orders_o_orderdate;
CREATE INDEX orders_o_orderdate ON orders (o_orderdate);
ANALYZE orders;

-- index that can help query ➋
DROP INDEX IF EXISTS lineitem_l_orderkey_l_partkey;
CREATE INDEX lineitem_l_orderkey_l_partkey ON lineitem (l_orderkey, l_partkey);
ANALYZE lineitem;


-- trade: buying/selling a part in these orders yields indicated margin
DROP TYPE IF EXISTS trade CASCADE;
CREATE TYPE trade AS (
  buy    int,           -- buy part of this order and...
  sell   int,           -- sell part of this order, this...
  margin numeric(15,2)  -- ... will be your margin
);

-- dated order: order happened on indicated date
-- (NB: in TPC-H, o_orderkey does NOT ascend with o_orderdate)
DROP TYPE IF EXISTS dated_order CASCADE;
CREATE TYPE dated_order AS (
  orderkey  int,  -- this order was placed...
  orderdate date  -- ... on this date
);

