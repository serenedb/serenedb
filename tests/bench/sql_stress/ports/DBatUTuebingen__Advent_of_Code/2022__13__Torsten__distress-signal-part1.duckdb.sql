SET integer_division = true;
-- AoC 2022, Day 13 (Part 1)

-- input file
DROP MACRO IF EXISTS input;
CREATE MACRO input() AS '2022/13/Torsten/input.txt';

-- no divider packets (empty table)
CREATE TABLE dividers (
  _       int,
  pair    int,
  "left?" boolean,
  packet  text
);

-- read input, perform packet encoding, return table nocomma(pair,"left?",c)
-- once encoded, packets in c may be simply compared via <
-- AoC 2022, Day 13 (code common to Parts 1 & 2)
--
-- • Include this via .read distress-signal.sql
--
-- • Assumes macro input() and table dividers(_,pair,"left?",packet) bein defined

-- • Yields table nocomma(pair,"left?",c) with depth-encoded packet strings c
--   - depth-based packet encoding inspired by
--     https://gist.github.com/a-ponomarev/eadf5e4305960729cb54cfe5b461245d

-- NB. No recursion involved, instead uses window APL sum scan trick
--     to track packet nesting depth.


CREATE TABLE nocomma AS
  WITH
  packets(_,pair,"left?",packet) AS (
    SELECT 1 + ROW_NUMBER() OVER () AS id, id // 2 AS pair, id % 2 = 0 AS "left?", c.packet
    FROM   read_csv_auto(input(), delim='', all_varchar=true, header=false) AS c(packet)
    WHERE  c.packet IS NOT NULL
    -- add divider packets
      UNION ALL
    TABLE dividers
  ),
  depth(pair,"left?",pos,c,depth) AS (
    SELECT  p.pair,
            p."left?",
            ROW_NUMBER() OVER (PARTITION BY p.pair, p."left?") AS pos,
            i.c,
            SUM(([0,1,-1])[array_position(['[',']'], i.c)+1]) OVER (PARTITION BY p.pair, p."left?" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) :: int AS depth
    FROM    packets AS p,
    LATERAL unnest(string_split(reverse(replace(p.packet,'10','A')), NULL)) AS i(c)
    --                          ^^^^^^^
    --                          ⚠️ try to compensate the lack of WITH ORDINALITY and the loss of string order
    --                             (this is a disgrace: DuckDB needs WITH ORDINALITY!)
  ),
  nocomma(pair,"left?",c) AS (
    SELECT d.pair,
           d."left?",
           string_agg(CASE
                        WHEN d.c = ','        THEN chr(ord('!') + d.depth)
                        WHEN d.c IN ('[',']') THEN ''
                        ELSE d.c
                      END,
                      '') AS c
    FROM   depth AS d
    GROUP BY d.pair, d."left?"
  )
  TABLE nocomma;

-- (sum of) indices with packet pairs in correct < order
SELECT SUM(l.pair) AS indices
FROM   nocomma AS l, nocomma AS r
WHERE  l.pair = r.pair
AND    l."left?" AND NOT r."left?"
AND    l.c < r.c;  -- correct ordering of left/right packets?
