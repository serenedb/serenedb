-- Liquid flow simulation
-- (example of a cell automaton in which a cell is influenced by as well
--  as influences its neighbours)
--
-- Loosely based on
-- https://www.reddit.com/r/cellular_automata/comments/6jhdfw/i_used_1dimensional_cellular_automata_to_make_a/
--
-- Visualize using ./fluid.py ‹CSV output file›

-- N: # of simualtion steps
CREATE OR REPLACE MACRO N() AS 300;

-- A repertoire of initial worlds
--
-- fluid1.txt:
--
-- ░▄           ▄░
-- ░██         ██░
-- ░██         ██░
-- ░██         ██░
-- ░██         ██░
-- ░░░░░░░░░░░░░░░


-- fluid2.txt:
--
-- ░                  ███░
-- ░                █████░
-- ░                █████░
-- ░                █████░
-- ░                █████░
-- ░                █████░
-- ░                █████░
-- ░                █████░
-- ░                █████░
-- ░░░░░░░░░░░░░░░░░░░░░░░


-- fluid3.txt:
--
-- ░         █████        ░
-- ░         █████        ░
-- ░         █████        ░
-- ░         █████        ░
-- ░         █████        ░
-- ░         █████        ░
-- ░         █████        ░
-- ░         █████        ░
-- ░         █████        ░
-- ░░░░░░░░░░░░░░░░░░░░░░░░


-- fluid4.txt:
--
-- ░                      ░
-- ░                      ░
-- ░                      ░
-- ░████████      ████████░
-- ░████████      ████████░
-- ░████████      ████████░
-- ░████████      ████████░
-- ░████████      ████████░
-- ░████████      ████████░
-- ░░░░░░░░░░░░░░░░░░░░░░░░

-- fluid5.txt:
--
-- ░                        ▄░
-- ░                     ████░
-- ░                   ██████░
-- ░░    ░░░         ████████░
-- ░░░  ░░░░░░      █████████░
-- ░░░░░░░░░░░░     █████████░
-- ░░░░░░░░░░░░░░░░░░░░░░░░░░░

CREATE OR REPLACE MACRO fluid() AS 'material/06/fluid5.txt';

DROP TABLE IF EXISTS input;
CREATE TEMPORARY TABLE input (
  y     int PRIMARY KEY,
  cells text NOT NULL
);

INSERT INTO input(y,cells)
  SELECT rows.y, rows.cells
  FROM   unnest((SELECT string_split(txt.content[:-2], E'\n')
                 FROM   read_text(fluid()) AS txt))
         WITH ORDINALITY AS rows(cells,y);

TABLE input
ORDER BY y;


-- One cell of the autmaton = one column (ground + water) in the world
--
DROP TABLE IF EXISTS fluid;
CREATE TABLE fluid (
  x      int     NOT NULL,                         -- x coordinate
  ground int     NOT NULL CHECK (ground % 8 = 0),  -- elevation at point x (divisible by 8, because of character-based rendering)
  water  numeric NOT NULL,                         -- water volume at point x
  PRIMARY KEY (x)
);

-- Turn input table format into valid initial world
--
INSERT INTO fluid(x, ground, water)
  SELECT  col.x,
          COALESCE(sum(8) FILTER (WHERE col.cell = '░'), 0)                            AS ground,
          sum(COALESCE(list_position(['▁','▂','▃','▄','▅','▆','▇','█'], col.cell), 0)) AS water
  FROM    input AS row,
  LATERAL unnest(string_split(row.cells, NULL))
          WITH ORDINALITY AS col(cell,x)
  GROUP BY col.x;

TABLE fluid
ORDER BY x;

-- influence of neighboring cells on position x
DROP TYPE IF EXISTS influence;
CREATE TYPE influence AS row(
  x        int,
  Δwater   numeric,
  Δkinetic numeric
);

-- Save flow simulation result so that we can copy it to a CSV file below
DROP TABLE IF EXISTS simulation;
CREATE TEMPORARY TABLE simulation AS
--
WITH RECURSIVE
sim(iter,x,ground,water,kinetic) AS (
  SELECT 0 AS iter, f.x, f.ground, f.water, 0.0 :: numeric AS kinetic
  FROM   fluid AS f
    UNION ALL
  SELECT s0.iter + 1 AS iter, s0.x, s0.ground,
         --  if cell s0 has not been updated, Δwater ≡ NULL (we then keep our current water volume)
         --           ┌───────────┴───────────┐
         s0.water   + COALESCE(agg.Δwater  , 0) AS water,   -- water volume at x
         s0.kinetic + COALESCE(agg.Δkinetic, 0) AS kinetic  -- kinetic energy at x (< 0: towards left neighbor, > 0: towards right neighbor)
  FROM   -- iterate over all cells s0 to find the changes that apply to it
         sim AS s0
         -- aggregate all the influences to be applied to current cell s0 (if there are none these yield NULL)
         --                          ┌────────┴─────────┐            ┌─────────┴──────────┐
           LEFT OUTER JOIN
         LATERAL (SELECT infs.xwk.x, sum(infs.xwk.Δwater) AS Δwater, sum(infs.xwk.Δkinetic) AS Δkinetic
                         -- encodes the rules of the fluid flow automaton:
                         -- the SELECT yields a single array of entries (x,Δwater,Δkinetic)
                         -- to indicate that the cell at x needs to change
                         -- its water volum by Δwater and its kinetic energy by Δkinetic
                  FROM   (SELECT (-- flow to the left
                                  --           potential energy
                                  --        ┌────────┴─────────┐
                                  CASE WHEN s1.ground + s1.water - s1.kinetic > lag(s1.ground, 1) OVER horizontal + LAG(s1.water, 1) OVER horizontal + lag(s1.kinetic, 1) OVER horizontal
                                  THEN [{x:s1.x-1, Δwater: LEAST(s1.water, s1.ground + s1.water - s1.kinetic - (lag(s1.ground, 1) OVER horizontal + lag(s1.water, 1) OVER horizontal + lag(s1.kinetic, 1) OVER horizontal)) / 4, Δkinetic:0.0},
                                        {x:s1.x  , Δwater:-LEAST(s1.water, s1.ground + s1.water - s1.kinetic - (lag(s1.ground, 1) OVER horizontal + lag(s1.water, 1) OVER horizontal + lag(s1.kinetic, 1) OVER horizontal)) / 4, Δkinetic:0.0},
                                        {x:s1.x-1, Δwater:0.0, Δkinetic:-lag(s1.kinetic, 1) OVER horizontal / 2 - LEAST(s1.water, s1.ground + s1.water - s1.kinetic - (lag(s1.ground, 1) OVER horizontal + lag(s1.water, 1) OVER horizontal + lag(s1.kinetic, 1) OVER horizontal)) / 4}
                                       ]
                                  ELSE []
                                  END
                                  ||
                                  -- flow to the right
                                  CASE WHEN s1.ground + s1.water + s1.kinetic > lead(s1.ground, 1) OVER horizontal + lead(s1.water, 1) OVER horizontal - lead(s1.kinetic, 1) OVER horizontal
                                  THEN [{x:s1.x+1, Δwater: LEAST(s1.water, s1.ground + s1.water + s1.kinetic - (lead(s1.ground, 1) OVER horizontal + lead(s1.water, 1) OVER horizontal - lead(s1.kinetic, 1) OVER horizontal)) / 4, Δkinetic:0.0},
                                        {x:s1.x  , Δwater:-LEAST(s1.water, s1.ground + s1.water + s1.kinetic - (lead(s1.ground, 1) OVER horizontal + lead(s1.water, 1) OVER horizontal - lead(s1.kinetic, 1) OVER horizontal)) / 4, Δkinetic:0.0},
                                        {x:s1.x+1, Δwater:0.0, Δkinetic:-lead(s1.kinetic, 1) OVER horizontal / 2 + LEAST(s1.water, s1.ground + s1.water + s1.kinetic - (lead(s1.ground, 1) OVER horizontal + lead(s1.water, 1) OVER horizontal - lead(s1.kinetic, 1) OVER horizontal)) / 4}
                                       ]
                                  ELSE []
                                  END
                                 ) AS influence
                          FROM   sim AS s1
                          -- window that allows us to inspect cells in the horizontal neighborhood
                          --     ┌─────────────┴─────────────┐
                          WINDOW horizontal AS (ORDER BY s1.x)
                         ) AS inf(influence),
                          --   turn array into table of (x,Δwater,Δkinetic) influence entries
                          --     ┌───────────────┴────────────────┐
                         LATERAL unnest(inf.influence) AS infs(xwk)
                  GROUP BY infs.xwk.x
                  ) AS agg(x, Δwater, Δkinetic)
                  -- find those influences that relate to current cell s0
                  -- ┌─────┴──────┐
                  ON (s0.x = agg.x)
  WHERE  s0.iter < N()
)
SELECT s.iter, s.x, s.ground, s.water
FROM   sim AS s
ORDER BY s.iter, s.x;

-- TABLE simulation
-- ORDER BY iter, x
-- LIMIT 100;

-- Export table simulation in CSV format for rendering in the terminal
-- (see Python program fluid-1D.py)
COPY simulation TO '/dev/null' (FORMAT csv, HEADER false);
