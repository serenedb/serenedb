SET integer_division = true;
-- AoC 2022, Day 21 

CREATE TABLE raw ( i int PRIMARY KEY, line text NOT NULL );

INSERT INTO raw
SELECT ROW_NUMBER() OVER () AS i, line 
FROM   read_csv_auto('2022/24/Chris/simple.txt', header=false) AS _(line);

-- Holds exactly one row that describes the dimensions of the valley.
-- (1,1) to (x,y)
CREATE TABLE dimensions ( x int, y int, lcm int );

-- Least common multiple
CREATE MACRO lcm(a,b) AS (
  WITH RECURSIVE 
  gcd(curr,next) AS (
    SELECT a, b
      UNION ALL 
    SELECT gcd.next, gcd.curr % gcd.next
    FROM   gcd
    WHERE  gcd.next <> 0
  )
  SELECT a*b/gcd.curr
  FROM   gcd 
  WHERE  gcd.next = 0
);

INSERT INTO dimensions 
SELECT MAX(length(r.line)-2), MAX(r.i-2), lcm((SELECT MAX(length(r.line)-2) FROM raw AS r), (SELECT MAX(r.i-2) FROM raw AS r))
FROM   raw AS r;

CREATE TYPE dir AS ENUM ('<','^','>','v');

CREATE TABLE blizzards ( step int, x int, y int );

CREATE UNIQUE INDEX pos_idx ON blizzards (step,x,y);

-- Produce all positions of the blizards in every step
-- There exist exactly lcm(width,height) many arrangements
INSERT INTO blizzards
WITH RECURSIVE 
steps(step,dir,x,y) AS (
  SELECT  0, arr[x] :: dir, x-1, r.i-1
  FROM    raw AS r,
  LATERAL (SELECT string_to_array(r.line, NULL)) AS _(arr),
  LATERAL (SELECT generate_subscripts(arr,1))  AS __(x)
  WHERE   arr[x] IN ('<','^','>','v')
    UNION ALL 
  SELECT s.step+1, 
         s.dir, 
         CASE s.dir 
            WHEN '<' THEN 
              CASE WHEN s.x = 1 THEN d.x ELSE s.x-1 END
            WHEN '>' THEN 
              CASE WHEN s.x = d.x THEN 1 ELSE s.x+1 END
            ELSE s.x
          END,
          CASE s.dir 
            WHEN '^' THEN 
              CASE WHEN s.y = 1 THEN d.y ELSE s.y-1 END
            WHEN 'v' THEN 
              CASE WHEN s.y = d.y THEN 1 ELSE s.y+1 END
            ELSE s.y
         END 
  FROM   steps      AS s, 
         dimensions AS d 
  WHERE  s.step+1 < d.lcm 
)
SELECT DISTINCT s.step, s.x, s.y FROM steps AS s;

-- Shortest path from (1,0) to (x,y+1)
CREATE MACRO shortest_path(sx,sy,sstep,tx,ty) AS TABLE (
  WITH RECURSIVE 
  astar(iter, visited) AS (
  SELECT 1, [[NULL :: int, NULL :: int, sx, sy, sstep]]
    UNION ALL (
  WITH
  visited(px, py, x, y, step) AS (
    SELECT arr[1], arr[2], arr[3], arr[4], arr[5] 
    FROM   astar AS a, unnest(a.visited) AS _(arr)
  )
  SELECT a.iter+1, array_append(a.visited, (
  SELECT [v.x, v.y, v.x+Δ.x, v.y+Δ.y, v.step+1]
  FROM   (VALUES (-1,0),(0,-1),(1,0),(0,1),(0,0)) AS Δ(x,y), 
         visited    AS v,
         dimensions AS d
  WHERE  ((v.x+Δ.x,v.y+Δ.y) = (tx,ty) OR (v.x+Δ.x BETWEEN 1 AND d.x AND v.y+Δ.y BETWEEN 1 AND d.y))
  AND    NOT EXISTS (SELECT 1 FROM visited   AS v_ WHERE (v_.x, v_.y, v_.step) = (v.x+Δ.x, v.y+Δ.y,  v.step+1       ))
  AND    NOT EXISTS (SELECT 1 FROM blizzards AS b  WHERE (b.x , b.y , b.step ) = (v.x+Δ.x, v.y+Δ.y, (v.step+1)%d.lcm))
  ORDER BY v.step + 1 + ABS(tx-(v.x+Δ.x)) + ABS(ty-(v.y+Δ.y))
  --         Steps    +     Heuristic (Manhattan-Distance)
  lIMIT 1))
  FROM    astar AS a
  WHERE   NOT EXISTS (SELECT 1 FROM visited AS v JOIN dimensions AS d ON (v.x,v.y) = (tx,ty))
  ))
  SELECT arr[3] AS x, arr[4] AS y, arr[5] AS step
  FROM   astar             AS a, 
         unnest(a.visited) AS _(arr),
         dimensions        AS d
  WHERE  (arr[3],arr[4]) = (tx,ty)
  AND    a.iter = (SELECT MAX(a.iter) FROM astar AS a)
);

CREATE TABLE walks AS (
  SELECT s.x, s.y, s.step
  FROM   dimensions AS d, 
         shortest_path(1,0,0,d.x,d.y+1) AS s
);

SELECT w.step "Day 21 (part one)"
FROM   walks AS w;