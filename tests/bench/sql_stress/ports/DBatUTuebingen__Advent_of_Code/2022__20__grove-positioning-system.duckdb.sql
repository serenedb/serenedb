SET integer_division = true;
-- AoC 2022, Day 20 (Parts 1 & 2)

-- Part 1 vs Part 2 (part1.sql, part2.sql):
-- configure # of mixing rounds and decryption key (rounds(), key())
-- Part 2
CREATE MACRO rounds() AS 10;
CREATE MACRO key()    AS 811589153 :: bigint;


-- like a % b (but works for a < 0)
CREATE MACRO modulo(a,b) AS
  ((a) % (b) + (b)) % (b);

--  number num at position pos in a file of given size moves here
CREATE MACRO move(num,pos,size) AS
  -1 + modulo(pos + modulo(num, size - 1) + 1.5, size);


WITH RECURSIVE
input(pos, num) AS (
  SELECT ROW_NUMBER() OVER () - 1 AS pos, c.num * key()
  FROM   read_csv_auto('2022/20/input.txt') AS c(num)
),
file(size) AS (
  SELECT COUNT(*) AS size
  FROM   input
),
mix(n, size, pos, num, loc) AS (
  SELECT 0 AS n, f.size, i.pos, i.num, i.pos AS loc
  FROM   input AS i, file AS f

    UNION ALL

  SELECT m.n + 1 AS n, m.size, m.pos, m.num,
         ROW_NUMBER() OVER (ORDER BY CASE WHEN m.pos = modulo(m.n, size)
                                          THEN move(m.num, m.loc, m.size)
                                          ELSE m.loc
                                     END) - 1  AS loc
  FROM   mix AS m
  WHERE  m.n < m.size * rounds()
),
decrypted(loc, num) AS (
  SELECT m.loc, m.num
  FROM   mix AS m
  WHERE  m.n = m.size * rounds()
)
SELECT SUM(d.num) AS grove
FROM   decrypted AS zero, decrypted AS d, file AS f,
       (VALUES (1000), (2000), (3000)) AS _(offs)
WHERE  zero.num = 0 AND d.loc = modulo(zero.loc + offs, f.size);

