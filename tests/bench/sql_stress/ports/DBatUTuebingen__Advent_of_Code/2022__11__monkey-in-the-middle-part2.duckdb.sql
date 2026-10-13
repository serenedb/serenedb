SET integer_division = true;
-- AoC 2022, Day 11 (Part 2)

CREATE MACRO rounds() AS 10000;

CREATE MACRO throws(m) AS
  m.turn % m.N = m.monkey;
CREATE MACRO round(m) AS
  m.turn / m.N;




WITH RECURSIVE
monkeys(N, monkey, items, op, arg, div, t, f, crt) AS (
  SELECT COUNT(*) OVER () AS N, ROW_NUMBER() OVER () - 1 AS monkey,
         i.items :: int8[], i.op, i.arg, i.div, i.t, i.f,
         -- crt ≡ product of *prime* divisors div
         -- (based on the Chinese Remainder Theorem)
         PRODUCT(i.div) OVER () AS crt
  FROM   input AS i(items, op, arg, div, t, f)
),
middle(turn, N, monkey, items, op, arg, div, t, f, crt) AS (
  SELECT 0 AS turn, m.*
  FROM   monkeys AS m

    UNION ALL

  SELECT CASE WHEN throws(m) AND m.items = [] OR c.catches IS NULL
              THEN m.turn + 1
              ELSE m.turn
         END AS turn,
         m.N, m.monkey,
         CASE WHEN throws(m)            THEN array_pop_front(m.items)
              WHEN m.monkey = c.catches THEN array_push_back(m.items, c.item)
              ELSE m.items
         END AS items,
         m.op, m.arg, m.div, m.t, m.f, m.crt
  FROM   middle AS m
           LEFT JOIN
         (SELECT CASE WHEN m.op = '+' THEN m.items[1] + COALESCE(m.arg, m.items[1])
                      WHEN m.op = '*' THEN m.items[1] * COALESCE(m.arg, m.items[1])
                 END % m.crt AS item,  -- ⚠️ changed for Part 2 (Part 1: / 3)
                 CASE WHEN item % m.div = 0
                      THEN m.t
                      ELSE m.f
                 END AS catches
          FROM   middle AS m
          WHERE  throws(m) AND m.items <> []) AS c(item, catches)
           ON true
  WHERE  round(m) < rounds()
),
inspected(monkey, times) AS (
  SELECT m.monkey, COUNT(*) FILTER (WHERE m.items <> []) AS times
  FROM   middle AS m
  WHERE  throws(m) AND round(m) < rounds()
  GROUP BY m.monkey
  ORDER BY times DESC
  LIMIT 2
)
SELECT PRODUCT(i.times) :: int8 AS monkey_business
FROM   inspected AS i;
