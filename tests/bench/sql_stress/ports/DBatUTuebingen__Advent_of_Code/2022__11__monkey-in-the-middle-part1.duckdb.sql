SET integer_division = true;
-- AoC 2022, Day 11 (Part 1)

CREATE MACRO rounds() AS 20;

CREATE MACRO throws(m) AS
  m.turn % m.N = m.monkey;
CREATE MACRO round(m) AS
  m.turn / m.N;




WITH RECURSIVE
monkeys(N, monkey, items, op, arg, div, t, f) AS (
  SELECT COUNT(*) OVER () AS N, ROW_NUMBER() OVER () - 1 AS monkey, i.*
  FROM   input AS i
),
middle(turn, N, monkey, items, op, arg, div, t, f) AS (
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
         m.op, m.arg, m.div, m.t, m.f
  FROM   middle AS m
           LEFT JOIN
         (SELECT CASE WHEN m.op = '+' THEN m.items[1] + COALESCE(m.arg, m.items[1])
                      WHEN m.op = '*' THEN m.items[1] * COALESCE(m.arg, m.items[1])
                 END / 3 AS item,
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
SELECT PRODUCT(i.times) :: int AS monkey_business
FROM   inspected AS i;

-- SELECT round(m) AS round, throws(m), m.monkey, m.turn, m.items
-- FROM   middle AS m
-- WHERE  round(m) = 20;



-- Plan:
-- - store monkeys in a table, one row per monkey (here: N = 8 rows)
-- - CTE working table columns: turn | monkey | items | ...,
--   cardinality of WT in each iteration is N
-- - increment column turn (see below), monkey turn % N is active
--   (later, can use column turn to identify round ≡ turn / N)
-- - in each iteration:
--   - in a subquery of q⭯, identify active monkey
--   - perform computation for that monkey,
--     subquery yields pair: monkey-thrown-to | thrown-item
--   - top-level query left-joined to subquery
--     (subquery may be empty if items list of active monkey is empty)
--   - in top-level query of q⭯, iterate over all monkeys m:
--     - turn:  if (m is active monkey and item list empty) or monkey-thrown-to is NULL,
--              increment turn, otherwise unchanged
--     - items: if m is active monkey, remove first item in items list
--              if m is monkey-thrown-to, add thrown-item to back of items list
--              otherwise, items unchanged
--     - all other columns unchanged