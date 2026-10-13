SET integer_division = true;
-- AoC 2022, Day 17 (Part 1)

-- # of rocks to drop
CREATE MACRO rocks() AS 2022;
-- AoC input file
CREATE MACRO input() AS '2022/17/input.txt';

-- simulation of pyroclastic flow (# of iterations is rocks())
-- AoC 2022, Day 17 (code common to Parts 1 & 2)
--
-- • Include this via .read pyroclastic-flow.sql
-- • Assumes macros input() and rocks() being defined


-- convert '..##..' strings into bit sequence
CREATE MACRO bits(s) AS
  list_sum(list_apply(range(length(s)),
                      b -> (reverse(s)[b+1] = '#') :: int * 1 << b)) :: int;

-- push rock left in chamber (don't move if collision)
CREATE MACRO push_left(rock, chamber) AS
  CASE WHEN list_bit_or(list_apply(generate_series(1, len(rock)),
                                   y -> rock[y] << 1 & chamber[y]))
       THEN rock
       ELSE list_apply(rock, r -> r << 1)
  END;

-- push rock right in chamber (don't move if collision)
CREATE MACRO push_right(rock, chamber) AS
  CASE WHEN list_bit_or(list_apply(generate_series(1, len(rock)),
                                   y -> rock[y] >> 1 & chamber[y]))
       THEN rock
       ELSE list_apply(rock, r -> r >> 1)
  END;

-- do rock and chamber collide?
CREATE MACRO collide(rock, chamber) AS
  list_bit_or(list_apply(generate_series(1, len(rock)),
                         y -> rock[y] & chamber[y]));

-- merge rock and chamber
CREATE MACRO merge(rock, chamber) AS
  list_apply(generate_series(1, len(rock)),
             y -> rock[y] | chamber[y]);

-- debugging: FROM draw(‹array of bits›)
CREATE MACRO draw(bits) AS TABLE
  SELECT list_aggr(list_apply(generate_series(8,0,-1),
                              b -> '#.'[1 + (bs & (1 << b) = 0) :: int]),
                   'string_agg', '')
  FROM   (SELECT unnest(bits)) AS _(bs);

-- five kinds of falling rocks
CREATE TABLE rocks AS
  SELECT *
  FROM   (VALUES (0, [bits('...####..')]),

                 (1, [bits('....#....'),
                      bits('...###...'),
                      bits('....#....')]),

                 (2, [bits('.....#...'),
                      bits('.....#...'),
                      bits('...###...')]),

                 (3, [bits('...#.....'),
                      bits('...#.....'),
                      bits('...#.....'),
                      bits('...#.....')]),

                 (4, [bits('...##....'),
                      bits('...##....')])) AS _(id, bits);

-- input: jet patterns (<<><><>...)
CREATE TABLE jets AS
  SELECT ROW_NUMBER () OVER () - 1 AS id, j.jet
  FROM   (SELECT unnest(string_split(c.jets, NULL)) AS jet
          FROM   read_csv_auto(input(), header=false) AS c(jets)) AS j;


-- simulate the pyroclastic flow
CREATE TABLE pyroclastic AS
  WITH RECURSIVE
  pyroclastic(flow) AS (
    SELECT {shape:   1,  -- next rock shape
            jet:     0,  -- next jet
            jets:    (SELECT COUNT(*) FROM jets),
            rock:    (SELECT r.bits FROM rocks AS r WHERE r.id = 0),
            y:       1,
            chamber: [bits('#.......#'),  --   ← row of rock 0
                      bits('#.......#'),  -- 1
                      bits('#.......#'),  -- 2 ← empty rows above ground
                      bits('#.......#'),  -- 3
                      bits('#########')], --   ← ground
           } AS flow

      UNION ALL

    SELECT CASE WHEN collide(pushed_rock, p.flow.chamber[p.flow.y + 1:]) -- collision if rock drops down?
                THEN {shape:   p.flow.shape + 1,
                      jet:     (p.flow.jet + 1) % p.flow.jets,
                      jets:    p.flow.jets,
                      rock:    next_rock,
                      y:       1,
                      chamber: [ bits('#.......#') for r in next_rock || [1,2,3] ]                    || -- empty space for next rock + 3 empty rows
                               [ r for r in p.flow.chamber[1:p.flow.y - 1] if r > bits('#.......#') ] || -- chamber above stopped rock
                               merge(pushed_rock, p.flow.chamber[p.flow.y:])                          || -- stopped rock in chamber
                               p.flow.chamber[p.flow.y + len(pushed_rock):]}                             -- chamber below stoppped rock
                ELSE {shape:   p.flow.shape,
                      jet:     (p.flow.jet + 1) % p.flow.jets,
                      jets:    p.flow.jets,
                      rock:    pushed_rock,
                      y:       p.flow.y + 1,
                      chamber: p.flow.chamber}
           END AS flow
    FROM   (SELECT p.flow,
                   CASE j.jet WHEN '<' THEN push_left( p.flow.rock, p.flow.chamber[p.flow.y:])
                              WHEN '>' THEN push_right(p.flow.rock, p.flow.chamber[p.flow.y:])
                   END AS pushed_rock,
                   r.bits AS next_rock
            FROM   pyroclastic AS p, jets AS j, rocks AS r
            WHERE  p.flow.jet       = j.id
            AND    p.flow.shape % 5 = r.id) AS p
    WHERE  p.flow.shape <= rocks()
  )
  TABLE pyroclastic;

-- height of tower in chamber
SELECT len([1 for r in p.flow.chamber if r > bits('#.......#')]) - 1 AS tower
FROM   pyroclastic AS p
WHERE  p.flow.shape > rocks();

-- debugging (chamber output)
--
-- SELECT  p.flow.shape, p.flow.jet, c AS chamber
-- FROM    pyroclastic AS p,
-- LATERAL (FROM draw(p.flow.chamber)) AS _(c);
