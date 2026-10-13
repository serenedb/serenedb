SET integer_division = true;
-- AoC 2022, Day 25 (Part 1)

WITH RECURSIVE
input(row, digit) AS (
  SELECT ROW_NUMBER() OVER () AS row,
         unnest(string_split(reverse(c.s), NULL)) AS digit
  FROM   read_csv_auto('2022/25/input.txt', header=false) AS c(s)
),
dec_to_snafu(snafu, dec, carry) AS (
  VALUES ('-', 4, 1), -- -1
         ('=', 3, 1), -- -2
         ('2', 2, 0), --  2
         ('1', 1, 0), --  1
         ('0', 0, 0)  --  0
),
places(place, digit) AS (
  SELECT ROW_NUMBER() OVER (PARTITION BY i.row) - 1 AS place,
         dts.dec - 5 * dts.carry                    AS digit
  FROM   input AS i, dec_to_snafu AS dts
  WHERE  i.digit = dts.snafu
),
decimal(sum) AS (
  SELECT SUM(p.digit * 5^p.place) :: int128 AS sum
  FROM   places AS p
),
snafu(dec, snafu) AS (
  SELECT d.sum AS dec, '' AS snafu
  FROM   decimal AS d
    UNION ALL
  SELECT s.dec / 5 + dts.carry AS dec, dts.snafu || s.snafu
  FROM   snafu AS s, dec_to_snafu AS dts
  WHERE  s.dec > 0 AND dts.dec = s.dec % 5
)
SELECT s.snafu
FROM   snafu AS s
WHERE  s.dec = 0;
