SET integer_division = true;

CREATE MACRO compare(fst, snd) AS (
    --FIXME: put code to compare two lists (from part 1) here
    SELECT true
);

-- read csv
WITH RECURSIVE input(row, signal) AS (
     SELECT ROW_NUMBER() OVER () AS row, s AS signal
     FROM read_csv('2022/13/input.txt',
                   delim='|', header=False, columns={'signal': 'char[]'}) AS c(s)
),
-- parse input into sensible schema
data(idx, signal) AS (
    SELECT ROW_NUMBER() OVER(ORDER BY row) AS idx, signal AS signal
    FROM input
    WHERE signal IS NOT NULL
)
SELECT x, y
FROM (SELECT COUNT(x)
      FROM (SELECT compare(signal, '[[2]]'::char[])
            FROM data) AS _(x)
      WHERE x) AS __(x),
     (SELECT COUNT(y)
      FROM (SELECT compare(signal, '[[6]]'::char[])
            FROM data) AS __(y)
      WHERE y) AS ____(y)
;