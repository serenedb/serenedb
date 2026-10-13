\set ON_ERROR_STOP true
CREATE TABLE dec01 (rownum bigserial, input integer);

/* Import the input file.  We're using perl to remove the final newline. */
\COPY dec01 (input) FROM PROGRAM 'perl -pe ''chomp if eof'' data/2018/Dec01/input.dat'


/* Part One is trivial in SQL */
SELECT sum(input)
FROM dec01;

/*
 * Part Two is a little bit more involved.
 *
 * Instead of looping over everything, we can just use modular arithmetic.
 * Each iteration in the looping solution will add the sum found in Part One to
 * each value.  We can just look for pairs of sums that are multiples of that
 * apart.  Then we take the one with the fewest of those multiples.  Then we
 * just need to make sure we find the first one in the list with that property.
 */
WITH
sums (rownum, sum) AS (
    SELECT rownum,
           sum(input) OVER (ORDER BY rownum)
    FROM dec01
)
SELECT a.sum
FROM sums AS a
JOIN sums AS b
    ON a.rownum <> b.rownum
   AND a.sum > b.sum
   AND (a.sum - b.sum) % (SELECT sum(input) FROM dec01) = 0
ORDER BY a.sum - b.sum, b.rownum
LIMIT 1;
