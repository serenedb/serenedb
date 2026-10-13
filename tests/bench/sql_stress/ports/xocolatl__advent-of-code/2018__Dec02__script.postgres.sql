\set ON_ERROR_STOP true

CREATE TABLE dec02 (
    rownum bigserial,
    input text
);

/* Import the input file.  We're using perl to remove the final newline. */
\COPY dec02 (input) FROM PROGRAM 'perl -pe ''chomp if eof'' data/2018/Dec02/input.dat'

/*
 * We need the fuzzystrmatch extension the levenshtein function for Part Two.
 */


/* Part One */
SELECT sum(two) * sum(three)
FROM (            
    SELECT input,
           count(*) FILTER (
               WHERE EXISTS (
                   SELECT 1 FROM unnest(string_to_array(input, null)) AS u(x)
                   GROUP BY u.x
                   HAVING count(*) = 2
               )) AS two,
           count(*) FILTER (
               WHERE EXISTS (
                   SELECT 1 FROM unnest(string_to_array(input, null)) AS u(x)
                   GROUP BY u.x
                   HAVING count(*) = 3
               )) AS three
    FROM dec02
    GROUP BY input
) _;

/*
 * Part Two
 *
 * This assumes that there is only one pair in the input that is off by one.
 */
WITH
inputs (a, b) AS (
    SELECT a.input,
           b.input
    FROM dec02 AS a
    JOIN dec02 AS b ON a.rownum < b.rownum
    WHERE levenshtein(a.input, b.input) = 1
)
SELECT string_agg(a, '' ORDER BY ord)
FROM unnest(string_to_array((SELECT a FROM inputs), null),
            string_to_array((SELECT b FROM inputs), null)
           ) WITH ORDINALITY AS x (a, b, ord)
WHERE a = b;
