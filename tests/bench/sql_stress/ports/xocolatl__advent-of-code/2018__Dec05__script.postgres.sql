\set ON_ERROR_STOP true

CREATE TABLE dec05 (
    rownum bigserial,
    input text
);

/* Import the input file.  We're using perl to remove the final newline. */
\COPY dec05 (input) FROM PROGRAM 'perl -pe ''chomp if eof'' data/2018/Dec05/input.dat'
VACUUM ANALYZE dec05;


/* Part One */
WITH RECURSIVE
work (l, u1, u2, r) as (
    SELECT '',
           substring(input FOR 1),
           substring(input FROM 2 FOR 1),
           substring(input FROM 3)
    FROM dec05
    UNION ALL
    SELECT CASE WHEN lower(u1) = lower(u2) AND u1 <> u2 THEN substring(l FROM 2) ELSE u1 || l END,
           CASE WHEN lower(u1) = lower(u2) AND u1 <> u2 THEN substring(l FOR 1) ELSE u2 END,
           substring(r FOR 1),
           substring(r FROM 2)
    FROM work
    WHERE (u1, u2) <> ('', '')
)
SELECT length(l)
FROM work
WHERE (u1, u2) = ('', '');

/* Part Two */
SELECT min((
    /* This is just Part One */
    WITH RECURSIVE
    work (l, u1, u2, r) as (
        SELECT '',
               substring(input FOR 1),
               substring(input FROM 2 FOR 1),
               substring(input FROM 3)
        FROM LATERAL (SELECT regexp_replace(dec05.input, u.l, '', 'ig')) AS r (input)
        UNION ALL
        SELECT CASE WHEN lower(u1) = lower(u2) AND u1 <> u2 THEN substring(l FROM 2) ELSE u1 || l END,
               CASE WHEN lower(u1) = lower(u2) AND u1 <> u2 THEN substring(l FOR 1) ELSE u2 END,
               substring(r FOR 1),
               substring(r FROM 2)
        FROM work
        WHERE (u1, u2) <> ('', '')
    )
    SELECT length(l)
    FROM work
    WHERE (u1, u2) = ('', '')
))
FROM dec05,
     LATERAL (SELECT DISTINCT lower(l) AS l FROM unnest(string_to_array(input, null)) AS u(l)) u(l);
