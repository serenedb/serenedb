CREATE OR REPLACE MACRO toInt64(x) AS CAST(trunc(CAST(x AS DOUBLE)) AS BIGINT);
CREATE OR REPLACE MACRO intDiv(a, b) AS a // b;
CREATE OR REPLACE MACRO replaceOne(s, a, b) AS CASE WHEN strpos(s, a) > 0 THEN substr(s, 1, strpos(s, a) - 1) || b || substr(s, strpos(s, a) + length(a)) ELSE s END;
--part a
SELECT
    list_reduce(
        list(
            toInt64(replaceOne(replaceOne(line, 'L', '-'), 'R', ''))
        ),
        (acc, el) -> (
            (acc[1] + el + 100 * 1000000) % 100 :: BIGINT,
            acc[2] + ((acc[1] + el + 100 * 1000000) % 100 = 0)::INT
        ),
        (50 :: BIGINT, 0 :: UBIGINT)
    )[2]
FROM
    "table";

--part b
SELECT
    list_reduce(
        list(
            toInt64(replaceOne(replaceOne(line, 'L', '-'), 'R', ''))
        ),
        (acc, el) -> (
            (acc[1] + el + 100 * 1000000) % 100 :: BIGINT,
            acc[2] + CASE
                WHEN acc[1] + el >= 100 THEN intDiv(acc[1] + el, 100)
                WHEN acc[1] + el <= 0 THEN IF(acc[1] = 0, 0, 1) + intDiv(-1 * (acc[1] + el), 100)
                ELSE 0
            END
        ),
        (50 :: BIGINT, 0 :: BIGINT)
    )[2]
FROM
    "table";