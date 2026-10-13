CREATE OR REPLACE MACRO toInt64(x) AS CAST(trunc(CAST(x AS DOUBLE)) AS BIGINT);
CREATE OR REPLACE MACRO toString(x) AS CAST(x AS VARCHAR);
CREATE OR REPLACE MACRO intDiv(a, b) AS a // b;
CREATE OR REPLACE MACRO exp10(x) AS pow(10, x);
CREATE OR REPLACE MACRO splitByChar(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO countDigits(x) AS length(CAST(abs(x) AS VARCHAR));
--part a
SELECT
    sum(
        list_sum(
            list_transform(
                "range"(invalid_lt_a, invalid_lte_b + 1),
                x -> x * (toInt64(exp10(countDigits(x))) + 1)
            )
        )
    )
FROM
    (
        SELECT
            list_transform(
                splitByChar('-', unnest(splitByChar(',', line))),
                x -> toInt64(x)
            ) AS query,
            IF(
                countDigits(query [1]) % 2 = 0,
                -- even digits (56, 1234...)
                query [1],
                -- odd digits (8, 673, 12334...)
                toInt64(exp10(countDigits(query [1])))
            ) AS a,
            IF(
                countDigits(query [2]) % 2 = 0,
                -- even digits (56, 1234...)
                query [2],
                -- odd digits (8, 673, 12334...)
                toInt64(exp10(countDigits(query [2]) - 1)) - 1
            ) AS b,
            intDiv(a, toInt64(exp10(countDigits(a) / 2))) AS a_first_half,
            a % toInt64(exp10(countDigits(a) / 2)) AS a_sec_half,
            intDiv(b, toInt64(exp10(countDigits(b) / 2))) AS b_first_half,
            b % toInt64(exp10(countDigits(b) / 2)) AS b_sec_half,
            a_first_half + 1 - (a_first_half >= a_sec_half)::INT AS invalid_lt_a,
            b_first_half - 1 + (b_first_half <= b_sec_half)::INT AS invalid_lte_b
        FROM
            "table"
    );

--part b
SELECT
    sum(x) FILTER (WHERE
        list_sum(
            list_transform(
                "range"(1, 1 + intDiv(countDigits(x), 2)),
                len -> (toInt64(
                    repeat(
                        substring(toString(x), 1, len),
                        intDiv(countDigits(x), len)
                    )
                ) = x)::INT
            )
        ) > 0
    )
FROM
    (
        SELECT
            unnest("range"(a, b)) AS x
        FROM
            (
                SELECT
                    list_transform(
                        splitByChar('-', unnest(splitByChar(',', line))),
                        x -> toInt64(x)
                    ) AS query,
                    query [1] AS a,
                    query [2] AS b
                FROM
                    "table"
            )
    );