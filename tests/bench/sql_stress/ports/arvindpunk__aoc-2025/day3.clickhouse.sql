CREATE OR REPLACE MACRO toString(x) AS CAST(x AS VARCHAR);
CREATE OR REPLACE MACRO arrayMax(a) AS list_max(a);
CREATE OR REPLACE MACRO hasSubsequence(h, n) AS regexp_matches(h, array_to_string(list_transform(string_split(n, NULL), c -> regexp_escape(c)), '.*'))::INT;
--part a
SELECT
    sum(
        arrayMax(
            list_transform(
                "range"(10, 100),
                x -> x * hasSubsequence(line, toString(x))
            )
        )
    )
FROM
    "table";

--part b