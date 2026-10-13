CREATE OR REPLACE MACRO toInt64(x) AS CAST(trunc(CAST(x AS DOUBLE)) AS BIGINT);
CREATE OR REPLACE MACRO arraySort(a) AS list_sort(a);
CREATE OR REPLACE MACRO extractAll(s, re) AS regexp_extract_all(s, re);
CREATE OR REPLACE MACRO mapAdd(a, b) AS map_from_entries(list_transform(list_distinct(list_concat(map_keys(a), map_keys(b))), k -> (k, coalesce(a[k], 0) + coalesce(b[k], 0))));
--part a
SELECT
    list_sum(
        list_transform(
            list_zip(
                (
                    SELECT
                        arraySort(
                            list(toInt64(extractAll(line, '[0-9]+') [1]))
                        )
                    FROM
                        "table"
                ),
                (
                    SELECT
                        arraySort(
                            list(toInt64(extractAll(line, '[0-9]+') [2]))
                        )
                    FROM
                        "table"
                )
            ),
            t -> abs(t[1] - t[2])
        )
    );

-- part b
SELECT
    sum(similarity)
FROM
    (
        WITH frequency_map AS (
            SELECT
                list_reduce(
                    list(
                        map([
                            toInt64(extractAll(line, '[0-9]+') [2])],
                            [toInt64(1)]
                        )
                    ),
                    (acc, curr) -> mapAdd(acc, curr),
                    map([]::BIGINT[], []::BIGINT[])
                )
            FROM
                "table"
        )
        SELECT
            toInt64(extractAll(line, '[0-9]+') [1]) AS "left",
            "left" * coalesce((
                SELECT
                    *
                FROM
                    frequency_map
            ) ["left"], 0) AS similarity
        FROM
            "table"
    )