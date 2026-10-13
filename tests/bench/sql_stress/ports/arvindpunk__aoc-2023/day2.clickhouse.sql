CREATE OR REPLACE MACRO toInt64(x) AS CAST(trunc(CAST(x AS DOUBLE)) AS BIGINT);
CREATE OR REPLACE MACRO max2(a, b) AS greatest(a, b);
CREATE OR REPLACE MACRO arrayProduct(a) AS list_product(a);
CREATE OR REPLACE MACRO splitByChar(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO splitByString(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO extractKeyValuePairs(s, kv, pd) AS list_transform(list_filter(string_split(s, pd), p -> trim(p) <> ''), p -> {'key': split_part(trim(p), kv, 1), 'value': split_part(trim(p), kv, 2)});
-- part a
SELECT
    sum(
        toInt64(splitByChar(' ', splitByChar(':', line) [1]) [2]) * list_bool_and(list_transform(
            list_transform(
                splitByString('; ', trim(splitByChar(':', line) [2])),
                x -> map_from_entries(list_transform(
                    extractKeyValuePairs(x, ' ', ','),
                e -> (e.value, toInt64(e.key))))
            ),
            m -> coalesce(m ['red'], 0) <= 12
            AND coalesce(m ['green'], 0) <= 13
            AND coalesce(m ['blue'], 0) <= 14
        ))::int
    )
FROM
    "table";

-- part b
SELECT
    sum(
        arrayProduct(
            list_reduce(
            list_transform(
                splitByString('; ', trim(splitByChar(':', line) [2])),
                x -> map_from_entries(list_transform(
                    extractKeyValuePairs(x, ' ', ','),
                e -> (e.value, toInt64(e.key))))
            ),
            (acc, m) -> [max2(acc[1], coalesce(m ['red'], 0)),
            max2(acc [2], coalesce(m ['green'], 0)),
            max2(acc [3], coalesce(m ['blue'], 0)) ],
            [0.::DOUBLE, 0.::DOUBLE, 0.::DOUBLE]
        )
    )
)
FROM
    "table";