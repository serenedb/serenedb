CREATE OR REPLACE MACRO toInt64(x) AS CAST(trunc(CAST(x AS DOUBLE)) AS BIGINT);
CREATE OR REPLACE MACRO extractAll(s, re) AS regexp_extract_all(s, re);
CREATE OR REPLACE MACRO multiMatchAnyIndex(h, ps) AS coalesce((list_sort(list_filter(list_transform(range(1, len(ps) + 1), i -> [strpos(h, ps[i]) + len(ps[i]) - 1, i]), x -> x[1] >= len(ps[x[2]]))))[1][2], 0);
-- part a
SELECT
    sum(
        toInt64(
            concat(
                extractAll(line, '[0-9]') [1],
                extractAll(line, '[0-9]') [-1]
            )
        )
    )
FROM
    "table";

-- part b
SELECT
    sum(
        multiMatchAnyIndex(
            line,
            ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'zero']
        ) % 10 * 10 + multiMatchAnyIndex(
            reverse(line),
            ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'eno', 'owt', 'eerht', 'ruof', 'evif', 'xis', 'neves', 'thgie', 'enin', 'orez']
        ) % 10
    )
FROM
    "table";