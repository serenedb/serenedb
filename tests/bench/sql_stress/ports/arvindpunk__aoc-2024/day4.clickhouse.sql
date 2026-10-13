
-- part a
WITH solution AS (
    SELECT
        list(string_split(line, NULL)) AS grid,
        count() :: BIGINT AS "rows",
        length(grid [1]) :: BIGINT AS "cols",
        list_transform(
            grid, (curr_row, x) -> list_transform(
                curr_row, (el, y) -> IF(
                    grid [ x ] [ y ] = 'X',
                    (
                        (
                            y + 3 <= "cols"
                            AND grid [ x + 0 ] [ y + 1 ] = 'M'
                            AND grid [ x + 0 ] [ y + 2 ] = 'A'
                            AND grid [ x + 0 ] [ y + 3 ] = 'S'
                        )::INT + (
                            x + 3 <= "rows"
                            AND y + 3 <= "cols"
                            AND grid [ x + 1 ] [ y + 1 ] = 'M'
                            AND grid [ x + 2 ] [ y + 2 ] = 'A'
                            AND grid [ x + 3 ] [ y + 3 ] = 'S'
                        )::INT + (
                            x + 3 <= "rows"
                            AND grid [ x + 1 ] [ y + 0 ] = 'M'
                            AND grid [ x + 2 ] [ y + 0 ] = 'A'
                            AND grid [ x + 3 ] [ y + 0 ] = 'S'
                        )::INT + (
                            x + 3 <= "rows"
                            AND y - 3 >= 1
                            AND grid [ x + 1 ] [ y - 1 ] = 'M'
                            AND grid [ x + 2 ] [ y - 2 ] = 'A'
                            AND grid [ x + 3 ] [ y - 3 ] = 'S'
                        )::INT + (
                            y - 3 >= 1
                            AND grid [ x - 0 ] [ y - 1 ] = 'M'
                            AND grid [ x - 0 ] [ y - 2 ] = 'A'
                            AND grid [ x - 0 ] [ y - 3 ] = 'S'
                        )::INT + (
                            x - 3 >= 1
                            AND y - 3 >= 1
                            AND grid [ x - 1 ] [ y - 1 ] = 'M'
                            AND grid [ x - 2 ] [ y - 2 ] = 'A'
                            AND grid [ x - 3 ] [ y - 3 ] = 'S'
                        )::INT + (
                            x - 3 >= 1
                            AND grid [ x - 1 ] [ y - 0 ] = 'M'
                            AND grid [ x - 2 ] [ y - 0 ] = 'A'
                            AND grid [ x - 3 ] [ y - 0 ] = 'S'
                        )::INT + (
                            x - 3 >= 1
                            AND y + 3 <= "cols"
                            AND grid [ x - 1 ] [ y + 1 ] = 'M'
                            AND grid [ x - 2 ] [ y + 2 ] = 'A'
                            AND grid [ x - 3 ] [ y + 3 ] = 'S'
                        )::INT
                    ) :: BIGINT,
                    0 :: BIGINT
                )
            )
        ) AS ct_grid,
        list_aggregate(
            list_transform(ct_grid, xs -> list_aggregate(xs, 'sum')),
            'sum'
        ) AS result,
        1
    FROM
        "table"
)
SELECT
    result
FROM
    solution;

-- part b
WITH solution AS (
    SELECT
        list(string_split(line, NULL)) AS grid,
        count() :: BIGINT AS "rows",
        length(grid [1]) :: BIGINT AS "cols",
        list_transform(
            grid, (curr_row, x) -> list_transform(
                curr_row, (el, y) -> IF(
                    grid [ x ] [ y ] = 'A'
                    AND x > 1
                    AND x < "rows"
                    AND y > 1
                    AND y < "cols",
                    (
                        (
                            grid [ x - 1 ] [ y - 1 ] = 'M'
                            AND grid [ x + 1 ] [ y + 1 ] = 'S'
                            AND (
                                (
                                    grid [ x - 1 ] [ y + 1 ] = 'M'
                                    AND grid [ x + 1 ] [ y - 1 ] = 'S'
                                )
                                OR (
                                    grid [ x - 1 ] [ y + 1 ] = 'S'
                                    AND grid [ x + 1 ] [ y - 1 ] = 'M'
                                )
                            )
                        )
                        OR (
                            grid [ x - 1 ] [ y - 1 ] = 'S'
                            AND grid [ x + 1 ] [ y + 1 ] = 'M'
                            AND (
                                (
                                    grid [ x - 1 ] [ y + 1 ] = 'M'
                                    AND grid [ x + 1 ] [ y - 1 ] = 'S'
                                )
                                OR (
                                    grid [ x - 1 ] [ y + 1 ] = 'S'
                                    AND grid [ x + 1 ] [ y - 1 ] = 'M'
                                )
                            )
                        )
                    ) :: BIGINT,
                    0 :: BIGINT
                )
            )
        ) AS ct_grid,
        list_aggregate(
            list_transform(ct_grid, xs -> list_aggregate(xs, 'sum')),
            'sum'
        ) AS result,
        1
    FROM
        "table"
)
SELECT
    result
FROM
    solution;