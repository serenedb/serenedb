CREATE OR REPLACE MACRO ch_numbers() AS TABLE SELECT range AS number FROM range(9223372036854775807);
CREATE OR REPLACE MACRO width() AS CAST(160 AS UINTEGER);
CREATE OR REPLACE MACRO height() AS CAST(120 AS UINTEGER);
CREATE OR REPLACE MACRO iTime() AS CAST(1.0 AS DOUBLE);
-- Basic Template
-- A simple starter template for creating new shaders

SELECT
    -- Colors are calculated based on time and pixel coordinates.
    -- The combination of sin and cos creates smooth, cycling color patterns.
    (0.5 + (0.5 * cos(iTime() + (x / width() * 5)))) AS r,
    (0.5 + (0.5 * sin(iTime() + (y / height() * 5)))) AS g,
    (0.5 + (0.5 * cos(iTime() + 0.5))) AS b
FROM
(
    -- Generate a grid of pixels using system.numbers and an explicit CROSS JOIN
    SELECT number AS y
    FROM ch_numbers() LIMIT height()
) AS t_y
CROSS JOIN (SELECT number AS x FROM ch_numbers() LIMIT width()) AS t_x
ORDER BY
    y ASC,
    x ASC