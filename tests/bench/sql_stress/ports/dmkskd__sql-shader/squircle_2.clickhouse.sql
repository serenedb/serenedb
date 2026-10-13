CREATE OR REPLACE MACRO ch_numbers() AS TABLE SELECT range AS number FROM range(9223372036854775807);
CREATE OR REPLACE MACRO width() AS CAST(160 AS UINTEGER);
CREATE OR REPLACE MACRO height() AS CAST(120 AS UINTEGER);
CREATE OR REPLACE MACRO iTime() AS CAST(1.0 AS DOUBLE);
-- "Squircle 2" by @XorDev - Converted to ClickHouse SQL
-- https://www.shadertoy.com/view/WfySDD
-- A lightweight shader with rotating squircle distance field and colorful waves
-- @run: resolution=640x480, zoom=1

SELECT
    -- Tonemap with tanh and calculate final RGB colors
    tanh((0.6 + 0.5 * w_r) / greatest(l / 0.2, -l)) AS r,
    tanh((0.6 + 0.5 * w_g) / greatest(l / 0.2, -l)) AS g,
    tanh((0.6 + 0.5 * w_b) / greatest(l / 0.2, -l)) AS b
FROM
(
    SELECT
        y,
        x,
        -- Distance field: sqrt of sum of squares (length of c*c in GLSL means component-wise square then length)
        cos(sqrt(pow(c_x, 4) + pow(c_y, 4)) / 0.1 - iTime()) AS l,
        -- Color wave components - using length(c) not length(c*c)
        sin(3.0 * sqrt(c_x * c_x + c_y * c_y) + 2.0 * c_y + iTime() + 6.0) AS w_r,
        sin(3.0 * sqrt(c_x * c_x + c_y * c_y) + 2.0 * c_y + iTime() + 1.0) AS w_g,
        sin(3.0 * sqrt(c_x * c_x + c_y * c_y) + 2.0 * c_y + iTime() + 2.0) AS w_b
    FROM
    (
        SELECT
            y,
            x,
            -- Rotate coordinates using the matrix from cos with offsets
            -- Matrix is: [cos(angle+0), cos(angle+11); cos(angle+33), cos(angle+0)]
            -- Which approximates: [cos(angle), -sin(angle); sin(angle), cos(angle)]
            p_x * cos(angle) + p_y * cos(angle + 11.0) AS c_x,
            p_x * cos(angle + 33.0) + p_y * cos(angle) AS c_y
        FROM
        (
            SELECT
                y,
                x,
                -- Center coordinates and scale to fit vertically
                (x * 2.0 - width()) / height() AS p_x,
                (y * 2.0 - height()) / height() AS p_y,
                -- Shortened time variable with rotation
                iTime() / 4.0 - sin(iTime()) / 4.0 AS angle
            FROM
            (
                SELECT number AS y
                FROM ch_numbers() LIMIT height()
            ) AS t_y
            CROSS JOIN (SELECT number AS x FROM ch_numbers() LIMIT width()) AS t_x
        )
    )
)
ORDER BY
    y ASC,
    x ASC