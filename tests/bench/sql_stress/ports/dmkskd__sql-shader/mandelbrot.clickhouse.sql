CREATE OR REPLACE MACRO ch_numbers() AS TABLE SELECT range AS number FROM range(9223372036854775807);
CREATE OR REPLACE MACRO toFloat64(x) AS CAST(x AS DOUBLE);
CREATE OR REPLACE MACRO toUInt32(x) AS CAST(trunc(x) AS UINTEGER);
CREATE OR REPLACE MACRO toUInt64(x) AS CAST(trunc(x) AS UBIGINT);
CREATE OR REPLACE MACRO intDiv(a, b) AS a // b;
CREATE OR REPLACE MACRO tupleElement(t, i) AS struct_extract(t, i);
CREATE OR REPLACE MACRO width() AS CAST(160 AS UINTEGER);
CREATE OR REPLACE MACRO height() AS CAST(140 AS UINTEGER);
CREATE OR REPLACE MACRO iTime() AS CAST(1.0 AS DOUBLE);
CREATE OR REPLACE MACRO max_iter() AS 164;
-- Mandelbrot Set - 2-Part Parallel Query with UNION ALL
-- @run: resolution=160x140, zoom=2

WITH
    part1 AS (
        SELECT 
            toUInt32(number % width()) AS x,
            toUInt32(intDiv(number, width()) + 0) AS y
        FROM ch_numbers()
        LIMIT toUInt64(width() * intDiv(height(), 1))
    ),
    compute_mandelbrot AS (
        SELECT
            x, y,
            ((x - width() / 2.0) / height()) AS p_x,
            ((y - height() / 2.0) / height()) AS p_y,
            cos(sin(iTime() / 8.0)) * 6.2831 AS ttm,
            (p_x * cos(ttm) - p_y * sin(ttm)) - cos(iTime() / 2.0) / 2.0 AS px,
            (p_x * sin(ttm) + p_y * cos(ttm)) - sin(iTime() / 3.0) / 5.0 AS py,
            -0.57735 + 0.004 + px / (200.0 + sin(iTime() / 7.0) * 50.0) AS c_x,
            0.57735 + py / (200.0 + sin(iTime() / 7.0) * 50.0) AS c_y,
            list_reduce(range(max_iter()), (acc, i) -> if(tupleElement(acc, 6) = 1, acc, row(
                    tupleElement(acc, 1) * tupleElement(acc, 1) - tupleElement(acc, 2) * tupleElement(acc, 2) + c_x,
                    2.0 * tupleElement(acc, 1) * tupleElement(acc, 2) + c_y,
                    2.0 * (tupleElement(acc, 1) * tupleElement(acc, 3) - tupleElement(acc, 2) * tupleElement(acc, 4)) + 1.0,
                    2.0 * (tupleElement(acc, 1) * tupleElement(acc, 4) + tupleElement(acc, 2) * tupleElement(acc, 3)),
                    CAST(tupleElement(acc, 5) + 1 AS UInt8),
                    CAST(if(tupleElement(acc, 1) * tupleElement(acc, 1) + tupleElement(acc, 2) * tupleElement(acc, 2) > 4.0, 1, 0) AS UInt8)
                )), row(0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, CAST(0 AS UInt8), CAST(0 AS UInt8))) AS result,
            tupleElement(result, 1) AS z_x,
            tupleElement(result, 2) AS z_y,
            tupleElement(result, 3) AS dz_x,
            tupleElement(result, 4) AS dz_y,
            tupleElement(result, 5) AS iterations,
            sqrt(z_x * z_x + z_y * z_y) AS z_len,
            sqrt(dz_x * dz_x + dz_y * dz_y) AS dz_len,
            if(iterations < max_iter(), 
                iterations + 1.0 - ln(ln(greatest(z_len, 2.0))) / ln(2.0),
                toFloat64(max_iter())
            ) AS smooth_iter,
            sqrt(1.0 / greatest(dz_len, 0.0001)) * ln(greatest(z_len * z_len, 0.0001)) AS dist,
            least(greatest(dist * 50.0, 0.0), 1.0) AS d,
            (toFloat64(max_iter()) - smooth_iter) / toFloat64(max_iter()) AS shade,
            if(smooth_iter < 16.0,
                pow(least(1.5 * (smooth_iter / 16.0), 1.0), 1.0) * 1.15,
                pow(least(1.5 * d, 1.0), 1.0) * 1.15
            ) AS base_r,
            if(smooth_iter < 16.0,
                pow(least(1.0 * (smooth_iter / 16.0), 1.0), 3.0) * 1.15,
                pow(least(1.0 * d, 1.0), 3.0) * 1.15
            ) AS base_g,
            if(smooth_iter < 16.0,
                pow(least(1.0 * (smooth_iter / 16.0), 1.0), 16.0) * 1.15,
                pow(least(1.0 * d, 1.0), 16.0) * 1.15
            ) AS base_b,
            pow(16.0 * (1.0 - x/width()) * (1.0 - y/height()) * (x/width()) * (y/height()), 1.0/8.0) * 1.15 AS vig
        FROM part1
        
    )
SELECT
    x, y,
    sqrt(greatest(base_r * shade * d * vig, 0.0)) AS r,
    sqrt(greatest(base_g * shade * d * vig, 0.0)) AS g,
    sqrt(greatest(base_b * shade * d * vig, 0.0)) AS b
FROM compute_mandelbrot
ORDER BY y ASC, x ASC