
    WITH RECURSIVE
      -- Generate x-axis coordinates
      xaxis(x, ix) AS (
        SELECT CAST(-2.5 AS DOUBLE PRECISION), 0
        UNION ALL
        SELECT x + 0.0025017869907076485, ix + 1
        FROM xaxis
        WHERE ix < 1399
      ),
      -- Generate y-axis coordinates
      yaxis(y, iy) AS (
        SELECT CAST(-1.0 AS DOUBLE PRECISION), 0
        UNION ALL
        SELECT y + 0.0025031289111389237, iy + 1
        FROM yaxis
        WHERE iy < 799
      ),
      -- Mandelbrot iteration
      mandelbrot_iterations(iter, ix, iy, cx, cy, zx, zy) AS (
        SELECT 0, ix, iy, x, y, CAST(0.0 AS DOUBLE PRECISION), CAST(0.0 AS DOUBLE PRECISION)
        FROM xaxis, yaxis

        UNION ALL

        SELECT
          iter + 1,
          ix,
          iy,
          cx,
          cy,
          zx * zx - zy * zy + cx,
          2.0 * zx * zy + cy
        FROM mandelbrot_iterations
        WHERE (zx * zx + zy * zy) < 4.0
          AND iter < 256
      ),
      -- Get max iteration for each pixel
      pixel_depths AS (
        SELECT ix, iy, MAX(iter) AS depth
        FROM mandelbrot_iterations
        GROUP BY ix, iy
      )
    SELECT ix, iy, depth
    FROM pixel_depths
    ORDER BY iy, ix;
    
