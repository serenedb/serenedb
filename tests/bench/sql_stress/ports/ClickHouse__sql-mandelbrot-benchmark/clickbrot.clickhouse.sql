
    WITH RECURSIVE
      -- Generate pixel grid
      pixels AS (
        SELECT
          CAST(px.x AS USMALLINT) AS x,
          CAST(py.y AS USMALLINT) AS y
        FROM range(1400) AS px(x), range(800) AS py(y)
      ),
      -- Recursively iterate z = z² + c, mapping the pixel to the complex plane on the fly
      mandelbrot_iterations AS (
        SELECT x, y, 0.0::DOUBLE AS zx, 0.0::DOUBLE AS zy, 0::UINTEGER AS iteration
        FROM pixels

        UNION ALL

        SELECT
          x, y,
          zx * zx - zy * zy + (-2.5::DOUBLE + x * 3.5::DOUBLE / 1399),
          2.0::DOUBLE * zx * zy + (-1.0::DOUBLE + y * 2.0::DOUBLE / 799),
          iteration + 1
        FROM mandelbrot_iterations
        WHERE iteration < 256
          AND (zx * zx + zy * zy) <= 4.0::DOUBLE
      )
    SELECT x, y, MAX(iteration) AS depth
    FROM mandelbrot_iterations
    GROUP BY x, y
    
