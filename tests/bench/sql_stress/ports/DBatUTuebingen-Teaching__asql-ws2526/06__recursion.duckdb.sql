
-- ➊ Generate the sequence of integers i ∊ {1,2,...,10}
-- (UNION variant):
--
WITH RECURSIVE
series(i) AS (
  VALUES (1)
    UNION
  SELECT s.i + 1 AS i
  FROM   series AS s
  WHERE  s.i < 10
)
TABLE series;


-- ➊ Generate the sequence of integers i ∊ {1,2,...,10}
-- (UNION variant, tracing the recursive table):
--
WITH RECURSIVE
series(i, "table 𝘀𝗲𝗿𝗶𝗲𝘀 after this iteration") AS (
  VALUES (1, [1])
    UNION
  SELECT s.i + 1 AS i, list(s.i + 1) OVER ()
  FROM   series AS s
  WHERE  s.i < 10
)
TABLE series;


-- ➊a macro that re-implements the
--     built-in generate_series(‹from›,‹to›[,‹step›]):
--
CREATE OR REPLACE MACRO my_generate_series(α,ω,step := 1) AS TABLE
  WITH RECURSIVE
  __series(generate_series) AS (
    SELECT α::int64
      UNION
    SELECT generate_series + step
    FROM   __series
    WHERE  generate_series < ω
  )
  TABLE __series;

-- Now use our own variant of generate_series()
SELECT s.i
FROM   my_generate_series(1135,1141,step := 3) AS s(i);



-- ➋ In the UNION variant, only previously undiscovered rows are added
--   to the final result and fed into the next iteration:
--
WITH RECURSIVE
series(i) AS (
  VALUES (1)
    UNION
  SELECT s.i + δ AS i -- generates one known + one new row (only the new row is kept)
  FROM   series AS s, (VALUES (0), (1)) AS _(δ)
  WHERE  s.i < 10
)
TABLE series;


-- ➋a In the UNION variant, only previously undiscovered rows are added
--    to the final result and fed into the next iteration:
--
WITH RECURSIVE
series(i) AS (
  VALUES (1)
    UNION
  SELECT s.i + 1 AS i -- generates the same new row twice (only one copy is kept)
  FROM   series AS s, (VALUES (0), (1)) AS _
  WHERE  s.i < 10
)
TABLE series;


-----------------------------------------------------------------------


-- ➌ UNION ALL variant: *all* rows generated in the iteration are added to
--   the result and fed into the next iteration:
--
WITH RECURSIVE
series(i) AS (
  VALUES (1)
    UNION ALL -- ⚠️ bag semantics
  SELECT s.i + 1 AS i -- generates two rows for any input row (*both* rows are kept)
  FROM   series AS s, (VALUES (0), (1)) AS _
  WHERE  s.i < 5
)
TABLE series;



-- ➌ UNION ALL variant: *all* rows generated in the iteration are added to
--   the result and fed into the next iteration (tracing the recursive table):
--
WITH RECURSIVE
series(i, "table 𝘀𝗲𝗿𝗶𝗲𝘀 after this iteration") AS (
  VALUES (1, [1])
    UNION ALL -- ⚠️ bag semantics
  SELECT s.i + 1 AS i, list(s.i + 1) OVER ()
  FROM   series AS s, (VALUES (0), (1)) AS _
  WHERE  s.i < 4
)
TABLE series;



-- ➍ Quiz: What will happen (and why?) with this UNION ALL variant of query ➋?
--
WITH RECURSIVE
series(i) AS (
  VALUES (1)
    UNION ALL -- ⚠️ bag semantics
  SELECT s.i + δ AS i -- generates one known + one new row for any input row (*both* rows are kept)
  FROM   series AS s, (VALUES (0), (1)) AS _(δ)
  WHERE  s.i < 5
)
TABLE series
LIMIT 20;
