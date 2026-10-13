-- Find the length of the longest common subsequence.
--
-- For example, consider the sequences (ABCD) and (ACBAD).
-- They have 5 length-2 common subsequences: (AB), (AC), (AD), (BD), and (CD);
-- 2 length-3 common subsequences: (ABD) and (ACD); and no longer common subsequences.
-- So (ABD) and (ACD) are their longest common subsequences.
--
-- Example source: https://en.wikipedia.org/wiki/Longest_common_subsequence_problem

-- Translation into a UDF using trampolined style

DROP TYPE IF EXISTS K;
CREATE TYPE K AS (l text, r text, x int, ref int);

DROP FUNCTION IF EXISTS lcs(text, text);

CREATE FUNCTION lcs(l text, r text) RETURNS int AS
$$
WITH RECURSIVE tramp(fn, l, r, x, k) AS (
  SELECT 'lcs', l, r , 0, ARRAY[] :: K[]
    UNION ALL
  SELECT _.*
  FROM   tramp AS t, LATERAL
          (SELECT 'apply', t.l, t.r, 0, t.k
          WHERE t.fn = 'lcs' AND
                ((t.l = '' AND t.r = '') OR (t.l = '' AND t.r <> '') OR (t.l <> '' AND t.r = ''))
            UNION ALL
          SELECT 'lcs', right(t.l,-1), right(t.r,-1), t.x,
                 array_prepend((t.l, t.r, 0, 1) :: K, t.k)
          WHERE t.fn = 'lcs' AND left(t.l,1) = left(t.r,1) AND
                NOT(t.l = '' OR t.r = '')
            UNION ALL
          SELECT 'lcs', right(t.l,-1), t.r, t.x,
                 array_prepend((t.l, t.r, 0, 2) :: K, t.k)
          WHERE t.fn = 'lcs' AND left(t.l,1) <> left(t.r,1) AND
                NOT(t.l = '' OR t.r = '')
            UNION ALL
          SELECT 'finish', t.l, t.r, t.x, t.k
          WHERE t.fn = 'apply' AND CARDINALITY(t.k) = 0
            UNION ALL
          SELECT 'apply', t.l, t.r, 1+t.x, t.k[2:]
          WHERE t.fn = 'apply' AND t.k[1].ref = 1
            UNION ALL
          SELECT 'lcs', t.k[1].l, right(t.k[1].r,-1), t.x,
                 array_prepend((t.k[1].l, t.k[1].r, t.x, 3) :: K, t.k[2:])
          WHERE t.fn = 'apply' AND t.k[1].ref = 2
            UNION ALL
          SELECT 'apply', t.l, t.r, GREATEST(t.k[1].x, t.x), t.k[2:]
          WHERE t.fn = 'apply' AND t.k[1].ref = 3
          ) AS _
) SELECT t.x FROM tramp AS t WHERE t.fn = 'finish';
$$ LANGUAGE SQL STABLE STRICT;

-----------------------------------------------------------------------
-- Determine the length of the longest common subsequence of seq1 and seq2

SELECT s1.seq, s2.seq, lcs(s1.seq,s2.seq)
FROM sequences AS s1, sequences AS s2
LIMIT :iterations;
