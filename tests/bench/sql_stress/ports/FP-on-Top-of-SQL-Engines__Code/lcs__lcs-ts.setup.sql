\set iterations 350
-- Longest common subsequence
--
-- Consider sequences (ABCD) and (ACBAD).
-- They have 5 length-2 common subsequences: (AB), (AC), (AD), (BD), and (CD);
-- 2 length-3 common subsequences: (ABD) and (ACD); and no longer common subsequences.
-- So (ABD) and (ACD) are their longest common subsequences.
--
-- Example source: https://en.wikipedia.org/wiki/Longest_common_subsequence_problem

\set min_length 8
\set max_length 7
\set sequences 100

SELECT setseed(0.42);

-- An RNA sequence is a sequence of either guanine, uracil, adenine and cytosine each
-- denoted by G, U, A, and C respectively.
DROP TABLE IF EXISTS sequences;
CREATE TABLE sequences (id int, seq text);

-- Generate random RNA sequences.
INSERT INTO sequences
WITH RECURSIVE
sequences(id, s) AS (
  SELECT 1, seq
  FROM (SELECT STRING_AGG((array['G','U','A','C'])[(1 + random()*(4-1))::int],'')
        FROM generate_series(1,floor(:min_length + random() * (:max_length - :min_length + 1)) :: int)) AS _(seq)
    UNION ALL
  SELECT s.id + 1, seq
  FROM   sequences AS s, (SELECT STRING_AGG((array['G','U','A','C'])[(1 + random()*(4-1))::int],'')
                          FROM   (SELECT :min_length + random() * (:max_length - :min_length + 1)) AS _(r), generate_series(1,floor(r) :: int)) AS _(seq)
  WHERE  s.id < :sequences
)
TABLE sequences;

analyze sequences;