-- english_icu: heavy ICU-stemming text dictionary, 50-word docs.
-- Stresses analyzer hot path; analyzer cost dominates over sink path.

\i scripts/perf/iresearch_sweep/cases/_vocab.sql

DROP TABLE IF EXISTS sweep_t;
DROP TEXT SEARCH DICTIONARY IF EXISTS sweep_en_icu;

CREATE TEXT SEARCH DICTIONARY sweep_en_icu(
  template = 'text',
  locale = 'en_US.UTF-8',
  case = 'lower',
  stemming = true,
  accent = true,
  frequency = true,
  position = true
);

CREATE TABLE sweep_t (
  pk INTEGER PRIMARY KEY,
  body VARCHAR
);

-- Deterministic generator: each row gets 50 words sampled from the vocab by id-shifted hash.
INSERT INTO sweep_t
SELECT
  id,
  (SELECT string_agg(word, ' ')
     FROM (
       SELECT v.word
       FROM range(50) AS t(word_idx)
       JOIN sweep_vocab v
         ON v.idx = ((hash(id::BIGINT * 1009 + word_idx) % 256 + 256) % 256)
     ) t)
FROM range(@N@) AS r(id);
