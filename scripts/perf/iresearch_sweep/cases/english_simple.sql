-- english_simple: lightweight text dict (no stemming/accents), 50-word docs.
-- Isolates sink/term-dict overhead from analyzer cost (same data as english_icu).

\i scripts/perf/iresearch_sweep/cases/_vocab.sql

DROP TABLE IF EXISTS sweep_t;
DROP TEXT SEARCH DICTIONARY IF EXISTS sweep_en_simple;

CREATE TEXT SEARCH DICTIONARY sweep_en_simple(
  template = 'text',
  locale = 'en_US.UTF-8',
  case = 'lower',
  stemming = false,
  accent = false,
  frequency = true,
  position = true
);

CREATE TABLE sweep_t (
  pk INTEGER PRIMARY KEY,
  body VARCHAR
);

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
