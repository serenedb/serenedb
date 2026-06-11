-- ngram: 2-3 char ngrams over the same 50-word English text.
-- Heavy token explosion: each word -> ~length-2 ngrams; total tokens ~10-15x word count.
-- Stresses term-dict size and insert throughput.

\i scripts/perf/iresearch_sweep/cases/_vocab.sql

DROP TABLE IF EXISTS sweep_t;
DROP TEXT SEARCH DICTIONARY IF EXISTS sweep_ngram;

CREATE TEXT SEARCH DICTIONARY sweep_ngram(
  template = 'ngram',
  mingram = 2,
  maxgram = 3,
  preserveoriginal = false,
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
