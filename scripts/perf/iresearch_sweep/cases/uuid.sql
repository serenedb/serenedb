-- uuid: fixed-width 32-char hex single-token rows (keyword tokenizer).
-- High-cardinality verbatim path. Deterministic via md5(id).

DROP TABLE IF EXISTS sweep_t;
DROP TEXT SEARCH DICTIONARY IF EXISTS sweep_verbatim;

CREATE TEXT SEARCH DICTIONARY sweep_verbatim(
  template = 'keyword'
);

CREATE TABLE sweep_t (
  pk INTEGER PRIMARY KEY,
  body VARCHAR
);

INSERT INTO sweep_t
SELECT id, md5(id::VARCHAR)
FROM range(@N@) AS r(id);
