-- url: variable-width single-token verbatim path.
-- Synthetic URLs with deterministic structure; tests cardinality + length variance.

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
SELECT id,
       'https://host' || (id % 1000)::VARCHAR ||
       '.example.com/path' || (id % 500)::VARCHAR ||
       '/' || id::VARCHAR ||
       '?q=' || md5(id::VARCHAR)
FROM range(@N@) AS r(id);
