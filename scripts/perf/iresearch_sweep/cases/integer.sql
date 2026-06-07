-- integer: numeric column, no analyzer. Hot scalar path through
-- SetFieldValueFromVector<INTEGER>. Modulo-spread to keep a healthy term
-- cardinality (~1M distinct).

DROP TABLE IF EXISTS sweep_t;

CREATE TABLE sweep_t (
  pk INTEGER PRIMARY KEY,
  body INTEGER
);

INSERT INTO sweep_t
SELECT id, ((id * 99991) % 1000003)::INTEGER
FROM range(@N@) AS r(id);
