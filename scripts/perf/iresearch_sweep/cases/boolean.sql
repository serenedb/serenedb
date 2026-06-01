-- boolean: tiny tokens, exercises BOOLEAN value path + null-marker if nulls
-- appear (we keep it non-null here so the focus is the value-side).

DROP TABLE IF EXISTS sweep_t;

CREATE TABLE sweep_t (
  pk INTEGER PRIMARY KEY,
  body BOOLEAN
);

INSERT INTO sweep_t
SELECT id, (id % 2 = 0)::BOOLEAN
FROM range(@N@) AS r(id);
