-- bigint: numeric column, timestamp-shaped 64-bit. Same path as integer
-- but exercises the int64 specialisation of SetFieldValueFromVector.

DROP TABLE IF EXISTS sweep_t;

CREATE TABLE sweep_t (
  pk INTEGER PRIMARY KEY,
  body BIGINT
);

-- Stride by ~1s in microseconds, with a deterministic jitter so the term
-- dict isn't trivially monotonic.
INSERT INTO sweep_t
SELECT id, (1700000000000 + id::BIGINT * 1000000 + (id * 99991) % 86400000000)::BIGINT
FROM range(@N@) AS r(id);
