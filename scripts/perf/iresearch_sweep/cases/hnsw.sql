-- hnsw: dim-128 float vectors. Exercises bulk-cs append + WriteNoopBatch
-- (no term-dict tokenization for the vector itself).

DROP TABLE IF EXISTS sweep_t;

CREATE TABLE sweep_t (
  pk INTEGER PRIMARY KEY,
  body FLOAT[128]
);

-- Deterministic but varied vectors: each component derived from
-- (id, dim_idx) hash, normalised to [0, 1].
INSERT INTO sweep_t
SELECT id,
       list_transform(
         range(128),
         x -> ((((id * 99991 + x * 7919)::BIGINT) % 1000 + 1000) % 1000 / 1000.0)::FLOAT
       )::FLOAT[128]
FROM range(@N@) AS r(id);
