-- Controlled dataset for the filtered-top-k study.
--  fox membership: hash(id)%10<7  -> ~70% of docs match @@ 'fox' (IDF>0)
--  tf_col = (id%20)+1              -> BM25 score driver, 1..20, independent of fox membership
--  r      = hash(id+999983)%100    -> filter independent of score (selectivity sweeps)
--  g      = id/5000                -> filter clustered in storage order (rg-prune)
--  zebra  in ~1% (rare query term, for df comparison)
CREATE TEXT SEARCH DICTIONARY en(template='text', locale='en_US.UTF-8', case='none', stemming=false, accent=false, frequency=true, position=true, norm=true);
CREATE TABLE d (id INTEGER PRIMARY KEY, tf_col INTEGER, r INTEGER, g INTEGER, body VARCHAR);
CREATE INDEX dt ON d USING inverted(id, body en) INCLUDE (tf_col, r, g);
INSERT INTO d (id, tf_col, r, g, body)
SELECT id,
       (id % 20) + 1,
       (hash(id + 999983) % 100)::INTEGER,
       id / 5000,
       (CASE WHEN (hash(id) % 10) < 7 THEN repeat('fox ', (id % 20) + 1) ELSE 'dog cat bird ' END)
       || (CASE WHEN id % 997 = 0 THEN 'zebra' ELSE '' END)
FROM (SELECT a*1000 + b AS id FROM generate_series(0,499) a, generate_series(1,1000) b) s;
VACUUM (REFRESH_TABLE) d;
