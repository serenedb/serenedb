CREATE TABLE duck_hex (k VARCHAR PRIMARY KEY, v BIGINT);
CREATE TABLE duck_int (k BIGINT PRIMARY KEY, v BIGINT);

CREATE TABLE search_hex (k VARCHAR, v BIGINT)
  WITH (storage = 'search', compaction_interval = 0);
CREATE INDEX search_hex_k ON search_hex USING inverted (k);
CREATE TABLE search_int (k BIGINT, v BIGINT)
  WITH (storage = 'search', compaction_interval = 0);
CREATE INDEX search_int_k ON search_int USING inverted (k);

INSERT INTO duck_hex SELECT md5(i::VARCHAR), i FROM range(@ROWS@) r(i);
INSERT INTO search_hex SELECT md5(i::VARCHAR), i FROM range(@ROWS@) r(i);
INSERT INTO duck_int SELECT (hash(i) >> 1)::BIGINT, i FROM range(@ROWS@) r(i);
INSERT INTO search_int SELECT (hash(i) >> 1)::BIGINT, i FROM range(@ROWS@) r(i);

VACUUM (REFRESH_TABLE) search_hex;
VACUUM (COMPACT_TABLE) search_hex;
VACUUM (REFRESH_TABLE) search_int;
VACUUM (COMPACT_TABLE) search_int;
CHECKPOINT;
