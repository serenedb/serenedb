CREATE SCHEMA o.s;
CREATE TYPE o.main.mood AS ENUM ('sad', 'ok', 'happy');
CREATE SEQUENCE o.main.seq START 10;
CREATE TABLE o.main.t (
	id INTEGER PRIMARY KEY,
	name VARCHAR NOT NULL UNIQUE,
	score DOUBLE DEFAULT 1.5 CHECK (score >= 0),
	m o.main.mood,
	tags VARCHAR[],
	st STRUCT(a INTEGER, b VARCHAR),
	mp MAP(VARCHAR, INTEGER),
	dt DATE,
	ts TIMESTAMP,
	d DECIMAL(18, 3),
	b BLOB,
	big HUGEINT,
	u UUID,
	iv INTERVAL,
	gen AS (score * 2)
);
CREATE TABLE o.main.child (id INTEGER REFERENCES t (id), note VARCHAR);
CREATE TABLE o.s.other (x BIGINT, y VARCHAR);
INSERT INTO o.main.t
SELECT i, 'n' || i, i / 10.0, (['sad', 'ok', 'happy'])[1 + i % 3], CASE WHEN i % 4 = 0 THEN NULL ELSE ['a', 'b' || i] END,
	{'a': i, 'b': 'x' || (i % 9)}, MAP {'k': i, 'l': i % 5}, DATE '2024-01-01' + i::INTEGER,
	TIMESTAMP '2024-01-01' + to_seconds(i * 37), i / 7.0, ('bytes' || i)::BLOB, i::HUGEINT * 1000000000000,
	('00000000-0000-4000-8000-' || lpad(i::VARCHAR, 12, '0'))::UUID, to_minutes(i)
FROM range(5000) r(i);
INSERT INTO o.main.child SELECT i % 5000, repeat('z', i % 50) FROM range(10000) r(i);
INSERT INTO o.s.other SELECT i, 'v' || (i % 17) FROM range(200000) r(i);
CREATE INDEX t_score_idx ON o.main.t (score);
CREATE VIEW o.main.v AS SELECT id, upper(name) AS u, score + 1 AS s1, coalesce(m::VARCHAR, '-') AS mm FROM o.main.t WHERE id % 2 = 0;
CREATE MACRO o.main.add1(x) AS x + 1;
CREATE MACRO o.main.tm(n) AS TABLE SELECT * FROM range(n);
COMMENT ON TABLE o.main.t IS 'the table';
COMMENT ON COLUMN o.main.t.name IS 'the name';
SELECT nextval('o.main.seq') FROM range(3);
