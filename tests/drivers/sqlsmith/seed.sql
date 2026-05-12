-- Seed schema for sqlsmith fuzzing.
--
-- SQLsmith introspects pg_catalog and generates random SELECT queries against
-- whatever relations and columns it finds. The variety of types, nullability,
-- and indexes here directly drives the variety of queries it tries.
--
-- Keep this small: every relation here is a hot path in every fuzzer run.

DROP TABLE IF EXISTS fuzz_orders;
DROP TABLE IF EXISTS fuzz_users;
DROP TABLE IF EXISTS fuzz_events;

CREATE TABLE fuzz_users (
	id INTEGER PRIMARY KEY,
	name TEXT,
	email TEXT,
	created_at TIMESTAMP,
	score DOUBLE PRECISION,
	is_active BOOLEAN
);

CREATE TABLE fuzz_orders (
	id BIGINT PRIMARY KEY,
	user_id INTEGER,
	total DOUBLE PRECISION,
	status TEXT,
	placed_at TIMESTAMP
);

CREATE TABLE fuzz_events (
	id BIGINT PRIMARY KEY,
	user_id INTEGER,
	kind TEXT,
	payload TEXT,
	ts TIMESTAMP
);

CREATE INDEX fuzz_orders_user_id_idx ON fuzz_orders (user_id);
CREATE INDEX fuzz_orders_status_idx  ON fuzz_orders (status);
CREATE INDEX fuzz_users_email_idx    ON fuzz_users  (email);
CREATE INDEX fuzz_events_user_id_idx ON fuzz_events (user_id);
CREATE INDEX fuzz_events_ts_idx      ON fuzz_events (ts);

INSERT INTO fuzz_users VALUES
	(1, 'alice', 'alice@example.com', '2026-01-01 00:00:00', 9.5, TRUE),
	(2, 'bob',   'bob@example.com',   '2026-02-01 00:00:00', 7.0, TRUE),
	(3, 'carol', 'carol@example.com', '2026-03-01 00:00:00', 5.5, FALSE),
	(4, 'dave',  NULL,                NULL,                  NULL, NULL);

INSERT INTO fuzz_orders VALUES
	(101, 1, 99.99,   'paid',     '2026-04-01 12:00:00'),
	(102, 1, 12.50,   'refunded', '2026-04-05 09:30:00'),
	(103, 2, 1500.00, 'pending',  '2026-04-10 18:45:00'),
	(104, 3, 0.00,    'cancelled','2026-04-12 08:15:00');

INSERT INTO fuzz_events VALUES
	(1001, 1, 'login',  '{"ip":"127.0.0.1"}', '2026-04-01 11:59:00'),
	(1002, 1, 'view',   'home',               '2026-04-01 12:00:30'),
	(1003, 2, 'login',  NULL,                 '2026-04-10 18:44:00'),
	(1004, 3, 'logout', NULL,                 '2026-04-12 08:30:00');
