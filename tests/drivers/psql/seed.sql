-- Portable seed for the psql parity suite. MUST run identically on serened
-- and on the real Postgres oracle, so it uses only standard SQL DDL (no
-- DuckDB-specific forms). The harness passes -v schema / -v schema2.
--
-- psql expands :"schema" to a double-quoted identifier.

DROP SCHEMA IF EXISTS :"schema"  CASCADE;
DROP SCHEMA IF EXISTS :"schema2" CASCADE;

CREATE SCHEMA :"schema";
CREATE SCHEMA :"schema2";

SET search_path TO :"schema", public;

CREATE TYPE :"schema".acct_status AS ENUM ('open', 'closed', 'frozen');

CREATE SEQUENCE :"schema".acct_seq;

CREATE TABLE :"schema".account (
    id      integer PRIMARY KEY,
    owner   text NOT NULL,
    balance double precision,
    status  text,
    CONSTRAINT balance_nonneg CHECK (balance >= 0)
);

CREATE INDEX account_owner_idx ON :"schema".account (owner);

CREATE VIEW :"schema".active_accounts AS
    SELECT id, owner FROM :"schema".account WHERE status = 'open';

INSERT INTO :"schema".account (id, owner, balance, status) VALUES
    (1, 'alice', 100.0, 'open'),
    (2, 'bob',     0.0, 'closed'),
    (3, 'carol',  50.0, 'frozen');

-- A second ENUM and a composite type exercise the type catalog (\dT).
CREATE TYPE :"schema".currency AS ENUM ('usd', 'eur', 'gbp');
CREATE TYPE :"schema".point2d  AS (x double precision, y double precision);

-- A second sequence, table, index, and view (so \ds/\dt/\di/\dv all have >1 row).
CREATE SEQUENCE :"schema".txn_seq;

CREATE TABLE :"schema".transactions (
    id          integer PRIMARY KEY,
    account_id  integer NOT NULL,
    amount      double precision NOT NULL,
    currency    text NOT NULL,
    location    :"schema".point2d
);

CREATE INDEX transactions_account_idx ON :"schema".transactions (account_id);

CREATE VIEW :"schema".transactions_by_owner AS
    SELECT a.owner, t.id, t.amount, t.currency
    FROM :"schema".account a JOIN :"schema".transactions t ON t.account_id = a.id;

INSERT INTO :"schema".transactions (id, account_id, amount, currency, location) VALUES
    (1, 1, 25.0, 'usd', ROW(0.0, 0.0)),
    (2, 1, 10.5, 'eur', ROW(1.0, 2.0)),
    (3, 3, 75.0, 'gbp', ROW(3.0, 4.0));

-- A user-defined SQL function so \df has a row to enumerate. It lives in
-- {schema} (alongside the rest) so the `SET search_path TO {schema}; \df`
-- form picks it up.  The argument is unnamed because serened doesn't capture
-- proargnames on CREATE FUNCTION; PG would otherwise show `x integer` while
-- serened shows just `integer`.
CREATE FUNCTION :"schema".double_it(integer) RETURNS integer
    LANGUAGE sql AS $$ SELECT $1 * 2 $$;

-- serened-only objects: things postgres can't represent. The harness sets
-- on_serenedb=true|false via psql -v; we wrap each block in \if so the seed
-- still applies cleanly to a real postgres oracle.
\if :on_serenedb
  -- Text search dictionary backed by serened's `text` tokenizer template.
  -- Surfaces in pg_ts_dict and `\dFd`.
  CREATE TEXT SEARCH DICTIONARY :"schema".simple_dict (
    template = 'text',
    locale   = 'en_US.UTF-8',
    case     = 'none'
  );
\endif
