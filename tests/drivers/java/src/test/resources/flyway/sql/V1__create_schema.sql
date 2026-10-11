CREATE TABLE guild (
    id integer PRIMARY KEY,
    name varchar(50) NOT NULL UNIQUE,
    founded date
);

CREATE TABLE hero (
    id bigserial PRIMARY KEY,
    guild_id integer REFERENCES guild (id),
    name text NOT NULL,
    level smallint NOT NULL DEFAULT 1 CHECK (level BETWEEN 1 AND 30),
    gold numeric(12, 2) NOT NULL DEFAULT 0,
    created timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX hero_guild_idx ON hero (guild_id);

COMMENT ON TABLE hero IS 'playable characters';

CREATE SEQUENCE ticket_seq START 1000;
