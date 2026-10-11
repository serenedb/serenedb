CREATE TABLE players (id bigserial PRIMARY KEY, team_id integer NOT NULL REFERENCES teams (id), nick text NOT NULL, mmr integer CHECK (mmr >= 0), active boolean DEFAULT true, joined timestamptz DEFAULT now(), UNIQUE (team_id, nick));
CREATE INDEX players_mmr_idx ON players (mmr);
INSERT INTO teams (name) VALUES ('OG');
