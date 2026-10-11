CREATE TABLE teams (id serial PRIMARY KEY, name varchar(40) NOT NULL UNIQUE, founded date, budget numeric(12,2) DEFAULT 0);
COMMENT ON TABLE teams IS 'pro teams';
