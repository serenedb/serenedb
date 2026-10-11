CREATE TABLE items (id serial PRIMARY KEY, hero_id integer NOT NULL REFERENCES heroes (id), name text NOT NULL, cost integer CHECK (cost >= 0));
INSERT INTO heroes (name, role) VALUES ('Pudge', 'support');
