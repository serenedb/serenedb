CREATE TABLE heroes (id serial PRIMARY KEY, name varchar(40) NOT NULL UNIQUE, role text);
CREATE INDEX heroes_role_idx ON heroes (role);
