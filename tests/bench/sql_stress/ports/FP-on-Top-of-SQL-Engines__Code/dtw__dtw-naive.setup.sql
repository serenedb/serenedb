\set iterations 10
-- Preparation
DROP TABLE IF EXISTS X;
CREATE TABLE X (
  t serial PRIMARY KEY,
  x double precision
);

DROP TABLE IF EXISTS Y;
CREATE TABLE Y (
  t serial PRIMARY KEY,
  y double precision
);

\set x 10
\set y 10

INSERT INTO X(t, x)
  SELECT t, random() AS x
  FROM   generate_series(1, :x) AS t;

INSERT INTO Y(t, y)
  SELECT t, random() AS y
  FROM   generate_series(1, :y) AS t;

analyze X; analyze Y;