DROP TABLE IF EXISTS material CASCADE;
CREATE TABLE material (materialid SERIAL PRIMARY KEY, name TEXT,
  mat_col_r DOUBLE PRECISION, mat_col_g DOUBLE PRECISION, mat_col_b DOUBLE PRECISION,
  is_metal BOOLEAN NOT NULL, shade_normal BOOLEAN NOT NULL, mirror_frac DOUBLE PRECISION NOT NULL,
  is_dielectric BOOLEAN NOT NULL, eta DOUBLE PRECISION NOT NULL DEFAULT 1.0);
INSERT INTO material (name, mat_col_r, mat_col_g, mat_col_b, is_metal, shade_normal, mirror_frac, is_dielectric, eta) VALUES
    ('dark', 0.1, 0.1, 0.1, FALSE, FALSE, 0.1, FALSE, 1.0),
    ('red', 0.95, 0.0, 0.0, FALSE, TRUE, 0.5, FALSE, 1.0),
    ('green', 0.0, 0.95, 0.0, FALSE, TRUE, 0.5, FALSE, 1.0),
    ('blue', 0.0, 0.0, 0.95, TRUE, TRUE, 0.5, FALSE, 1.0),
    ('grey', 0.1, 0.1, 0.1, FALSE, FALSE, 0.5, FALSE, 1.0),
    ('bright', 1.0, 1.0, 1.0, TRUE, TRUE, 0.5, FALSE, 1.0),
    ('mirror', NULL, NULL, NULL, TRUE, FALSE, 0.99, FALSE, 1.0),
    ('bluemirror', 0.0, 0.0, 0.3, TRUE, FALSE, 0.9, FALSE, 1.0),
    ('greenmirror', 0.0, 0.2, 0.0, TRUE, FALSE, 0.9, FALSE, 1.0),
    ('notquiteair', NULL, NULL, NULL, FALSE, FALSE, 1.0, TRUE, 1.00000001),
    ('glass', NULL, NULL, NULL, FALSE, FALSE, 0.95, TRUE, 1.5),
    ('greenglass', 0.0, 0.2, 0.0, FALSE, FALSE, 0.8, TRUE, 1.5),
    ('diamond', NULL, NULL, NULL, FALSE, FALSE, 0.99, TRUE, 2.4),
    ('antiglass', NULL, NULL, NULL, FALSE, FALSE, 0.99, TRUE, 0.2)
;

DROP TABLE IF EXISTS scene CASCADE;
CREATE TABLE IF NOT EXISTS scene (sceneid SERIAL PRIMARY KEY,
   scenename TEXT UNIQUE NOT NULL);
INSERT INTO scene (scenename) VALUES ('dielectricparty'),
                                     ('oneglassball'),
                                     ('onediamondball'),
                                     ('oneantiglassball'),
                                     ('onegreyball'),
                                     ('onegreenball'),
                                     ('twomirrorballs'),
                                     ('twodiffuseballs'),
                                     ('onemirrorball'),
                                     ('reflectiontest'),
                                     ('threemirrors'),
                                     ('adjacentballs'),
                                     ('glassmatrix'),
                                     ('airy'),
                                     ('busyday');

DROP TABLE IF EXISTS sphere CASCADE;
CREATE TABLE sphere (sphereid SERIAL, sceneid INTEGER NOT NULL REFERENCES scene(sceneid),
  cx DOUBLE PRECISION NOT NULL, cy DOUBLE PRECISION NOT NULL, cz DOUBLE PRECISION NOT NULL,
  radius DOUBLE PRECISION, radius2 DOUBLE PRECISION, materialid INTEGER NOT NULL REFERENCES material(materialid),
  vel_x DOUBLE PRECISION NOT NULL DEFAULT 0.0, vel_y DOUBLE PRECISION NOT NULL DEFAULT 0.0, vel_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  coefficient_of_restitution DOUBLE PRECISION NOT NULL DEFAULT 1.0);

INSERT INTO sphere (cx, cy, cz, radius, materialid, sceneid) VALUES
(0, 24, -10, 5,
   (SELECT materialid FROM material WHERE name='bright'), (SELECT sceneid FROM scene WHERE scenename='reflectiontest')),
(0, 5, 0, 5,
   (SELECT materialid FROM material WHERE name='red'), (SELECT sceneid FROM scene WHERE scenename='reflectiontest')),
(-17, 15, -30, 15,
   (SELECT materialid FROM material WHERE name='bluemirror'), (SELECT sceneid FROM scene WHERE scenename='reflectiontest')),
(24, 23, 10, 23,
   (SELECT materialid FROM material WHERE name='greenmirror'), (SELECT sceneid FROM scene WHERE scenename='reflectiontest')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='twomirrorballs')),
(20, 25, -30, 25,
   (SELECT materialid FROM material WHERE name='greenmirror'), (SELECT sceneid FROM scene WHERE scenename='twomirrorballs')),
(-20, 25, 0, 25,
   (SELECT materialid FROM material WHERE name='mirror'), (SELECT sceneid FROM scene WHERE scenename='twomirrorballs')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='twodiffuseballs')),
(20, 25, -30, 25,
   (SELECT materialid FROM material WHERE name='dark'), (SELECT sceneid FROM scene WHERE scenename='twodiffuseballs')),
(-20, 25, 0, 25,
   (SELECT materialid FROM material WHERE name='green'), (SELECT sceneid FROM scene WHERE scenename='twodiffuseballs')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='onemirrorball')),
(-20, 25, 0, 25,
   (SELECT materialid FROM material WHERE name='mirror'), (SELECT sceneid FROM scene WHERE scenename='onemirrorball')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='oneglassball')),
(0, 25, -10, 25,
   (SELECT materialid FROM material WHERE name='glass'), (SELECT sceneid FROM scene WHERE scenename='oneglassball')),
(20, 25, 80, 25,
   (SELECT materialid FROM material WHERE name='red'), (SELECT sceneid FROM scene WHERE scenename='oneglassball')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='onediamondball')),
(0, 25, -10, 25,
   (SELECT materialid FROM material WHERE name='diamond'), (SELECT sceneid FROM scene WHERE scenename='onediamondball')),
(20, 25, 80, 25,
   (SELECT materialid FROM material WHERE name='red'), (SELECT sceneid FROM scene WHERE scenename='onediamondball')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='oneantiglassball')),
(0, 25, -10, 25,
   (SELECT materialid FROM material WHERE name='antiglass'), (SELECT sceneid FROM scene WHERE scenename='oneantiglassball')),
(20, 25, 80, 25,
   (SELECT materialid FROM material WHERE name='red'), (SELECT sceneid FROM scene WHERE scenename='oneantiglassball')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='dielectricparty')),
(0, 12, 0, NULL,
   (SELECT materialid FROM material WHERE name='glass'), (SELECT sceneid FROM scene WHERE scenename='dielectricparty')),
(25, 12, 0, NULL,
   (SELECT materialid FROM material WHERE name='antiglass'), (SELECT sceneid FROM scene WHERE scenename='dielectricparty')),
(-25, 12, 0, NULL,
   (SELECT materialid FROM material WHERE name='diamond'), (SELECT sceneid FROM scene WHERE scenename='dielectricparty')),
(15, 10, 20, NULL,
   (SELECT materialid FROM material WHERE name='red'), (SELECT sceneid FROM scene WHERE scenename='dielectricparty')),
(-5, 10, 30, NULL,
   (SELECT materialid FROM material WHERE name='green'), (SELECT sceneid FROM scene WHERE scenename='dielectricparty')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='airy')),
(0, 12, 0, NULL,
   (SELECT materialid FROM material WHERE name='notquiteair'), (SELECT sceneid FROM scene WHERE scenename='airy')),
(25, 12, 0, NULL,
   (SELECT materialid FROM material WHERE name='notquiteair'), (SELECT sceneid FROM scene WHERE scenename='airy')),
(-25, 12, 0, NULL,
   (SELECT materialid FROM material WHERE name='notquiteair'), (SELECT sceneid FROM scene WHERE scenename='airy')),
(15, 10, 20, NULL,
   (SELECT materialid FROM material WHERE name='red'), (SELECT sceneid FROM scene WHERE scenename='airy')),
(-5, 10, 30, NULL,
   (SELECT materialid FROM material WHERE name='green'), (SELECT sceneid FROM scene WHERE scenename='airy')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='green'), (SELECT sceneid FROM scene WHERE scenename='adjacentballs')),
(-24, 12, 0, 12,
   (SELECT materialid FROM material WHERE name='red'), (SELECT sceneid FROM scene WHERE scenename='adjacentballs')),
(0, 12, 0, 12,
   (SELECT materialid FROM material WHERE name='mirror'), (SELECT sceneid FROM scene WHERE scenename='adjacentballs')),
(24, 12, 0, 12,
   (SELECT materialid FROM material WHERE name='bright'), (SELECT sceneid FROM scene WHERE scenename='adjacentballs')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='onegreyball')),
(20, 25, 0, 25,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='onegreyball')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='onegreenball')),
(20, 25, 0, 25,
   (SELECT materialid FROM material WHERE name='green'), (SELECT sceneid FROM scene WHERE scenename='onegreenball')),

(-20, 15, -15, 22,
   (SELECT materialid FROM material WHERE name='mirror'), (SELECT sceneid FROM scene WHERE scenename='threemirrors')),
(0, 0, 0, 5,
   (SELECT materialid FROM material WHERE name='mirror'), (SELECT sceneid FROM scene WHERE scenename='threemirrors')),
(30, -15, 0, 25,
   (SELECT materialid FROM material WHERE name='mirror'), (SELECT sceneid FROM scene WHERE scenename='threemirrors')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='busyday')),

(0, -1250, 0, 1250,
   (SELECT materialid FROM material WHERE name='grey'), (SELECT sceneid FROM scene WHERE scenename='glassmatrix')),
(20, 25, 80, 25,
   (SELECT materialid FROM material WHERE name='red'), (SELECT sceneid FROM scene WHERE scenename='glassmatrix'))
;

INSERT INTO sphere (cx, cy, cz, radius, materialid, sceneid, coefficient_of_restitution)
SELECT (RANDOM()-0.5) * 100, 50 + RANDOM() * 50, (RANDOM()-0.5) * 100, RANDOM() * 5.0,
       1+CAST((RANDOM()*(SELECT MAX(materialid)-1 FROM material)) AS INTEGER), (SELECT sceneid FROM scene WHERE scenename='busyday'),
       0.7*RANDOM()+0.3
    FROM generate_series(1, 20)
    GROUP BY generate_series;

INSERT INTO sphere (cx, cy, cz, radius, materialid, sceneid, coefficient_of_restitution)
WITH params(r, x1, y1) AS (SELECT 5, -60, -15)
SELECT x1 + 2 * r * x, y1 + 2 * r * y, -30, r,
      (SELECT materialid FROM material WHERE name='glass'), (SELECT sceneid FROM scene WHERE scenename='glassmatrix'),
      1.0
    FROM generate_series(1, 11) X, generate_series(1, 11) Y, params;

UPDATE sphere SET radius = cy WHERE radius IS NULL;
UPDATE sphere SET radius2 = radius*radius WHERE radius2 IS NULL;

DROP TABLE IF EXISTS camera CASCADE;
CREATE TABLE camera (cameraid INTEGER PRIMARY KEY, sceneid INTEGER NOT NULL REFERENCES scene(sceneid),
  x DOUBLE PRECISION NOT NULL, y DOUBLE PRECISION NOT NULL, z DOUBLE PRECISION NOT NULL,
  rot_x DOUBLE PRECISION NOT NULL, rot_y DOUBLE PRECISION NOT NULL, rot_z DOUBLE PRECISION NOT NULL,
  fov_rad_x DOUBLE PRECISION NOT NULL, fov_rad_y DOUBLE PRECISION NOT NULL,
  max_ray_depth INTEGER NOT NULL, samples_per_px INTEGER NOT NULL);
INSERT INTO camera (cameraid, x, y, z, rot_x, rot_y, rot_z, fov_rad_x, fov_rad_y, max_ray_depth, samples_per_px, sceneid)
  VALUES (1.0, 0.0, 65.0, -120.0, -0.34, 0.0, 0.0, PI()/3.0, PI()/3.0,
          30, 50, (SELECT sceneid FROM scene WHERE scenename='busyday'));

DROP TABLE IF EXISTS img CASCADE;
CREATE TABLE img (res_x INTEGER NOT NULL, res_y INTEGER NOT NULL, gamma DOUBLE PRECISION);
    INSERT INTO img (res_x, res_y, gamma)
        VALUES (650, 650, 1.0);


DROP VIEW IF EXISTS updateworld;
CREATE VIEW updateworld AS (SELECT 0.0 AS dt, 0.0 AS grav_x, 0.0 AS grav_y, 0.0 AS grav_z);

-- INSERT INTO updateworld (dt, grav_x, grav_y, grav_z) VALUES (0.1, 0.0, -9.8, 0.0);
-- select cx, cy, cz, vel_x, vel_y, vel_z from sphere
--   where sceneid=(SELECT sceneid FROM scene WHERE scenename='busyday');