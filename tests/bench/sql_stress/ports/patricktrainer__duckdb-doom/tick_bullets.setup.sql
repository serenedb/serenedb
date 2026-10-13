-- extracted from patricktrainer/duckdb-doom index.html by _tools/extract_duckdb_doom.py
-- MAP
          DROP TABLE IF EXISTS map;
          CREATE TABLE map(x INT, y INT, tile CHAR);
          INSERT INTO map (x,y,tile)
            SELECT x,y,'#' FROM range(0,15) AS t(x) CROSS JOIN (VALUES(0),(15)) AS y(y)
          UNION ALL
            SELECT x,y,'#' FROM (VALUES(0),(15)) AS x(x) CROSS JOIN range(1,14) AS t(y);
          INSERT INTO map (x,y,tile)
            SELECT x,y,'.' FROM range(1,14) AS t1(x) CROSS JOIN range(1,14) AS t2(y);
          UPDATE map SET tile = '#' WHERE (x,y) IN ((5,5),(6,5),(7,5),(8,5),(5,10),(6,10),(7,10),(8,10));

          -- PLAYER
          DROP TABLE IF EXISTS player;
          CREATE TABLE player(x DOUBLE, y DOUBLE, dir DOUBLE, icon CHAR DEFAULT '@');
          INSERT INTO player(x,y,dir) VALUES (8.5,8.5,0);

          -- ENEMIES (Generate Randomly)
          DROP TABLE IF EXISTS enemies;
          CREATE TABLE enemies(
            id INT PRIMARY KEY,
            x DOUBLE,
            y DOUBLE,
            icon CHAR DEFAULT 'E',
            circle_dir INT DEFAULT CASE WHEN random() < 0.5 THEN 1 ELSE -1 END
          );
          WITH RandomPositions AS (
              SELECT 1 + floor(random() * 14)::INT AS rx, 1 + floor(random() * 14)::INT AS ry
              FROM generate_series(1, 25) -- Generate 25 candidates
          ),
          ValidPositions AS (
              SELECT DISTINCT rx, ry
              FROM RandomPositions rp
              WHERE NOT EXISTS (SELECT 1 FROM map m WHERE m.x=rp.rx AND m.y=rp.ry AND m.tile='#')
              AND NOT (rp.rx BETWEEN 7 AND 9 AND rp.ry BETWEEN 7 AND 9)
              LIMIT 5 -- Limit to 5 enemies
          ),
          NumberedPositions AS (
              SELECT row_number() OVER () AS id, rx + 0.5 AS x, ry + 0.5 AS y
              FROM ValidPositions
          )
          INSERT INTO enemies (id, x, y) SELECT id, x, y FROM NumberedPositions;

          -- BULLETS
          DROP TABLE IF EXISTS bullets;
          DROP SEQUENCE IF EXISTS bullets_id_seq;
          CREATE SEQUENCE bullets_id_seq START 1;
          CREATE TABLE bullets( id INTEGER PRIMARY KEY DEFAULT nextval('bullets_id_seq'), x DOUBLE, y DOUBLE, dx DOUBLE, dy DOUBLE );

          -- SETTINGS
          DROP TABLE IF EXISTS settings;
          CREATE TABLE settings(fov DOUBLE, step DOUBLE, max_steps INT, view_w INT, view_h INT);
          INSERT INTO settings VALUES (PI()/3, 0.1, 100, 32, 16);

          -- RENDER VIEW (Walls/Floor/Ceiling only)
          DROP VIEW IF EXISTS render_3d_frame;
          CREATE OR REPLACE VIEW render_3d_frame AS
          WITH RECURSIVE
          s AS (SELECT * FROM settings LIMIT 1),
          p AS (SELECT * FROM player LIMIT 1),
          potential_cols AS ( SELECT col FROM generate_series(0, 255) AS gs(col) ),
          potential_rows AS ( SELECT row FROM generate_series(0, 255) AS gs(row) ),
          cols AS ( SELECT pc.col FROM potential_cols pc, s WHERE pc.col < s.view_w ),
          rows_gen AS ( SELECT pr.row FROM potential_rows pr, s WHERE pr.row < s.view_h ),
          rays AS ( SELECT c.col, (p.dir - s.fov/2.0 + s.fov * (c.col*1.0 / (s.view_w - 1))) AS angle FROM cols c, s, p ),
          raytrace(col, step_count, fx, fy, angle) AS ( SELECT r.col, 1, p.x + COS(r.angle)*s.step, p.y + SIN(r.angle)*s.step, r.angle FROM rays r, p, s UNION ALL SELECT rt.col, rt.step_count + 1, rt.fx + COS(rt.angle)*s.step, rt.fy + SIN(rt.angle)*s.step, rt.angle FROM raytrace rt, s WHERE rt.step_count < s.max_steps AND NOT EXISTS (SELECT 1 FROM map m WHERE m.x = CAST(rt.fx AS INT) AND m.y = CAST(rt.fy AS INT) AND m.tile = '#') ),
          hit_walls AS ( SELECT col, MIN(step_count) as min_steps FROM raytrace rt WHERE EXISTS (SELECT 1 FROM map m WHERE m.x = CAST(rt.fx AS INT) AND m.y = CAST(rt.fy AS INT) AND m.tile = '#') GROUP BY col ),
          distances AS ( SELECT c.col, COALESCE(hw.min_steps * s.step, s.max_steps * s.step) AS dist FROM cols c LEFT JOIN hit_walls hw ON c.col = hw.col JOIN s ON TRUE ),
          heights AS ( SELECT d.col, CASE WHEN d.dist <= 0 THEN s.view_h ELSE GREATEST(0, LEAST(s.view_h, CAST(s.view_h / (d.dist * COS(r.angle - p.dir)) AS INT))) END AS height FROM distances d JOIN s ON TRUE JOIN rays r ON d.col = r.col JOIN player p ON TRUE ),
          pixels AS ( SELECT c.col AS x, rg.row AS y, CASE WHEN rg.row < (s.view_h - h.height) / 2 THEN ' ' WHEN rg.row >= (s.view_h + h.height) / 2 THEN '.' WHEN d.dist < s.max_steps * s.step / 4 THEN '█' WHEN d.dist < s.max_steps * s.step * 2 / 4 THEN '▓' WHEN d.dist < s.max_steps * s.step * 3 / 4 THEN '▒' ELSE '░' END AS ch FROM cols c CROSS JOIN rows_gen rg JOIN s ON TRUE JOIN heights h ON c.col = h.col JOIN distances d ON c.col = d.col )
          SELECT y, string_agg(ch, '' ORDER BY x) AS row FROM pixels GROUP BY y ORDER BY y;

          -- VIEW FOR COLUMN DISTANCES (for depth buffer)
          DROP VIEW IF EXISTS column_distances;
          CREATE VIEW column_distances AS
          WITH RECURSIVE
          s AS (SELECT * FROM settings LIMIT 1),
          p AS (SELECT * FROM player LIMIT 1),
          potential_cols AS ( SELECT col FROM generate_series(0, 255) AS gs(col) ),
          cols AS ( SELECT pc.col FROM potential_cols pc, s WHERE pc.col < s.view_w ),
          rays AS ( SELECT c.col, (p.dir - s.fov/2.0 + s.fov * (c.col*1.0 / (s.view_w - 1))) AS angle FROM cols c, s, p ),
          raytrace(col, step_count, fx, fy, angle) AS ( SELECT r.col, 1, p.x + COS(r.angle)*s.step, p.y + SIN(r.angle)*s.step, r.angle FROM rays r, p, s UNION ALL SELECT rt.col, rt.step_count + 1, rt.fx + COS(rt.angle)*s.step, rt.fy + SIN(rt.angle)*s.step, rt.angle FROM raytrace rt, s WHERE rt.step_count < s.max_steps AND NOT EXISTS (SELECT 1 FROM map m WHERE m.x = CAST(rt.fx AS INT) AND m.y = CAST(rt.fy AS INT) AND m.tile = '#') ),
          hit_walls AS ( SELECT col, MIN(step_count) as min_steps FROM raytrace rt WHERE EXISTS (SELECT 1 FROM map m WHERE m.x = CAST(rt.fx AS INT) AND m.y = CAST(rt.fy AS INT) AND m.tile = '#') GROUP BY col )
          SELECT
              c.col AS x,
              COALESCE(hw.min_steps * s.step * COS(r.angle - p.dir), s.max_steps * s.step) AS dist_corrected
          FROM cols c
          LEFT JOIN hit_walls hw ON c.col = hw.col
          JOIN s ON TRUE
          JOIN rays r ON c.col = r.col JOIN player p ON TRUE;
