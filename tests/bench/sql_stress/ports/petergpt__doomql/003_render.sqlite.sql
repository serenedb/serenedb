DROP VIEW IF EXISTS render_frame;
DROP VIEW IF EXISTS frame_pixels;

-- One invocation of this view calculates the full raster. DDA traversal,
-- projection, texturing, sprites, weapon, crosshair and colour effects all
-- remain inside SQL.
CREATE VIEW frame_pixels AS
WITH RECURSIVE
cfg AS MATERIALIZED (
    SELECT
        s.screen_width AS width,
        s.screen_height AS height,
        s.horizontal_fov AS fov,
        s.max_depth,
        s.max_ray_steps,
        p.x AS player_x,
        p.y AS player_y,
        p.angle,
        cos(p.angle) AS dir_x,
        sin(p.angle) AS dir_y,
        -sin(p.angle) * tan(s.horizontal_fov / 2.0) AS plane_x,
        cos(p.angle) * tan(s.horizontal_fov / 2.0) AS plane_y,
        s.screen_width / (2.0 * tan(s.horizontal_fov / 2.0)) AS projection_plane,
        p.damage_flash,
        gs.state AS game_state,
        gc.now_us,
        w.muzzle_until_us
    FROM settings AS s
    JOIN player AS p ON p.id = 1
    JOIN game_status AS gs ON gs.id = 1
    JOIN game_clock AS gc ON gc.id = 1
    JOIN weapon_state AS w ON w.id = 1
    WHERE s.id = 1
),
rays AS MATERIALIZED (
    SELECT
        n.n AS x,
        2.0 * (n.n + 0.5) / c.width - 1.0 AS camera_x,
        c.dir_x + c.plane_x * (2.0 * (n.n + 0.5) / c.width - 1.0) AS ray_x,
        c.dir_y + c.plane_y * (2.0 * (n.n + 0.5) / c.width - 1.0) AS ray_y
    FROM numbers AS n
    CROSS JOIN cfg AS c
    WHERE n.n < c.width
),
ray_start AS (
    SELECT
        r.x,
        r.camera_x,
        r.ray_x,
        r.ray_y,
        cast(trunc(floor(c.player_x)) AS BIGINT) AS map_x,
        cast(trunc(floor(c.player_y)) AS BIGINT) AS map_y,
        CASE WHEN abs(r.ray_x) < 0.000000000001 THEN 1.0e30 ELSE abs(1.0 / r.ray_x) END AS delta_x,
        CASE WHEN abs(r.ray_y) < 0.000000000001 THEN 1.0e30 ELSE abs(1.0 / r.ray_y) END AS delta_y,
        CASE WHEN r.ray_x < 0 THEN -1 ELSE 1 END AS step_x,
        CASE WHEN r.ray_y < 0 THEN -1 ELSE 1 END AS step_y,
        CASE WHEN r.ray_x < 0
             THEN (c.player_x - floor(c.player_x)) *
                  CASE WHEN abs(r.ray_x) < 0.000000000001 THEN 1.0e30 ELSE abs(1.0 / r.ray_x) END
             ELSE (floor(c.player_x) + 1.0 - c.player_x) *
                  CASE WHEN abs(r.ray_x) < 0.000000000001 THEN 1.0e30 ELSE abs(1.0 / r.ray_x) END
        END AS side_x,
        CASE WHEN r.ray_y < 0
             THEN (c.player_y - floor(c.player_y)) *
                  CASE WHEN abs(r.ray_y) < 0.000000000001 THEN 1.0e30 ELSE abs(1.0 / r.ray_y) END
             ELSE (floor(c.player_y) + 1.0 - c.player_y) *
                  CASE WHEN abs(r.ray_y) < 0.000000000001 THEN 1.0e30 ELSE abs(1.0 / r.ray_y) END
        END AS side_y
    FROM rays AS r
    CROSS JOIN cfg AS c
),
walk(
    x, camera_x, ray_x, ray_y, map_x, map_y,
    delta_x, delta_y, step_x, step_y, side_x, side_y,
    hit_side, depth, distance
) AS (
    SELECT
        x, camera_x, ray_x, ray_y, map_x, map_y,
        delta_x, delta_y, step_x, step_y, side_x, side_y,
        -1, 0, CAST(0.0 AS DOUBLE PRECISION)
    FROM ray_start

    UNION ALL

    SELECT
        w.x,
        w.camera_x,
        w.ray_x,
        w.ray_y,
        w.map_x + CASE WHEN w.side_x < w.side_y THEN w.step_x ELSE 0 END,
        w.map_y + CASE WHEN w.side_x < w.side_y THEN 0 ELSE w.step_y END,
        w.delta_x,
        w.delta_y,
        w.step_x,
        w.step_y,
        w.side_x + CASE WHEN w.side_x < w.side_y THEN w.delta_x ELSE 0 END,
        w.side_y + CASE WHEN w.side_x < w.side_y THEN 0 ELSE w.delta_y END,
        CASE WHEN w.side_x < w.side_y THEN 0 ELSE 1 END,
        w.depth + 1,
        least(w.side_x, w.side_y)
    FROM walk AS w
    CROSS JOIN cfg AS c
    WHERE w.depth < c.max_ray_steps
      AND w.distance < c.max_depth
      AND least(w.side_x, w.side_y) <= c.max_depth
      AND NOT EXISTS (
          SELECT 1
          FROM map_cells AS m
          WHERE m.x = w.map_x AND m.y = w.map_y
            AND (
                m.solid = 1
                OR EXISTS (
                    SELECT 1 FROM doors AS d
                    WHERE d.x = m.x AND d.y = m.y AND d.open_fraction < 0.82
                )
            )
      )
),
hit_candidates AS (
    SELECT
        w.*,
        m.material_id
    FROM walk AS w
    JOIN map_cells AS m ON m.x = w.map_x AND m.y = w.map_y
    CROSS JOIN cfg AS c
    WHERE w.depth > 0
      AND w.distance <= c.max_depth
      AND (
          m.solid = 1
          OR EXISTS (
              SELECT 1 FROM doors AS d
              WHERE d.x = m.x AND d.y = m.y AND d.open_fraction < 0.82
          )
      )
),
ray_hits AS MATERIALIZED (
    SELECT
        r.x,
        r.camera_x,
        r.ray_x,
        r.ray_y,
        CASE WHEN hc.x IS NULL THEN c.max_depth ELSE greatest(0.001, hc.distance) END AS distance,
        CASE WHEN hc.x IS NULL THEN 0 ELSE 1 END AS did_hit,
        hc.hit_side,
        hc.material_id,
        c.player_x + r.ray_x * CASE WHEN hc.x IS NULL THEN c.max_depth ELSE hc.distance END AS hit_x,
        c.player_y + r.ray_y * CASE WHEN hc.x IS NULL THEN c.max_depth ELSE hc.distance END AS hit_y
    FROM rays AS r
    CROSS JOIN cfg AS c
    LEFT JOIN hit_candidates AS hc ON hc.x = r.x
),
wall_columns AS MATERIALIZED (
    SELECT
        rh.*,
        c.projection_plane / greatest(rh.distance, 0.05) AS wall_height,
        (c.height - c.projection_plane / greatest(rh.distance, 0.05)) / 2.0 AS wall_top,
        (c.height + c.projection_plane / greatest(rh.distance, 0.05)) / 2.0 AS wall_bottom,
        CASE WHEN rh.hit_side = 0
             THEN rh.hit_y - floor(rh.hit_y)
             ELSE rh.hit_x - floor(rh.hit_x)
        END AS texture_u,
        m.base_r, m.base_g, m.base_b,
        m.accent_r, m.accent_g, m.accent_b,
        m.emissive
    FROM ray_hits AS rh
    CROSS JOIN cfg AS c
    LEFT JOIN materials AS m ON m.id = rh.material_id
),
pixel_grid AS (
    SELECT nx.n AS x, ny.n AS y
    FROM numbers AS nx
    CROSS JOIN numbers AS ny
    CROSS JOIN cfg AS c
    WHERE nx.n < c.width AND ny.n < c.height
),
background AS MATERIALIZED (
    SELECT
        pg.x,
        pg.y,
        wc.distance AS wall_distance,
        wc.ray_x,
        wc.ray_y,
        CASE WHEN pg.y < c.height / 2 THEN
            cast(trunc(5 + 10.0 * pg.y / c.height) AS BIGINT)
        ELSE
            cast(trunc(12 + 42 * greatest(0.0, 1.0 - least(1.0,
                (c.height * 0.50 / greatest(0.5, pg.y - c.height / 2.0)) / c.max_depth
            ))) AS BIGINT)
        END AS r,
        CASE WHEN pg.y < c.height / 2 THEN
            cast(trunc(7 + 12.0 * pg.y / c.height) AS BIGINT)
        ELSE
            cast(trunc(15 + 46 * greatest(0.0, 1.0 - least(1.0,
                (c.height * 0.50 / greatest(0.5, pg.y - c.height / 2.0)) / c.max_depth
            ))) AS BIGINT)
        END AS g,
        CASE WHEN pg.y < c.height / 2 THEN
            cast(trunc(15 + 20.0 * pg.y / c.height) AS BIGINT)
        ELSE
            cast(trunc(20 + 53 * greatest(0.0, 1.0 - least(1.0,
                (c.height * 0.50 / greatest(0.5, pg.y - c.height / 2.0)) / c.max_depth
            ))) AS BIGINT)
        END AS b
    FROM pixel_grid AS pg
    JOIN wall_columns AS wc ON wc.x = pg.x
    CROSS JOIN cfg AS c
),
wall_pixels AS (
    SELECT
        bg.x,
        bg.y,
        CASE
            WHEN wc.did_hit = 1 AND bg.y >= wc.wall_top AND bg.y <= wc.wall_bottom
            THEN 1 ELSE 0
        END AS is_wall,
        cast(trunc(least(255, greatest(0,
            (CASE WHEN
                cast(trunc(wc.texture_u * 16) AS BIGINT) % 8 = 0
                OR cast(trunc(((bg.y - wc.wall_top) / greatest(wc.wall_height, 0.01)) * 18) AS BIGINT) % 9 = 0
             THEN wc.accent_r ELSE wc.base_r END)
            * (round((greatest(0.14, 1.0 - wc.distance / c.max_depth)
                * CASE WHEN wc.hit_side = 1 THEN 0.72 ELSE 1.0 END
                + wc.emissive) * 8.0) / 8.0)
        ))) AS BIGINT) AS wall_r,
        cast(trunc(least(255, greatest(0,
            (CASE WHEN
                cast(trunc(wc.texture_u * 16) AS BIGINT) % 8 = 0
                OR cast(trunc(((bg.y - wc.wall_top) / greatest(wc.wall_height, 0.01)) * 18) AS BIGINT) % 9 = 0
             THEN wc.accent_g ELSE wc.base_g END)
            * (round((greatest(0.14, 1.0 - wc.distance / c.max_depth)
                * CASE WHEN wc.hit_side = 1 THEN 0.72 ELSE 1.0 END
                + wc.emissive) * 8.0) / 8.0)
        ))) AS BIGINT) AS wall_g,
        cast(trunc(least(255, greatest(0,
            (CASE WHEN
                cast(trunc(wc.texture_u * 16) AS BIGINT) % 8 = 0
                OR cast(trunc(((bg.y - wc.wall_top) / greatest(wc.wall_height, 0.01)) * 18) AS BIGINT) % 9 = 0
             THEN wc.accent_b ELSE wc.base_b END)
            * (round((greatest(0.14, 1.0 - wc.distance / c.max_depth)
                * CASE WHEN wc.hit_side = 1 THEN 0.72 ELSE 1.0 END
                + wc.emissive) * 8.0) / 8.0)
        ))) AS BIGINT) AS wall_b
    FROM background AS bg
    JOIN wall_columns AS wc ON wc.x = bg.x
    CROSS JOIN cfg AS c
),
base_pixels AS (
    SELECT
        bg.x,
        bg.y,
        bg.wall_distance,
        CASE WHEN wp.is_wall = 1 THEN wp.wall_r ELSE
            CASE WHEN bg.y >= c.height / 2 AND (
                (cast(trunc(floor(c.player_x + (c.height * 0.50 / greatest(0.5, bg.y - c.height / 2.0)) * bg.ray_x)) AS BIGINT)
                + cast(trunc(floor(c.player_y + (c.height * 0.50 / greatest(0.5, bg.y - c.height / 2.0)) * bg.ray_y)) AS BIGINT)) % 2 = 0
            ) THEN cast(trunc(bg.r * 0.72) AS BIGINT) ELSE bg.r END
        END AS r,
        CASE WHEN wp.is_wall = 1 THEN wp.wall_g ELSE
            CASE WHEN bg.y >= c.height / 2 AND (
                (cast(trunc(floor(c.player_x + (c.height * 0.50 / greatest(0.5, bg.y - c.height / 2.0)) * bg.ray_x)) AS BIGINT)
                + cast(trunc(floor(c.player_y + (c.height * 0.50 / greatest(0.5, bg.y - c.height / 2.0)) * bg.ray_y)) AS BIGINT)) % 2 = 0
            ) THEN cast(trunc(bg.g * 0.72) AS BIGINT) ELSE bg.g END
        END AS g,
        CASE WHEN wp.is_wall = 1 THEN wp.wall_b ELSE
            CASE WHEN bg.y >= c.height / 2 AND (
                (cast(trunc(floor(c.player_x + (c.height * 0.50 / greatest(0.5, bg.y - c.height / 2.0)) * bg.ray_x)) AS BIGINT)
                + cast(trunc(floor(c.player_y + (c.height * 0.50 / greatest(0.5, bg.y - c.height / 2.0)) * bg.ray_y)) AS BIGINT)) % 2 = 0
            ) THEN cast(trunc(bg.b * 0.72) AS BIGINT) ELSE bg.b END
        END AS b
    FROM background AS bg
    JOIN wall_pixels AS wp ON wp.x = bg.x AND wp.y = bg.y
    CROSS JOIN cfg AS c
),
projected_entities AS (
    SELECT
        e.id,
        e.type_id,
        et.sprite_id,
        sp.width AS sprite_width,
        sp.height AS sprite_height,
        ((e.x - c.player_x) * c.dir_x + (e.y - c.player_y) * c.dir_y) AS depth,
        (-(e.x - c.player_x) * c.dir_y + (e.y - c.player_y) * c.dir_x) AS lateral,
        c.projection_plane * et.sprite_scale /
            greatest(0.05, ((e.x - c.player_x) * c.dir_x + (e.y - c.player_y) * c.dir_y)) AS size,
        c.width / 2.0 * (1.0 +
            (-(e.x - c.player_x) * c.dir_y + (e.y - c.player_y) * c.dir_x)
            / greatest(0.05, ((e.x - c.player_x) * c.dir_x + (e.y - c.player_y) * c.dir_y))
            / tan(c.fov / 2.0)
        ) AS center_x,
        c.height / 2.0 AS center_y
    FROM entities AS e
    JOIN entity_types AS et ON et.id = e.type_id
    JOIN sprites AS sp ON sp.id = et.sprite_id
    CROSS JOIN cfg AS c
    WHERE e.active = 1
      AND ((e.x - c.player_x) * c.dir_x + (e.y - c.player_y) * c.dir_y) > 0.08
      AND ((e.x - c.player_x) * c.dir_x + (e.y - c.player_y) * c.dir_y) < c.max_depth
      AND abs(
          (-(e.x - c.player_x) * c.dir_y + (e.y - c.player_y) * c.dir_x)
          / greatest(0.05, ((e.x - c.player_x) * c.dir_x + (e.y - c.player_y) * c.dir_y))
          / tan(c.fov / 2.0)
      ) < 1.15
),
sprite_samples AS (
    SELECT
        nx.n AS x,
        ny.n AS y,
        pe.id AS entity_id,
        pe.depth,
        spx.r,
        spx.g,
        spx.b,
        row_number() OVER (
            PARTITION BY nx.n, ny.n
            ORDER BY pe.depth, pe.id
        ) AS sprite_rank
    FROM projected_entities AS pe
    CROSS JOIN cfg AS c
    JOIN numbers AS nx
      ON nx.n >= greatest(0, cast(trunc(floor(pe.center_x - pe.size / 2.0)) AS BIGINT))
     AND nx.n < least(c.width, cast(trunc(ceil(pe.center_x + pe.size / 2.0)) AS BIGINT))
    JOIN numbers AS ny
      ON ny.n >= greatest(0, cast(trunc(floor(pe.center_y - pe.size / 2.0)) AS BIGINT))
     AND ny.n < least(c.height, cast(trunc(ceil(pe.center_y + pe.size / 2.0)) AS BIGINT))
    JOIN ray_hits AS rh ON rh.x = nx.n AND pe.depth < rh.distance
    JOIN sprite_pixels AS spx
      ON spx.sprite_id = pe.sprite_id
     AND spx.x = cast(trunc(floor((nx.n - (pe.center_x - pe.size / 2.0)) * pe.sprite_width / pe.size)) AS BIGINT)
     AND spx.y = cast(trunc(floor((ny.n - (pe.center_y - pe.size / 2.0)) * pe.sprite_height / pe.size)) AS BIGINT)
    WHERE spx.a > 0
),
world_pixels AS (
    SELECT
        bp.x,
        bp.y,
        coalesce(ss.r, bp.r) AS r,
        coalesce(ss.g, bp.g) AS g,
        coalesce(ss.b, bp.b) AS b
    FROM base_pixels AS bp
    LEFT JOIN sprite_samples AS ss
      ON ss.x = bp.x AND ss.y = bp.y AND ss.sprite_rank = 1
),
overlay_pixels AS (
    SELECT
        wp.x,
        wp.y,
        CASE
            WHEN (wp.x = cast(trunc(c.width / 2) AS BIGINT) AND abs(wp.y - c.height / 2) <= 2)
              OR (wp.y = cast(trunc(c.height / 2) AS BIGINT) AND abs(wp.x - c.width / 2) <= 2)
            THEN 232
            WHEN c.now_us < c.muzzle_until_us
              AND ((wp.x - c.width / 2.0) * (wp.x - c.width / 2.0) / 48.0
                 + (wp.y - c.height * 0.67) * (wp.y - c.height * 0.67) / 18.0) < 1.0
            THEN 255
            WHEN wp.y >= c.height * 0.76
              AND abs(wp.x - c.width / 2.0) <= 2.3 + (wp.y - c.height * 0.76) * 0.10
            THEN 16
            WHEN wp.y >= c.height * 0.86
              AND abs(wp.x - c.width / 2.0) BETWEEN 10 AND 18
            THEN 126
            WHEN wp.y >= c.height * 0.72
              AND abs(wp.x - c.width / 2.0) <= 3.5 + (wp.y - c.height * 0.72) * 0.47
            THEN cast(trunc(38 + 36 * (wp.y / c.height)) AS BIGINT)
            ELSE wp.r
        END AS r,
        CASE
            WHEN (wp.x = cast(trunc(c.width / 2) AS BIGINT) AND abs(wp.y - c.height / 2) <= 2)
              OR (wp.y = cast(trunc(c.height / 2) AS BIGINT) AND abs(wp.x - c.width / 2) <= 2)
            THEN 240
            WHEN c.now_us < c.muzzle_until_us
              AND ((wp.x - c.width / 2.0) * (wp.x - c.width / 2.0) / 48.0
                 + (wp.y - c.height * 0.67) * (wp.y - c.height * 0.67) / 18.0) < 1.0
            THEN 190
            WHEN wp.y >= c.height * 0.76
              AND abs(wp.x - c.width / 2.0) <= 2.3 + (wp.y - c.height * 0.76) * 0.10
            THEN 20
            WHEN wp.y >= c.height * 0.86
              AND abs(wp.x - c.width / 2.0) BETWEEN 10 AND 18
            THEN 43
            WHEN wp.y >= c.height * 0.72
              AND abs(wp.x - c.width / 2.0) <= 3.5 + (wp.y - c.height * 0.72) * 0.47
            THEN cast(trunc(53 + 42 * (wp.y / c.height)) AS BIGINT)
            ELSE wp.g
        END AS g,
        CASE
            WHEN (wp.x = cast(trunc(c.width / 2) AS BIGINT) AND abs(wp.y - c.height / 2) <= 2)
              OR (wp.y = cast(trunc(c.height / 2) AS BIGINT) AND abs(wp.x - c.width / 2) <= 2)
            THEN 248
            WHEN c.now_us < c.muzzle_until_us
              AND ((wp.x - c.width / 2.0) * (wp.x - c.width / 2.0) / 48.0
                 + (wp.y - c.height * 0.67) * (wp.y - c.height * 0.67) / 18.0) < 1.0
            THEN 42
            WHEN wp.y >= c.height * 0.76
              AND abs(wp.x - c.width / 2.0) <= 2.3 + (wp.y - c.height * 0.76) * 0.10
            THEN 28
            WHEN wp.y >= c.height * 0.86
              AND abs(wp.x - c.width / 2.0) BETWEEN 10 AND 18
            THEN 38
            WHEN wp.y >= c.height * 0.72
              AND abs(wp.x - c.width / 2.0) <= 3.5 + (wp.y - c.height * 0.72) * 0.47
            THEN cast(trunc(72 + 44 * (wp.y / c.height)) AS BIGINT)
            ELSE wp.b
        END AS b
    FROM world_pixels AS wp
    CROSS JOIN cfg AS c
),
final_pixels AS (
    SELECT
        op.x,
        op.y,
        cast(trunc(least(255, greatest(0,
            CASE c.game_state
                WHEN 'dead' THEN op.r * 0.45 + 105
                WHEN 'won' THEN op.r * 0.55
                WHEN 'paused' THEN op.r * 0.42
                ELSE op.r * (1.0 - c.damage_flash * 0.35) + 105 * c.damage_flash
            END
        ))) AS BIGINT) AS r,
        cast(trunc(least(255, greatest(0,
            CASE c.game_state
                WHEN 'dead' THEN op.g * 0.25
                WHEN 'won' THEN op.g * 0.72 + 48
                WHEN 'paused' THEN op.g * 0.42
                ELSE op.g * (1.0 - c.damage_flash * 0.65)
            END
        ))) AS BIGINT) AS g,
        cast(trunc(least(255, greatest(0,
            CASE c.game_state
                WHEN 'dead' THEN op.b * 0.25
                WHEN 'won' THEN op.b * 0.72 + 58
                WHEN 'paused' THEN op.b * 0.50
                ELSE op.b * (1.0 - c.damage_flash * 0.65)
            END
        ))) AS BIGINT) AS b
    FROM overlay_pixels AS op
    CROSS JOIN cfg AS c
)
SELECT x, y, r, g, b
FROM final_pixels;

-- Pair two vertical pixels into one U+2580 terminal cell. SQL emits every SGR
-- colour instruction and the final reset; the host prints rows unchanged.
CREATE VIEW render_frame AS
WITH
pixel_pairs AS (
    SELECT
        x,
        cast(trunc(y / 2) AS BIGINT) AS terminal_y,
        max(CASE WHEN y % 2 = 0 THEN (r << 16) | (g << 8) | b END) AS fg,
        max(CASE WHEN y % 2 = 1 THEN (r << 16) | (g << 8) | b END) AS bg
    FROM frame_pixels
    GROUP BY x, cast(trunc(y / 2) AS BIGINT)
),
with_previous AS (
    SELECT
        *,
        lag(fg) OVER (PARTITION BY terminal_y ORDER BY x) AS previous_fg,
        lag(bg) OVER (PARTITION BY terminal_y ORDER BY x) AS previous_bg
    FROM pixel_pairs
),
tokens AS (
    SELECT
        x,
        terminal_y,
        CASE WHEN previous_fg = fg THEN '' ELSE
            chr(27) || printf('[38;2;%d;%d;%dm',
                (fg >> 16) & 255, (fg >> 8) & 255, fg & 255)
        END ||
        CASE WHEN previous_bg = bg THEN '' ELSE
            chr(27) || printf('[48;2;%d;%d;%dm',
                (bg >> 16) & 255, (bg >> 8) & 255, bg & 255)
        END || '▀' AS token
    FROM with_previous
),
pixel_lines AS (
    SELECT
        terminal_y AS y,
        group_concat(token, '' ORDER BY x) || chr(27) || '[0m' AS scanline
    FROM tokens
    GROUP BY terminal_y
),
hud_source AS MATERIALIZED (
    SELECT
        s.screen_width AS width,
        s.screen_height / 2 AS first_y,
        p.health,
        p.max_health,
        p.ammo,
        p.has_token,
        gs.score,
        gs.state,
        gs.message,
        gc.tick
    FROM settings AS s
    JOIN player AS p ON p.id = 1
    JOIN game_status AS gs ON gs.id = 1
    JOIN game_clock AS gc ON gc.id = 1
    WHERE s.id = 1
),
hud_lines(y, scanline) AS (
    SELECT
        first_y,
        chr(27) || '[48;2;10;14;22m' || chr(27) || '[38;2;232;238;245m' ||
        substr(printf(' HP %03d/%03d   AMMO %03d   SCORE %05d   INDEX %s   TICK %07d',
            health, max_health, ammo, score,
            CASE WHEN has_token = 1 THEN 'ONLINE' ELSE 'MISSING' END,
            tick) || printf('%*s', width, ''), 1, width) || chr(27) || '[0m'
    FROM hud_source
    UNION ALL
    SELECT
        first_y + 1,
        chr(27) || '[48;2;18;9;13m' || chr(27) || '[38;2;255;190;74m' ||
        substr((' ' || message) || printf('%*s', width, ''), 1, width) || chr(27) || '[0m'
    FROM hud_source
    UNION ALL
    SELECT
        first_y + 2,
        chr(27) || '[48;2;6;8;13m' || chr(27) || '[38;2;116;199;214m' ||
        substr(' WASD MOVE  J/L OR ARROWS TURN  SPACE FIRE  E USE  P PAUSE  CTRL-C EXIT' ||
            printf('%*s', width, ''), 1, width) || chr(27) || '[0m'
    FROM hud_source
)
SELECT y, scanline FROM pixel_lines
UNION ALL
SELECT y, scanline FROM hud_lines;
