-- A persisted monotonic epoch can reset after an OS reboot or database move.
-- SQL rebases absolute deadlines before adopting the lower clock sample.
UPDATE weapon_state
SET ready_at_us = greatest(
        (SELECT sample_us FROM game_clock WHERE id = 1),
        ready_at_us
          + (SELECT sample_us - now_us FROM game_clock WHERE id = 1)
    ),
    muzzle_until_us = CASE
        WHEN muzzle_until_us <= (SELECT now_us FROM game_clock WHERE id = 1) THEN 0
        ELSE muzzle_until_us
          + (SELECT sample_us - now_us FROM game_clock WHERE id = 1)
    END
WHERE id = 1
  AND (SELECT sample_us < now_us FROM game_clock WHERE id = 1);

UPDATE entities
SET next_attack_us = greatest(
    (SELECT sample_us FROM game_clock WHERE id = 1),
    next_attack_us + (SELECT sample_us - now_us FROM game_clock WHERE id = 1)
)
WHERE (SELECT sample_us < now_us FROM game_clock WHERE id = 1);

DELETE FROM input_events
WHERE captured_us > (SELECT sample_us FROM game_clock WHERE id = 1)
  AND (SELECT sample_us < now_us FROM game_clock WHERE id = 1);

UPDATE action_latches
SET armed = 1,
    last_event_us = (SELECT sample_us FROM game_clock WHERE id = 1)
        - (SELECT edge_rearm_us FROM settings WHERE id = 1)
WHERE (SELECT sample_us < now_us FROM game_clock WHERE id = 1);

-- SQL derives elapsed time from the host-supplied monotonic sample.
UPDATE game_clock
SET dt_us = CASE
        WHEN sample_us < now_us THEN 0
        ELSE least(
            (SELECT max_dt_us FROM settings WHERE id = 1),
            sample_us - now_us
        )
    END,
    now_us = sample_us,
    tick = tick + 1
WHERE id = 1;

-- The terminal supplies raw columns and rows. SQL owns the resolution policy:
-- reserve three terminal rows for the HUD, convert each remaining half-block
-- row to two framebuffer pixels, and clamp to the renderer's tested domain.
UPDATE settings
SET screen_width = least(160, greatest(40,
        (SELECT columns FROM terminal_surface WHERE id = 1)
    )),
    screen_height = least(120, greatest(30,
        2 * ((SELECT rows FROM terminal_surface WHERE id = 1) - 3)
    ))
WHERE id = 1;

-- Transactional before-images for the native audit below. They are SQL-owned
-- scratch rows, not a second simulation.
DELETE FROM tick_player_before;
INSERT INTO tick_player_before(id,x,y,angle,health,ammo,has_token)
SELECT id,x,y,angle,health,ammo,has_token FROM player WHERE id=1;

DELETE FROM tick_door_before;
INSERT INTO tick_door_before(id,locked,open_fraction)
SELECT id,locked,open_fraction FROM doors;

DELETE FROM tick_entity_before;
INSERT INTO tick_entity_before(entity_id,health,active)
SELECT id,health,active FROM entities;

DELETE FROM tick_status_before;
INSERT INTO tick_status_before(id,state,message,score)
SELECT id,state,message,score FROM game_status WHERE id=1;

UPDATE action_latches
SET armed = 1
WHERE armed = 0
  AND (SELECT now_us FROM game_clock WHERE id = 1) - last_event_us
      >= (SELECT edge_rearm_us FROM settings WHERE id = 1);

-- TRACE_SOURCE_BEGIN input_mapping
DELETE FROM tick_actions;
INSERT INTO tick_actions(
    id, forward, backward, strafe_left, strafe_right,
    turn_left, turn_right, fire_pressed, interact_pressed,
    restart_pressed, pause_pressed
)
SELECT
    1,
    coalesce((SELECT held FROM recent_actions WHERE action = 'forward'), 0.0),
    coalesce((SELECT held FROM recent_actions WHERE action = 'backward'), 0.0),
    coalesce((SELECT held FROM recent_actions WHERE action = 'strafe_left'), 0.0),
    coalesce((SELECT held FROM recent_actions WHERE action = 'strafe_right'), 0.0),
    coalesce((SELECT held FROM recent_actions WHERE action = 'turn_left'), 0.0),
    coalesce((SELECT held FROM recent_actions WHERE action = 'turn_right'), 0.0),
    EXISTS(SELECT 1 FROM recent_actions WHERE action = 'fire' AND pressed = 1),
    EXISTS(
        SELECT 1 FROM recent_actions AS ra
        JOIN action_latches AS al ON al.action = ra.action
        WHERE ra.action = 'interact' AND ra.pressed = 1 AND al.armed = 1
    ),
    EXISTS(
        SELECT 1 FROM recent_actions AS ra
        JOIN action_latches AS al ON al.action = ra.action
        WHERE ra.action = 'restart' AND ra.pressed = 1 AND al.armed = 1
    ),
    EXISTS(
        SELECT 1 FROM recent_actions AS ra
        JOIN action_latches AS al ON al.action = ra.action
        WHERE ra.action = 'pause' AND ra.pressed = 1 AND al.armed = 1
    );
-- TRACE_SOURCE_END input_mapping

UPDATE action_latches
SET armed = 0,
    last_event_us = coalesce((
        SELECT max(ie.captured_us)
        FROM input_events AS ie
        JOIN key_bindings AS kb ON kb.raw_code = ie.raw_code
        WHERE kb.action = action_latches.action
          AND ie.consumed_tick IS NULL
    ), last_event_us)
WHERE EXISTS (
    SELECT 1
    FROM input_events AS ie
    JOIN key_bindings AS kb ON kb.raw_code = ie.raw_code
    WHERE kb.action = action_latches.action
      AND ie.consumed_tick IS NULL
);

-- A restart is itself SQL-owned. The clock remains monotonic, while all
-- authoritative gameplay state returns to its spawn data.
DELETE FROM event_log
WHERE EXISTS (
    SELECT 1 FROM tick_actions, game_status
    WHERE tick_actions.id = 1
      AND tick_actions.restart_pressed = 1
      AND game_status.id = 1
      AND game_status.state IN ('dead', 'won')
);

DELETE FROM entities
WHERE EXISTS (
    SELECT 1 FROM tick_actions, game_status
    WHERE tick_actions.id = 1
      AND tick_actions.restart_pressed = 1
      AND game_status.id = 1
      AND game_status.state IN ('dead', 'won')
);

INSERT INTO entities(id, type_id, x, y, angle, health, state, next_attack_us, active)
SELECT es.id, es.type_id, es.x, es.y, es.angle, et.max_health, 'idle', 0, 1
FROM entity_spawns AS es
JOIN entity_types AS et ON et.id = es.type_id
WHERE EXISTS (
    SELECT 1 FROM tick_actions, game_status
    WHERE tick_actions.id = 1
      AND tick_actions.restart_pressed = 1
      AND game_status.id = 1
      AND game_status.state IN ('dead', 'won')
)
ORDER BY es.id;

UPDATE player
SET x = (SELECT x FROM player_spawn WHERE id = 1),
    y = (SELECT y FROM player_spawn WHERE id = 1),
    angle = (SELECT angle FROM player_spawn WHERE id = 1),
    health = (SELECT health FROM player_spawn WHERE id = 1),
    max_health = (SELECT health FROM player_spawn WHERE id = 1),
    ammo = (SELECT ammo FROM player_spawn WHERE id = 1),
    has_token = 0,
    damage_flash = 0
WHERE id = 1
  AND EXISTS (
      SELECT 1 FROM tick_actions, game_status
      WHERE tick_actions.id = 1
        AND tick_actions.restart_pressed = 1
        AND game_status.id = 1
        AND game_status.state IN ('dead', 'won')
  );

UPDATE weapon_state
SET ready_at_us = (SELECT now_us FROM game_clock WHERE id = 1),
    muzzle_until_us = 0
WHERE id = 1
  AND EXISTS (
      SELECT 1 FROM tick_actions, game_status
      WHERE tick_actions.id = 1
        AND tick_actions.restart_pressed = 1
        AND game_status.id = 1
        AND game_status.state IN ('dead', 'won')
  );

UPDATE doors
SET open_fraction = 0,
    locked = 1
WHERE EXISTS (
    SELECT 1 FROM tick_actions, game_status
    WHERE tick_actions.id = 1
      AND tick_actions.restart_pressed = 1
      AND game_status.id = 1
      AND game_status.state IN ('dead', 'won')
);

UPDATE game_status
SET state = 'running',
    message = 'FIND THE INDEX TOKEN',
    score = 0
WHERE id = 1
  AND state IN ('dead', 'won')
  AND (SELECT restart_pressed FROM tick_actions WHERE id = 1) = 1;

UPDATE game_status
SET state = CASE state WHEN 'running' THEN 'paused' ELSE 'running' END,
    message = CASE state WHEN 'running' THEN 'TRANSACTION PAUSED' ELSE message END
WHERE id = 1
  AND state IN ('running', 'paused')
  AND (SELECT pause_pressed FROM tick_actions WHERE id = 1) = 1;

UPDATE player
SET damage_flash = greatest(
    0.0,
    damage_flash - (SELECT dt_us / 220000.0 FROM game_clock WHERE id = 1)
)
WHERE id = 1;

-- TRACE_SOURCE_BEGIN player_motion
-- Rotation and movement are calculated from SQL-owned action leases. A held
-- terminal key is represented by repeated raw events; a tap expires after the
-- configured lease without requiring a key-up event from the terminal.
UPDATE player
SET angle = atan2(
    sin(angle + (
        (SELECT turn_right - turn_left FROM tick_actions WHERE id = 1)
        * (SELECT turn_speed FROM settings WHERE id = 1)
        * (SELECT dt_us / 1000000.0 FROM game_clock WHERE id = 1)
    )),
    cos(angle + (
        (SELECT turn_right - turn_left FROM tick_actions WHERE id = 1)
        * (SELECT turn_speed FROM settings WHERE id = 1)
        * (SELECT dt_us / 1000000.0 FROM game_clock WHERE id = 1)
    ))
)
WHERE id = 1
  AND (SELECT state FROM game_status WHERE id = 1) = 'running';

DELETE FROM tick_motion;
INSERT INTO tick_motion(id, dx, dy)
WITH intent AS (
    SELECT
        (ta.forward - ta.backward) AS forward_axis,
        (ta.strafe_right - ta.strafe_left) AS strafe_axis,
        p.angle,
        s.move_speed * gc.dt_us / 1000000.0 AS distance
    FROM tick_actions AS ta
    JOIN player AS p ON p.id = 1
    JOIN settings AS s ON s.id = 1
    JOIN game_clock AS gc ON gc.id = 1
), normalized AS (
    SELECT *,
        CASE
            WHEN forward_axis = 0 AND strafe_axis = 0 THEN 1.0
            ELSE sqrt(forward_axis * forward_axis + strafe_axis * strafe_axis)
        END AS magnitude
    FROM intent
)
SELECT
    1,
    distance * (
        cos(angle) * forward_axis + cos(angle + 1.5707963267948966) * strafe_axis
    ) / magnitude,
    distance * (
        sin(angle) * forward_axis + sin(angle + 1.5707963267948966) * strafe_axis
    ) / magnitude
FROM normalized;

-- X and Y resolve independently, giving deterministic wall sliding.
UPDATE player
SET x = x + (SELECT dx FROM tick_motion WHERE id = 1)
WHERE id = 1
  AND (SELECT state FROM game_status WHERE id = 1) = 'running'
  AND NOT EXISTS (
      SELECT 1
      FROM map_cells AS m
      JOIN settings AS s ON s.id = 1
      WHERE (
          m.solid = 1
          OR EXISTS (
              SELECT 1 FROM doors AS d
              WHERE d.x = m.x AND d.y = m.y AND d.open_fraction < 0.82
          )
      )
      AND (
          greatest(m.x - (player.x + (SELECT dx FROM tick_motion WHERE id = 1)), 0.0,
              (player.x + (SELECT dx FROM tick_motion WHERE id = 1)) - (m.x + 1.0))
          *
          greatest(m.x - (player.x + (SELECT dx FROM tick_motion WHERE id = 1)), 0.0,
              (player.x + (SELECT dx FROM tick_motion WHERE id = 1)) - (m.x + 1.0))
          +
          greatest(m.y - player.y, 0.0, player.y - (m.y + 1.0))
          *
          greatest(m.y - player.y, 0.0, player.y - (m.y + 1.0))
      ) < s.player_radius * s.player_radius
  );

UPDATE player
SET y = y + (SELECT dy FROM tick_motion WHERE id = 1)
WHERE id = 1
  AND (SELECT state FROM game_status WHERE id = 1) = 'running'
  AND NOT EXISTS (
      SELECT 1
      FROM map_cells AS m
      JOIN settings AS s ON s.id = 1
      WHERE (
          m.solid = 1
          OR EXISTS (
              SELECT 1 FROM doors AS d
              WHERE d.x = m.x AND d.y = m.y AND d.open_fraction < 0.82
          )
      )
      AND (
          greatest(m.x - player.x, 0.0, player.x - (m.x + 1.0))
          *
          greatest(m.x - player.x, 0.0, player.x - (m.x + 1.0))
          +
          greatest(m.y - (player.y + (SELECT dy FROM tick_motion WHERE id = 1)), 0.0,
              (player.y + (SELECT dy FROM tick_motion WHERE id = 1)) - (m.y + 1.0))
          *
          greatest(m.y - (player.y + (SELECT dy FROM tick_motion WHERE id = 1)), 0.0,
              (player.y + (SELECT dy FROM tick_motion WHERE id = 1)) - (m.y + 1.0))
      ) < s.player_radius * s.player_radius
  );
-- TRACE_SOURCE_END player_motion

-- TRACE_SOURCE_BEGIN pickup_resolution
-- Pickups are ordinary relational entities. Apply their values before marking
-- them inactive so multiple simultaneous pickups remain deterministic.
INSERT INTO event_log(tick, kind, source_id, target_id, value, detail)
SELECT gc.tick, 'pickup', e.id, 1, et.pickup_value, et.name
FROM entities AS e
JOIN entity_types AS et ON et.id = e.type_id AND et.kind = 'pickup'
JOIN player AS p ON p.id = 1
JOIN game_clock AS gc ON gc.id = 1
WHERE e.active = 1
  AND (e.x - p.x) * (e.x - p.x) + (e.y - p.y) * (e.y - p.y) < 0.42 * 0.42
  AND (SELECT state FROM game_status WHERE id = 1) = 'running'
ORDER BY e.id;

UPDATE player
SET health = least(max_health, health + coalesce((
        SELECT sum(et.pickup_value)
        FROM entities AS e
        JOIN entity_types AS et ON et.id = e.type_id
        WHERE e.active = 1 AND et.pickup_kind = 'health'
          AND (e.x - player.x) * (e.x - player.x) + (e.y - player.y) * (e.y - player.y) < 0.42 * 0.42
    ), 0)),
    ammo = ammo + coalesce((
        SELECT sum(et.pickup_value)
        FROM entities AS e
        JOIN entity_types AS et ON et.id = e.type_id
        WHERE e.active = 1 AND et.pickup_kind = 'ammo'
          AND (e.x - player.x) * (e.x - player.x) + (e.y - player.y) * (e.y - player.y) < 0.42 * 0.42
    ), 0),
    has_token = greatest(has_token, coalesce((
        SELECT max(et.pickup_value)
        FROM entities AS e
        JOIN entity_types AS et ON et.id = e.type_id
        WHERE e.active = 1 AND et.pickup_kind = 'token'
          AND (e.x - player.x) * (e.x - player.x) + (e.y - player.y) * (e.y - player.y) < 0.42 * 0.42
    ), 0))
WHERE id = 1
  AND (SELECT state FROM game_status WHERE id = 1) = 'running';

UPDATE game_status
SET score = score + coalesce((
    SELECT sum(et.score_value)
    FROM entities AS e
    JOIN entity_types AS et ON et.id = e.type_id
    JOIN player AS p ON p.id = 1
    WHERE e.active = 1 AND et.kind = 'pickup'
      AND (e.x - p.x) * (e.x - p.x) + (e.y - p.y) * (e.y - p.y) < 0.42 * 0.42
), 0)
WHERE id = 1 AND state = 'running';

UPDATE entities
SET active = 0,
    state = 'collected'
WHERE active = 1
  AND type_id IN (SELECT id FROM entity_types WHERE kind = 'pickup')
  AND EXISTS (
      SELECT 1 FROM player AS p
      WHERE (entities.x - p.x) * (entities.x - p.x)
          + (entities.y - p.y) * (entities.y - p.y) < 0.42 * 0.42
  )
  AND (SELECT state FROM game_status WHERE id = 1) = 'running';
-- TRACE_SOURCE_END pickup_resolution

-- TRACE_SOURCE_BEGIN door_interaction
-- The index gate is locked until the token has been collected. Interaction is
-- edge-triggered, so one key press cannot repeatedly toggle the same door.
INSERT INTO event_log(tick, kind, source_id, target_id, value, detail)
SELECT gc.tick, 'door', 1, d.id, p.has_token,
       CASE WHEN d.locked = 1 AND p.has_token = 0 THEN 'INDEX REQUIRED'
            WHEN d.open_fraction < 0.5 THEN 'GATE OPENED'
            ELSE 'GATE CLOSED' END
FROM doors AS d
JOIN player AS p ON p.id = 1
JOIN game_clock AS gc ON gc.id = 1
WHERE (SELECT interact_pressed FROM tick_actions WHERE id = 1) = 1
  AND (SELECT state FROM game_status WHERE id = 1) = 'running'
  AND (d.x + 0.5 - p.x) * (d.x + 0.5 - p.x)
      + (d.y + 0.5 - p.y) * (d.y + 0.5 - p.y) < 1.45 * 1.45;

UPDATE doors
SET locked = CASE WHEN locked = 1 AND (SELECT has_token FROM player WHERE id = 1) = 1 THEN 0 ELSE locked END,
    open_fraction = CASE
        WHEN locked = 1 AND (SELECT has_token FROM player WHERE id = 1) = 1 THEN 1.0
        WHEN locked = 0 THEN 1.0 - open_fraction
        ELSE open_fraction
    END
WHERE (SELECT interact_pressed FROM tick_actions WHERE id = 1) = 1
  AND (SELECT state FROM game_status WHERE id = 1) = 'running'
  AND EXISTS (
      SELECT 1 FROM player AS p
      WHERE (doors.x + 0.5 - p.x) * (doors.x + 0.5 - p.x)
          + (doors.y + 0.5 - p.y) * (doors.y + 0.5 - p.y) < 1.45 * 1.45
  );
-- TRACE_SOURCE_END door_interaction

-- TRACE_SOURCE_BEGIN shot_resolution
-- Resolve one deterministic centre-line hitscan shot. The nearest enemy must
-- be closer than the first wall sample along the camera direction.
DELETE FROM tick_shot;
WITH params AS MATERIALIZED (
    SELECT
        p.x, p.y, p.angle, p.ammo,
        w.range, w.ready_at_us,
        gc.now_us,
        ta.fire_pressed,
        CASE WHEN gs.state = 'running' AND ta.fire_pressed = 1
                  AND p.ammo > 0 AND gc.now_us >= w.ready_at_us
             THEN 1 ELSE 0 END AS fired
    FROM player AS p
    JOIN weapon_state AS w ON w.id = 1
    JOIN game_clock AS gc ON gc.id = 1
    JOIN tick_actions AS ta ON ta.id = 1
    JOIN game_status AS gs ON gs.id = 1
), wall_depth AS MATERIALIZED (
    SELECT coalesce(min(n.n * 0.08), (SELECT range FROM params)) AS distance
    FROM numbers AS n
    CROSS JOIN params AS p
    WHERE n.n BETWEEN 1 AND cast(trunc(p.range / 0.08) AS BIGINT)
      AND EXISTS (
          SELECT 1
          FROM map_cells AS m
          WHERE m.x = cast(trunc(floor(p.x + cos(p.angle) * n.n * 0.08)) AS BIGINT)
            AND m.y = cast(trunc(floor(p.y + sin(p.angle) * n.n * 0.08)) AS BIGINT)
            AND (
                m.solid = 1
                OR EXISTS (
                    SELECT 1 FROM doors AS d
                    WHERE d.x = m.x AND d.y = m.y AND d.open_fraction < 0.82
                )
            )
      )
), candidates AS MATERIALIZED (
    SELECT
        e.id,
        ((e.x - p.x) * cos(p.angle) + (e.y - p.y) * sin(p.angle)) AS along,
        abs(-(e.x - p.x) * sin(p.angle) + (e.y - p.y) * cos(p.angle)) AS side,
        et.radius
    FROM entities AS e
    JOIN entity_types AS et ON et.id = e.type_id AND et.kind = 'enemy'
    CROSS JOIN params AS p
    WHERE e.active = 1 AND e.health > 0
)
INSERT INTO tick_shot(id, fired, target_id)
SELECT
    1,
    p.fired,
    CASE WHEN p.fired = 1 THEN (
        SELECT c.id
        FROM candidates AS c
        CROSS JOIN wall_depth AS wd
        WHERE c.along > 0
          AND c.along < p.range
          AND c.along < wd.distance
          AND c.side <= c.radius + 0.08
        ORDER BY c.along, c.id
        LIMIT 1
    ) END
FROM params AS p;

UPDATE weapon_state
SET ready_at_us = (SELECT now_us FROM game_clock WHERE id = 1) + cooldown_us,
    muzzle_until_us = (SELECT now_us FROM game_clock WHERE id = 1) + 80000
WHERE id = 1 AND (SELECT fired FROM tick_shot WHERE id = 1) = 1;

UPDATE player
SET ammo = ammo - 1
WHERE id = 1 AND (SELECT fired FROM tick_shot WHERE id = 1) = 1;

INSERT INTO event_log(tick, kind, source_id, target_id, value, detail)
SELECT gc.tick, 'shot', 1, ts.target_id,
       CASE WHEN ts.target_id IS NULL THEN 0 ELSE w.damage END,
       CASE WHEN ts.target_id IS NULL THEN 'MISS' ELSE 'HIT' END
FROM tick_shot AS ts
JOIN game_clock AS gc ON gc.id = 1
JOIN weapon_state AS w ON w.id = 1
WHERE ts.fired = 1;

UPDATE entities
SET health = greatest(0, health - (SELECT damage FROM weapon_state WHERE id = 1)),
    state = CASE
        WHEN health - (SELECT damage FROM weapon_state WHERE id = 1) <= 0 THEN 'dead'
        ELSE 'hurt'
    END
WHERE id = (SELECT target_id FROM tick_shot WHERE id = 1)
  AND active = 1;

INSERT INTO event_log(tick, kind, source_id, target_id, value, detail)
SELECT gc.tick, 'enemy_death', 1, e.id, et.score_value, et.name
FROM entities AS e
JOIN entity_types AS et ON et.id = e.type_id
JOIN game_clock AS gc ON gc.id = 1
WHERE e.id = (SELECT target_id FROM tick_shot WHERE id = 1)
  AND e.active = 1
  AND e.health <= 0;

UPDATE game_status
SET score = score + coalesce((
    SELECT et.score_value
    FROM entities AS e
    JOIN entity_types AS et ON et.id = e.type_id
    WHERE e.id = (SELECT target_id FROM tick_shot WHERE id = 1)
      AND e.active = 1 AND e.health <= 0
), 0)
WHERE id = 1;

UPDATE entities
SET active = 0, state = 'dead'
WHERE id = (SELECT target_id FROM tick_shot WHERE id = 1)
  AND health <= 0;
-- TRACE_SOURCE_END shot_resolution

-- Enemy movement is planned as a relation before any enemy row moves, avoiding
-- update-order-dependent behaviour.
DELETE FROM tick_enemy_plan;
WITH enemy_vectors AS MATERIALIZED (
    SELECT
        e.id,
        e.x,
        e.y,
        et.id AS type_id,
        et.speed,
        et.attack_range,
        sqrt((p.x - e.x) * (p.x - e.x) + (p.y - e.y) * (p.y - e.y)) AS distance,
        CASE
            WHEN et.id = 2 AND sqrt((p.x - e.x) * (p.x - e.x) + (p.y - e.y) * (p.y - e.y)) < 1.8 THEN -1.0
            WHEN et.id = 2 AND sqrt((p.x - e.x) * (p.x - e.x) + (p.y - e.y) * (p.y - e.y)) <= 3.1 THEN 0.0
            WHEN et.id <> 2 AND sqrt((p.x - e.x) * (p.x - e.x) + (p.y - e.y) * (p.y - e.y)) <= et.attack_range * 0.88 THEN 0.0
            ELSE 1.0
        END AS direction_sign,
        p.x AS player_x,
        p.y AS player_y,
        gc.dt_us / 1000000.0 AS dt
    FROM entities AS e
    JOIN entity_types AS et ON et.id = e.type_id AND et.kind = 'enemy'
    JOIN player AS p ON p.id = 1
    JOIN game_clock AS gc ON gc.id = 1
    WHERE e.active = 1
      AND e.health > 0
      AND (SELECT state FROM game_status WHERE id = 1) = 'running'
)
INSERT INTO tick_enemy_plan(entity_id, next_x, next_y)
SELECT
    id,
    x + CASE WHEN distance < 0.0001 THEN 0 ELSE (player_x - x) / distance * speed * dt * direction_sign END,
    y + CASE WHEN distance < 0.0001 THEN 0 ELSE (player_y - y) / distance * speed * dt * direction_sign END
FROM enemy_vectors
ORDER BY id;

UPDATE entities
SET x = (SELECT next_x FROM tick_enemy_plan WHERE entity_id = entities.id),
    state = 'chase'
WHERE id IN (SELECT entity_id FROM tick_enemy_plan)
  AND NOT EXISTS (
      SELECT 1
      FROM tick_enemy_plan AS ep
      JOIN entity_types AS et ON et.id = entities.type_id
      CROSS JOIN map_cells AS m
      WHERE ep.entity_id = entities.id
        AND (
            m.solid = 1
            OR EXISTS (
                SELECT 1 FROM doors AS d
                WHERE d.x = m.x AND d.y = m.y AND d.open_fraction < 0.82
            )
        )
        AND (
            greatest(m.x - ep.next_x, 0.0, ep.next_x - (m.x + 1.0))
            * greatest(m.x - ep.next_x, 0.0, ep.next_x - (m.x + 1.0))
            + greatest(m.y - entities.y, 0.0, entities.y - (m.y + 1.0))
            * greatest(m.y - entities.y, 0.0, entities.y - (m.y + 1.0))
        ) < et.radius * et.radius
  );

UPDATE entities
SET y = (SELECT next_y FROM tick_enemy_plan WHERE entity_id = entities.id)
WHERE id IN (SELECT entity_id FROM tick_enemy_plan)
  AND NOT EXISTS (
      SELECT 1
      FROM tick_enemy_plan AS ep
      JOIN entity_types AS et ON et.id = entities.type_id
      CROSS JOIN map_cells AS m
      WHERE ep.entity_id = entities.id
        AND (
            m.solid = 1
            OR EXISTS (
                SELECT 1 FROM doors AS d
                WHERE d.x = m.x AND d.y = m.y AND d.open_fraction < 0.82
            )
        )
        AND (
            greatest(m.x - entities.x, 0.0, entities.x - (m.x + 1.0))
            * greatest(m.x - entities.x, 0.0, entities.x - (m.x + 1.0))
            + greatest(m.y - ep.next_y, 0.0, ep.next_y - (m.y + 1.0))
            * greatest(m.y - ep.next_y, 0.0, ep.next_y - (m.y + 1.0))
        ) < et.radius * et.radius
  );

-- TRACE_SOURCE_BEGIN enemy_attack
-- Enemies attack after moving. All valid attacks are aggregated before the
-- player's health is updated, so row order cannot alter damage.
INSERT INTO event_log(tick, kind, source_id, target_id, value, detail)
SELECT gc.tick, 'enemy_attack', e.id, 1, et.contact_damage, et.name
FROM entities AS e
JOIN entity_types AS et ON et.id = e.type_id AND et.kind = 'enemy'
JOIN player AS p ON p.id = 1
JOIN game_clock AS gc ON gc.id = 1
WHERE e.active = 1
  AND e.health > 0
  AND gc.now_us >= e.next_attack_us
  AND (e.x - p.x) * (e.x - p.x) + (e.y - p.y) * (e.y - p.y) <= et.attack_range * et.attack_range
  AND (SELECT state FROM game_status WHERE id = 1) = 'running'
  AND NOT EXISTS (
      SELECT 1
      FROM numbers AS n
      JOIN map_cells AS m
        ON m.x = cast(trunc(floor(e.x + (p.x - e.x) * n.n / 64.0)) AS BIGINT)
       AND m.y = cast(trunc(floor(e.y + (p.y - e.y) * n.n / 64.0)) AS BIGINT)
      WHERE n.n BETWEEN 1 AND 63
        AND (
            m.solid = 1
            OR EXISTS (
                SELECT 1 FROM doors AS d
                WHERE d.x = m.x AND d.y = m.y AND d.open_fraction < 0.82
            )
        )
  )
ORDER BY e.id;

UPDATE player
SET health = greatest(0, health - coalesce((
        SELECT sum(value)
        FROM event_log
        WHERE tick = (SELECT tick FROM game_clock WHERE id = 1)
          AND kind = 'enemy_attack'
    ), 0)),
    damage_flash = CASE WHEN EXISTS (
        SELECT 1 FROM event_log
        WHERE tick = (SELECT tick FROM game_clock WHERE id = 1)
          AND kind = 'enemy_attack'
    ) THEN 1.0 ELSE damage_flash END
WHERE id = 1
  AND (SELECT state FROM game_status WHERE id = 1) = 'running';

UPDATE entities
SET next_attack_us = (SELECT now_us FROM game_clock WHERE id = 1)
        + (SELECT attack_cooldown_us FROM entity_types WHERE id = entities.type_id),
    state = 'attack'
WHERE id IN (
    SELECT source_id FROM event_log
    WHERE tick = (SELECT tick FROM game_clock WHERE id = 1)
      AND kind = 'enemy_attack'
);
-- TRACE_SOURCE_END enemy_attack

-- TRACE_SOURCE_BEGIN state_resolution
INSERT INTO event_log(tick, kind, source_id, target_id, value, detail)
SELECT gc.tick, 'player_death', NULL, 1, 0, 'TRANSACTION ABORTED'
FROM game_clock AS gc
JOIN player AS p ON p.id = 1
JOIN game_status AS gs ON gs.id = 1
WHERE p.health <= 0 AND gs.state = 'running';

UPDATE game_status
SET state = 'dead', message = 'TRANSACTION ABORTED — PRESS R TO RESTART'
WHERE id = 1
  AND state = 'running'
  AND (SELECT health FROM player WHERE id = 1) <= 0;

-- Lethal damage wins ties: victory is checked only if the player remains alive.
INSERT INTO event_log(tick, kind, source_id, target_id, value, detail)
SELECT gc.tick, 'victory', 1, NULL, 1000, 'COMMIT ACCEPTED'
FROM game_clock AS gc
JOIN game_status AS gs ON gs.id = 1
JOIN player AS p ON p.id = 1
JOIN map_cells AS m
  ON m.x = cast(trunc(floor(p.x)) AS BIGINT)
 AND m.y = cast(trunc(floor(p.y)) AS BIGINT)
WHERE gs.state = 'running'
  AND p.health > 0
  AND m.is_exit = 1
  AND NOT EXISTS (
      SELECT 1 FROM entities AS e
      JOIN entity_types AS et ON et.id = e.type_id
      WHERE e.active = 1 AND et.is_boss = 1
  );

UPDATE game_status
SET state = 'won',
    message = 'COMMIT ACCEPTED — PRESS R TO RESTART',
    score = score + 1000
WHERE id = 1
  AND state = 'running'
  AND EXISTS (
      SELECT 1 FROM player AS p
      JOIN map_cells AS m
        ON m.x = cast(trunc(floor(p.x)) AS BIGINT)
       AND m.y = cast(trunc(floor(p.y)) AS BIGINT)
      WHERE p.health > 0 AND m.is_exit = 1
  )
  AND NOT EXISTS (
      SELECT 1 FROM entities AS e
      JOIN entity_types AS et ON et.id = e.type_id
      WHERE e.active = 1 AND et.is_boss = 1
  );

UPDATE game_status
SET message = CASE
    WHEN (SELECT has_token FROM player WHERE id = 1) = 0 THEN 'FIND THE INDEX TOKEN'
    WHEN EXISTS (SELECT 1 FROM doors WHERE open_fraction < 0.82) THEN 'UNLOCK THE INDEX GATE [E]'
    WHEN EXISTS (
        SELECT 1 FROM entities AS e
        JOIN entity_types AS et ON et.id = e.type_id
        WHERE e.active = 1 AND et.is_boss = 1
    ) THEN 'PURGE THE DEADLOCK'
    ELSE 'REACH THE COMMIT PORTAL'
END
WHERE id = 1 AND state = 'running';
-- TRACE_SOURCE_END state_resolution

-- Native audit rows. Every row below is committed or rolled back with the
-- gameplay changes it describes. Nothing here feeds back into game decisions.
INSERT INTO sql_trace_events(
    tick,priority,kind,source_tag,subject,before_value,after_value,detail
)
SELECT
    gc.tick,70,'input','input_mapping',
    printf('input_events[%d]',ie.id),
    printf('raw=X''%s''',upper(hex(ie.raw_code))),
    printf('action=%s',coalesce(kb.action,'unbound')),
    printf('captured_us=%d; SQL consumed_tick=%d',ie.captured_us,gc.tick)
FROM input_events AS ie
LEFT JOIN key_bindings AS kb ON kb.raw_code=ie.raw_code
JOIN game_clock AS gc ON gc.id=1
WHERE ie.consumed_tick IS NULL
ORDER BY ie.id;

INSERT INTO sql_trace_events(
    tick,priority,kind,source_tag,subject,before_value,after_value,detail
)
SELECT
    gc.tick,80,'motion','player_motion','player.position',
    printf('x=%.4f y=%.4f angle=%.2fdeg',
        old.x,old.y,old.angle*180.0/pi()),
    printf('x=%.4f y=%.4f angle=%.2fdeg',
        p.x,p.y,p.angle*180.0/pi()),
    printf(
        'tick_motion dx=%.5f dy=%.5f; actions fwd=%.2f back=%.2f left=%.2f right=%.2f turn_l=%.2f turn_r=%.2f',
        tm.dx,tm.dy,ta.forward,ta.backward,ta.strafe_left,ta.strafe_right,
        ta.turn_left,ta.turn_right
    )
FROM tick_player_before AS old
JOIN player AS p ON p.id=old.id
JOIN tick_motion AS tm ON tm.id=p.id
JOIN tick_actions AS ta ON ta.id=p.id
JOIN game_clock AS gc ON gc.id=1
WHERE ta.restart_pressed=0
  AND (
      abs(p.x-old.x)>0.000000001 OR abs(p.y-old.y)>0.000000001
      OR abs(p.angle-old.angle)>0.000000001
  );

INSERT INTO sql_trace_events(
    tick,priority,kind,source_tag,subject,before_value,after_value,detail
)
SELECT
    gc.tick,100,'shot','shot_resolution',
    CASE WHEN ts.target_id IS NULL THEN 'weapon / no target'
         ELSE printf('entities[%d].health',ts.target_id) END,
    CASE WHEN ts.target_id IS NULL
         THEN printf('ammo=%d; target=NULL',oldp.ammo)
         ELSE printf('ammo=%d; health=%d',oldp.ammo,olde.health) END,
    CASE WHEN ts.target_id IS NULL
         THEN printf('ammo=%d; target=NULL',p.ammo)
         ELSE printf('ammo=%d; health=%d; active=%d',p.ammo,e.health,e.active) END,
    printf('tick_shot fired=%d target_id=%s; event_log=%s damage=%d',
        ts.fired,coalesce(cast(ts.target_id AS TEXT),'NULL'),
        coalesce(ev.detail,'NONE'),coalesce(ev.value,0))
FROM tick_shot AS ts
JOIN game_clock AS gc ON gc.id=1
JOIN tick_player_before AS oldp ON oldp.id=1
JOIN player AS p ON p.id=1
LEFT JOIN tick_entity_before AS olde ON olde.entity_id=ts.target_id
LEFT JOIN entities AS e ON e.id=ts.target_id
LEFT JOIN event_log AS ev ON ev.tick=gc.tick AND ev.kind='shot'
WHERE ts.fired=1;

INSERT INTO sql_trace_events(
    tick,priority,kind,source_tag,subject,before_value,after_value,detail
)
SELECT
    gc.tick,95,'interaction','door_interaction',
    printf('doors[%d]',d.id),
    printf('locked=%d open_fraction=%.2f',old.locked,old.open_fraction),
    printf('locked=%d open_fraction=%.2f',d.locked,d.open_fraction),
    printf('event_log=%s; has_token=%d',ev.detail,p.has_token)
FROM event_log AS ev
JOIN game_clock AS gc ON gc.tick=ev.tick AND gc.id=1
JOIN doors AS d ON d.id=ev.target_id
JOIN tick_door_before AS old ON old.id=d.id
JOIN player AS p ON p.id=1
WHERE ev.kind='door';

INSERT INTO sql_trace_events(
    tick,priority,kind,source_tag,subject,before_value,after_value,detail
)
SELECT
    gc.tick,92,'pickup','pickup_resolution',ev.detail,
    printf('health=%d ammo=%d token=%d',old.health,old.ammo,old.has_token),
    printf('health=%d ammo=%d token=%d',p.health,p.ammo,p.has_token),
    printf('entity_id=%d value=%d',ev.source_id,coalesce(ev.value,0))
FROM event_log AS ev
JOIN game_clock AS gc ON gc.tick=ev.tick AND gc.id=1
JOIN tick_player_before AS old ON old.id=1
JOIN player AS p ON p.id=1
WHERE ev.kind='pickup';

INSERT INTO sql_trace_events(
    tick,priority,kind,source_tag,subject,before_value,after_value,detail
)
SELECT
    gc.tick,90,'damage','enemy_attack','player.health',
    printf('tick_start_health=%d',old.health),
    printf('tick_end_health=%d',p.health),
    printf('attackers=%s total_damage=%d',
        group_concat(ev.detail,','),sum(coalesce(ev.value,0)))
FROM event_log AS ev
JOIN game_clock AS gc ON gc.tick=ev.tick AND gc.id=1
JOIN tick_player_before AS old ON old.id=1
JOIN player AS p ON p.id=1
WHERE ev.kind='enemy_attack'
GROUP BY gc.tick,old.health,p.health;

INSERT INTO sql_trace_events(
    tick,priority,kind,source_tag,subject,before_value,after_value,detail
)
SELECT
    gc.tick,110,'state','state_resolution','game_status',
    printf('state=%s score=%d message=%s',old.state,old.score,old.message),
    printf('state=%s score=%d message=%s',gs.state,gs.score,gs.message),
    'authoritative game_status row changed inside this tick'
FROM tick_status_before AS old
JOIN game_status AS gs ON gs.id=old.id
JOIN game_clock AS gc ON gc.id=1
WHERE old.state<>gs.state OR old.score<>gs.score OR old.message<>gs.message;

DELETE FROM sql_trace_events
WHERE tick < (SELECT greatest(0,tick-180) FROM game_clock WHERE id=1)
  AND id NOT IN (
      SELECT max(id) FROM sql_trace_events GROUP BY kind
  );

UPDATE input_events
SET consumed_tick = (SELECT tick FROM game_clock WHERE id = 1)
WHERE consumed_tick IS NULL;

DELETE FROM input_events
WHERE captured_us < (SELECT now_us - 2000000 FROM game_clock WHERE id = 1);

UPDATE game_clock
SET seed = (seed * 1103515245 + 12345) % 2147483647
WHERE id = 1;
