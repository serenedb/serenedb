INSERT OR REPLACE INTO videos (id, width, height, fps) VALUES ('rave', 400, 400, 10) RETURNING id;

WITH RECURSIVE
  video AS (SELECT * FROM videos WHERE id = 'rave'),
  _frames(frame) AS (
    SELECT 1 UNION ALL
    SELECT frame + 1 FROM _frames, video WHERE frame < 50
  )
INSERT INTO images (id, width, height, frame, video_id) SELECT CONCAT('rave/', frame), width, height, frame, 'rave' FROM _frames, video;

WITH RECURSIVE
  video AS (SELECT * FROM videos WHERE id = 'rave'),
  horizontal(x) AS (SELECT width FROM video UNION ALL SELECT x - 1 FROM horizontal WHERE x > 1),
  vertical(y) AS (SELECT height FROM video UNION ALL SELECT y - 1 FROM vertical WHERE y > 1),
  _frames(id, frame) AS (SELECT id, frame FROM images WHERE video_id = (SELECT id FROM video)),
  _pixels(frame_id, x, y, r, g, b) AS
    (SELECT
      frames_id,
      x, y,
      ROUND(255 * value)*middle::int,
      ROUND(20 * value),
      ROUND(180 * value)*(NOT middle)::int + 75
    FROM (SELECT *,
        _frames.id AS frames_id,
        (CASE
          WHEN SIN((x - 200.0)*(x - 200.0)*0.0004) + TAN((y - 200.0)*(y - 200.0)*0.0004) - 0.1 < COS((10.0 + frame)*(x - 200.0)/(y - 200.0)) THEN 1.0
          ELSE 0.0
        END) AS value,
        (y > 140 AND y < 260) AS middle
      FROM vertical, horizontal, _frames)
  )
INSERT INTO pixels (image_id, x, y, r, g, b) SELECT frame_id, x, y, r, g, b FROM _pixels;
