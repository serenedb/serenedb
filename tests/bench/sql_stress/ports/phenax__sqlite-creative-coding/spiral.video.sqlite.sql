INSERT OR REPLACE INTO videos (id, width, height, fps) VALUES ('spiral', 400, 400, 20) RETURNING id;

WITH RECURSIVE
  video AS (SELECT * FROM videos WHERE id = 'spiral'),
  frames(frame) AS
    (SELECT 1 UNION ALL
      SELECT frame + 1 FROM frames, video WHERE frame < 60)
INSERT INTO images (id, width, height, frame, video_id)
  SELECT CONCAT('spiral/', frame), width, height, frame, 'spiral'
    FROM frames, video;

WITH RECURSIVE
  video AS
    (SELECT * FROM videos WHERE id = 'spiral'),
  horizontal(x) AS
    (SELECT width FROM video UNION ALL SELECT x - 1 FROM horizontal WHERE x > 1),
  vertical(y) AS
    (SELECT height FROM video UNION ALL SELECT y - 1 FROM vertical WHERE y > 1),
  frames(id, frame) AS
    (SELECT id, frame FROM images WHERE video_id = (SELECT id FROM video)),
  theta_increments(inc) AS
    (SELECT -14 UNION ALL SELECT inc + 1 FROM theta_increments WHERE inc < 14),
  theta_increments_2(inc) AS
    (SELECT -14.5 UNION ALL SELECT * FROM theta_increments UNION ALL SELECT 14.5),
  _pixels(frame_id, x, y, r, g, b) AS
    (SELECT
      frame_id,
      x, y,
      LEAST(255, ROUND(50.0 * value + 15)),
      LEAST(255, ROUND(200.0 * value*value*value) + 15),
      LEAST(255, ROUND(200.0 * value*value*value) + 8)
    FROM  (SELECT *,
            frames.id AS frame_id,
            (WITH
              coords AS (SELECT
                          (x - width/2.0)/100.0 AS xval,
                          (y - height/2.0)/100.0 AS yval),
              theta AS  (SELECT
                          (CASE WHEN xval = 0 THEN PI()/2 ELSE ATAN(yval/xval) END) AS theta,
                          (0.05 * frame) / (SELECT COUNT(*) FROM frames) + 0.2 AS factor,
                          1.5 AS scale,
                          0.06 AS thicc
                         FROM coords),
              deltas AS (SELECT
                           SQRT(
                             POW(scale*SIN(factor*(theta + inc*PI()))*COS((theta + inc*PI())) - xval, 2) +
                             POW(scale*SIN(factor*(theta + inc*PI()))*SIN((theta + inc*PI())) - yval, 2)
                           ) AS delta
                         FROM theta, coords, theta_increments_2 ORDER BY ABS(inc) ASC)
            VALUES  (IFNULL((SELECT
                               1.0 - (delta / thicc)
                              FROM deltas, theta
                              WHERE delta <= thicc
                              LIMIT 1), 0))) AS value
      FROM vertical, horizontal, video, frames))
INSERT INTO pixels (image_id, x, y, r, g, b) SELECT frame_id, x, y, r, g, b FROM _pixels;
