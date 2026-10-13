SET integer_division = true;
-- AoC 2022, Day 15 (Part 1)

WITH RECURSIVE
  input(row, line) AS (
    SELECT row_number() OVER () AS row
         , c.line
      FROM read_csv_auto('2022/15/input.txt', SEP='', header=false) AS c(line)
  ),

  sensors(sensor, beacon, distance) AS (
    SELECT { 'x': parts[2] :: int, 'y': parts[3] :: int } AS sensor
         , { 'x': parts[4] :: int, 'y': parts[5] :: int } AS beacon
         , ABS(sensor['x'] - beacon['x']) + ABS(sensor['y'] - beacon['y']) AS distance
      FROM input AS i, (SELECT string_split_regex(i.line, 'Sensor at x=|, y=|: closest beacon is at x=')) AS _(parts)
  ),

  interests(position, layer_change, idx) AS (
    SELECT item.position     AS position
         , item.layer_change AS layer_change
         , row_number() OVER (ORDER BY (position, layer_change)) AS idx
      FROM (SELECT unnest(
                    [ { 'position': s.sensor."x" - (s.distance - diff), 'layer_change': +1 }
                    , { 'position': s.sensor."x" + (s.distance - diff), 'layer_change': -1 }
                    ]) AS item
              FROM sensors AS s, (SELECT ABS(s.sensor."y" - 2000000) AS diff)
             WHERE diff <= s.distance)
  ),

  sweepline(idx, last_position, layer, count) AS (
    SELECT 1, NULL, 0, 1
      UNION ALL
    SELECT i.idx + 1
         , i.position
         , s.layer + i.layer_change
         , CASE WHEN i.layer_change < 0 OR s.layer > 0
                THEN s.count + i.position - s.last_position
                ELSE s.count
           END
      FROM sweepline AS s
         , interests AS i
     WHERE i.idx = s.idx
  ),

  overapping_features(count) AS (
    SELECT COUNT(DISTINCT s.beacon)
      FROM sensors AS s
     WHERE s.beacon."y" = 2000000
  )

SELECT (last_value(s.count) OVER (ORDER BY s.idx DESC)) - o.count AS n_blocked
  FROM sweepline           AS s
     , overapping_features AS o
 LIMIT 1;
