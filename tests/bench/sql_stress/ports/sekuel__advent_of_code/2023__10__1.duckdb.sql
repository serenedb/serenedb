WITH RECURSIVE input AS (
    SELECT
        unnest(split(column0, NULL)) AS pipe,
        generate_subscripts(split(column0, NULL), 1) AS x,
        row_number() OVER () AS y
    FROM read_csv_auto('./2023/10/input.txt', sep = '', header=false)
), directions (pipe, ns, ew) AS (
    VALUES
        ('-',  0, -1), ('-', 0,  1),
        ('|', -1,  0), ('|', 1,  0),
        ('L', -1,  0), ('L', 0,  1),
        ('J', -1,  0), ('J', 0, -1),
        ('7',  1,  0), ('7', 0, -1),
        ('F',  1,  0), ('F', 0,  1)
), pipes AS MATERIALIZED (
    SELECT
        maps.pipe AS curr_pipe,
        maps.x AS curr_x,
        maps.y AS curr_y,
        _next.pipe,
        _next.x,
        _next.y,
        1::int AS distance,
    FROM input _next
    JOIN directions ON _next.pipe = directions.pipe
    JOIN input maps ON _next.x = maps.x + directions.ew AND _next.y = maps.y + directions.ns
    WHERE maps.pipe = 'S'
    UNION ALL
    SELECT
        _curr.pipe,
        _curr.x,
        _curr.y,
        _next.pipe,
        _next.x,
        _next.y,
        distance + 1,
    FROM pipes AS _curr
    JOIN directions ON _curr.pipe = directions.pipe
    JOIN input AS _next 
        ON (_next.x != _curr.curr_x OR _next.y != _curr.curr_y) 
        AND _next.x = _curr.x + directions.ew AND _next.y = _curr.y + directions.ns
), visited AS MATERIALIZED (
    SELECT DISTINCT
        input.pipe,
        input.x, input.y,
        IF(pipes.pipe IS NOT NULL, true, false) AS is_visited
    FROM input 
    LEFT JOIN (SELECT DISTINCT pipe, x, y FROM pipes) pipes USING (x, y)
    ORDER BY y, x
), scanlines AS MATERIALIZED (
    SELECT
        *,
        sum(1) FILTER (is_visited AND pipe IN ('|', 'J', 'L')) OVER (PARTITION BY y ORDER BY x) % 2 = 1 AS is_odd
    FROM visited
), nicer_pipes(pipe, nice_pipe) AS (
    VALUES
        ('S', 'S'),
        ('.', '·'),
        ('-', '─'),
        ('|', '│'),
        ('F', '╭'),
        ('7', '╮'),
        ('L', '╰'),
        ('J', '╯')
), maze AS MATERIALIZED (
    SELECT 
        y, 
        string_agg(
            CASE 
                WHEN is_odd AND NOT is_visited THEN '🦆'
                ELSE nice_pipe
            END
            , '' ORDER BY x) AS nicer_pipe
    FROM scanlines
    JOIN nicer_pipes USING (pipe)
    GROUP BY 1
)
SELECT 'Part I' AS part, [(max(distance) // 2)] AS answer FROM pipes
UNION ALL
SELECT 'Part II' AS part, [count(*) FILTER (is_odd AND NOT is_visited)] AS answer FROM scanlines
UNION ALL
SELECT 'Maze' AS part, list(maze.nicer_pipe ORDER BY maze.y) AS answer FROM maze;