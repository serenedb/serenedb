WITH input AS (
    SELECT
        split(split(cubes, ':')[1], ' ')[2]::int AS game_id,
        split(replace(split(cubes, ': ')[2], ';', ','), ', ') AS cubes
    FROM read_csv('./2023/02/input.csv', columns = { cubes: text }, DELIM = '', header=false)
), unnested AS (
    SELECT
        game_id,
        unnest(cubes) AS cubes
    FROM input
), cubes AS (
    SELECT
        game_id,
        split(cubes, ' ')[1]::int AS cubes_amount,
        split(cubes, ' ')[2] AS cubes_color
    FROM unnested
), impossible_games AS (
                SELECT game_id FROM cubes WHERE (cubes_color = 'red' and cubes_amount > 12)
    UNION ALL   SELECT game_id FROM cubes WHERE (cubes_color = 'green' and cubes_amount > 13)
    UNION ALL   SELECT game_id FROM cubes WHERE (cubes_color = 'blue' and cubes_amount > 14)
), possible_cubes AS (
    SELECT
        game_id,
        cubes_color,
        max(cubes_amount) AS min_cubes
    FROM cubes
    GROUP BY 1, 2
), powers AS (
    SELECT
        game_id,
        product(min_cubes)::int AS _power
    FROM possible_cubes
    GROUP BY 1
)
SELECT 'Part I' AS part, sum(DISTINCT game_id) FILTER (game_id NOT IN (SELECT game_id FROM impossible_games)) AS answer FROM cubes 
UNION ALL
SELECT 'Part II' AS part, sum(_power) AS answer FROM powers;
