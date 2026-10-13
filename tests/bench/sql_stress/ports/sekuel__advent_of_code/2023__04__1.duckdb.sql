WITH RECURSIVE input AS (
    SELECT 
        split(split_part(cards, ':', 1), ' ')[-1]::int AS id,
        split(split_part(split_part(cards, ': ', 2), ' | ', 1), ' ')  AS winnings,
        split(split_part(split_part(cards, ':', 2), ' | ', 2), ' ') AS val,
    FROM read_csv('./2023/04/input.csv', columns = { cards: text }, sep = '', header=false) 
), calc_points AS (
    SELECT
        *,
        list_filter(list_intersect(winnings, val), x -> x != '') AS matches,
        list_unique(matches)::int AS matches_count,
        IF(matches_count > 0, POWER(2, matches_count - 1), 0)::int AS points,
        range(id + 1, id + matches_count + 1, 1) AS copies
    FROM input
), unnested_copies AS (
    SELECT
        id,
        copy_id
    FROM calc_points
    LEFT JOIN unnest(copies) c(copy_id) ON TRUE
), scratchcards AS (
    SELECT
        id,
        copy_id
    FROM unnested_copies
    UNION ALL
    SELECT
        unnested_copies.id,
        unnested_copies.copy_id
    FROM scratchcards
    JOIN unnested_copies 
        ON scratchcards.copy_id = unnested_copies.id
        AND unnested_copies.copy_id IS NOT NULL
)
SELECT 'Part I' AS part, sum(points) AS answer FROM calc_points
UNION ALL
SELECT 'Part II' AS part, (count(DISTINCT id) + count(copy_id)) AS answer FROM scratchcards;