WITH RECURSIVE seeds AS (
    SELECT unnest(seed)::bigint AS seed
    FROM (
        SELECT split(split_part(column0, ': ', 2), ' ') AS seed 
        FROM read_csv_auto('./2023/05/input.csv', header=false) LIMIT 1
    )
), maps AS (
    SELECT 
        id,
        replace(first(column0) OVER (PARTITION BY id), 'map:', '') AS map_name,
        split_part(column0, ' ', 1)::BIGINT AS dst_start,
        split_part(column0, ' ', 2)::BIGINT AS src_start,
        split_part(column0, ' ', 3)::BIGINT AS range_length
    FROM (
        SELECT 
            column0, 
            regexp_matches(column0, '[\d]') checks,
            sum(CASE WHEN checks = false THEN 1 ELSE 0 END) OVER (ROWS UNBOUNDED PRECEDING) AS id
        FROM read_csv_auto('./2023/05/input.csv', skip='2', header=false)
        WHERE column0 IS NOT NULL
    )
    QUALIFY regexp_matches(column0, '[\d]')
), ranges AS (
    SELECT 
        *, 
        src_start+range_length-1 AS src_end
    FROM maps
), walk AS (
    SELECT
        1 AS id,
        seed,
        map_name,
        seed AS val, 
        coalesce( dst_start + (seed - src_start), seed) AS _next
    FROM seeds
    LEFT JOIN ranges 
        ON seeds.seed BETWEEN ranges.src_start AND ranges.src_end
        AND ranges.id = 1
    UNION
    SELECT
        walk.id + 1,
        seed,
        ranges.map_name,
        walk._next AS val,
        coalesce(ranges.dst_start + (walk._next - ranges.src_start), walk._next) AS _next
    FROM walk
    LEFT JOIN ranges 
        ON walk._next BETWEEN ranges.src_start AND ranges.src_end
        AND ranges.id = walk.id + 1
    WHERE walk.id < 7
)
SELECT 'Part I' AS part, min(_next) AS answer FROM walk WHERE id = 7;
