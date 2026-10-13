WITH data AS (
    SELECT CAST(replace(replace(column0, 'L', '-'), 'R','') AS INTEGER) as sequence, ROW_NUMBER() OVER() as rnr, abs(sequence) // 100 as extra_zeros
    FROM read_csv_auto('01/example.csv', header = false, delim = '')
),
path as (
    WITH RECURSIVE lock_rec (
        RecursionDepth,
        lock_sequence,
        lock_sum,
        indic
    ) AS (

        -- Base case
        SELECT 1  AS RecursionDepth,
               0  AS lock_sequence,
               50 AS lock_sum,
               0 AS indic
        UNION ALL
        -- Recursive step
        SELECT RecursionDepth + 1,
               data.sequence,
               if(lock_rec.lock_sum + data.sequence < 0, ((lock_rec.lock_sum + data.sequence) % 100 + 100) % 100, (lock_rec.lock_sum + data.sequence) % 100) AS lock_sum,
                if(lock_rec.lock_sum != 0 and (lock_rec.lock_sum + (data.sequence % 100) < 0 OR lock_rec.lock_sum + (data.sequence % 100) > 100), 1 + extra_zeros, extra_zeros) AS indic
        FROM lock_rec JOIN data ON data.rnr = lock_rec.RecursionDepth
        WHERE lock_rec.RecursionDepth < 5000
    )
    SELECT * FROM lock_rec
)
SELECT
    SUM(
        if(lock_sum == 0, 1, 0)
    ) AS part1,
    SUM(
        if(lock_sum == 0, 1 + indic, indic)
    ) AS part2
FROM path
