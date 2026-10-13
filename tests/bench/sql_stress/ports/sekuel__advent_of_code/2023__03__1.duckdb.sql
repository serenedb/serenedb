WITH input AS (
    SELECT 
        row_number() OVER () AS _row,
        string_split_regex(parts, '[^\d]') AS val_num,
        string_split_regex(parts, '') AS val_all
    FROM read_csv('./2023/03/input.txt', columns = { parts: text }, delim = '', header=false)
), stg_num_grid AS (
    SELECT
        _row,
        unnest(val_num) AS val,
        generate_subscripts(val_num, 1) AS _col
    FROM input
), num_grid AS (
    SELECT 
        _row,
        _col,
        coalesce(_col + (sum(len(val)) OVER wdw), _col) AS _col_start,
        LEN(val) - 1 + coalesce(_col + (sum(len(val)) OVER wdw), _col) AS _col_end,
        val::int AS val
    FROM stg_num_grid
    WHERE val != ''
    WINDOW wdw AS (
        PARTITION BY _row 
        ORDER BY _col 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
    ORDER BY _row, _col
), stg_symbol_grid AS (
    SELECT 
        _row,
        generate_subscripts(val_all, 1) AS _col,
        unnest(val_all) AS val_all,
    FROM input
), symbol_grid AS (
    SELECT 
        _row,
        _col AS _col_symbol,
        val_all AS symbol
    FROM stg_symbol_grid 
    WHERE regexp_matches(val_all, '[^\d|^\.]')
), joined AS (
    SELECT
        symbol_grid._row AS _row,
        num_grid._col_start,
        num_grid._col_end,
        num_grid.val,
        symbol_grid._col_symbol,
        symbol_grid.symbol
    FROM num_grid
    LEFT JOIN symbol_grid 
        ON (
            num_grid._row = symbol_grid._row
            AND (
                num_grid._col_start = symbol_grid._col_symbol + 1
                OR num_grid._col_end = symbol_grid._col_symbol - 1
            )
        )
        OR (
            num_grid._row = symbol_grid._row - 1
            AND (
                symbol_grid._col_symbol BETWEEN num_grid._col_start AND num_grid._col_end
                OR num_grid._col_start = symbol_grid._col_symbol + 1
                OR num_grid._col_end = symbol_grid._col_symbol - 1
            )
        )
        OR (
            num_grid._row = symbol_grid._row + 1
            AND (
                symbol_grid._col_symbol BETWEEN num_grid._col_start AND num_grid._col_end
                OR num_grid._col_start = symbol_grid._col_symbol + 1
                OR num_grid._col_end = symbol_grid._col_symbol - 1
            )
        )
), gear_ratios AS (
    SELECT 
        _row,
        _col_symbol,
        count(*) AS gear_count,
        product(val)::int AS gear_ratio
    FROM joined
    WHERE symbol = '*'
    GROUP BY 1,2
)
SELECT 'Part I' AS part, sum(val) FILTER (symbol IS NOT NULL) AS answer FROM joined
UNION ALL
SELECT 'Part II' AS part, sum(gear_ratio) FILTER (gear_count = 2) AS answer FROM gear_ratios;