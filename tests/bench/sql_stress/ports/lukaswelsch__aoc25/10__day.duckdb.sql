WITH data AS (
    SELECT string_split_regex(column0, ' ') as arr, ROW_NUMBER() OVER() as rowid
    FROM read_csv_auto('10/example.csv', header = false, delim='')
),
splitted AS(
    SELECT
        unnest(arr) as cell,
        generate_subscripts(arr, 1) AS button_id,
        rowid
    FROM data
),
buttons AS (
    SELECT *,
           translate(cell, '.#[]', '01') AS button
    FROM splitted
    WHERE cell not like '{%'
    AND button_id=1
),
button_presses AS (
    SELECT *
    FROM splitted
    WHERE cell not like '{%'
    AND button_id!=1
),
combined_table AS (
    SELECT buttons.cell as button_cell, rowid, button, button_presses.cell as press_cell
    FROM buttons
    JOIN button_presses USING(rowid)
    ORDER BY rowid
),
parsed AS (
    SELECT
        rowid,
        button_cell,
        press_cell,
        button,
        regexp_extract_all(press_cell, '[0-9]+')::INT[] AS press
    FROM combined_table
),
maxbit AS (
    SELECT unnest(press) AS maxbit FROM parsed
),
bitmap AS(
    SELECT
        rowid,
        button_cell,
        button,
        press_cell,
        (
            SELECT string_agg(
                CASE WHEN i = ANY(press) THEN 1 ELSE 0 END, ''
            )
            FROM range((SELECT max(maxbit) FROM maxbit)+1) t(i)
        )::BIT AS bits,
        ROW_NUMBER() OVER() as id
    FROM parsed
),
rec_check AS (
    WITH RECURSIVE base AS (
        SELECT
        rowid, button, id, bits, bits AS button_presses, 1 AS size
        FROM bitmap
        UNION ALL
        SELECT
        b.rowid, b.button, x.id, x.bits, xor(b.button_presses, x.bits) AS button_presses, b.size + 1 AS size
        FROM base b
        JOIN bitmap x
        ON x.rowid = b.rowid
        AND b.id < x.id
    )
    SELECT rowid, button, id, bits, button_presses::STRING as button_presses, size FROM base
),
min_combinations AS (
    SELECT MIN(size) as num_configs, rowid
    FROM rec_check
    WHERE button == button_presses[:len(button)]
    GROUP BY rowid
)
SELECT SUM(num_configs)
FROM min_combinations


