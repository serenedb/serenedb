with

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_04/sample.data', header=false)
),

grid as (
    select
        unnest(split(data, NULL)) as letter,
        row_number() over () as row_id,
        generate_subscripts(split(data, NULL), 1) AS col_id,
    from input
),

/*
    Rows increment to the right (->)
    Columns increment down (v)
*/
search_directions(direction, row_i, col_i, letter_to_match) as (
    values
        /*
            M . .
            . A .
            . . S
        */
        ('0-degrees', -1, -1, 'M'),
        ('0-degrees',  0,  0, 'A'),
        ('0-degrees',  1,  1, 'S'),

        /*
            . . M
            . A .
            S . .
        */
        ('90-degrees',  1, -1, 'M'),
        ('90-degrees',  0,  0, 'A'),
        ('90-degrees', -1,  1, 'S'),

        /*
            S . .
            . A .
            . . M
        */
        ('180-degrees',  1,  1, 'M'),
        ('180-degrees',  0,  0, 'A'),
        ('180-degrees', -1, -1, 'S'),

        /*
            . . S
            . A .
            M . .
        */
        ('270-degrees', -1,  1, 'M'),
        ('270-degrees',  0,  0, 'A'),
        ('270-degrees',  1, -1, 'S'),
),

searches as (
    select
        row_id,
        col_id,
        direction,
        exists(
            from grid as i  /* inner (i for brevity) */
            where (o.row_id,           o.col_id,           o.letter_to_match)
                = (i.row_id + o.row_i, i.col_id + o.col_i, i.letter)
        ) as letter_found,
    from (from grid cross join search_directions) as o  /* outer (o for brevity) */
)

from (
    select row_id, col_id, direction
    from searches
    group by row_id, col_id, direction
    having 3 = count_if(letter_found)
    qualify 2 = count(*) over (partition by row_id, col_id)
)
select count(distinct (row_id, col_id))
;
