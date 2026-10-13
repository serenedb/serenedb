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
        ('east', 0, 0, 'X'),
        ('east', 1, 0, 'M'),
        ('east', 2, 0, 'A'),
        ('east', 3, 0, 'S'),
        ('south-east', 0, 0, 'X'),
        ('south-east', 1, 1, 'M'),
        ('south-east', 2, 2, 'A'),
        ('south-east', 3, 3, 'S'),
        ('south', 0, 0, 'X'),
        ('south', 0, 1, 'M'),
        ('south', 0, 2, 'A'),
        ('south', 0, 3, 'S'),
        ('south-west',  0, 0, 'X'),
        ('south-west', -1, 1, 'M'),
        ('south-west', -2, 2, 'A'),
        ('south-west', -3, 3, 'S'),
        ('west',  0, 0, 'X'),
        ('west', -1, 0, 'M'),
        ('west', -2, 0, 'A'),
        ('west', -3, 0, 'S'),
        ('north-west',  0,  0, 'X'),
        ('north-west', -1, -1, 'M'),
        ('north-west', -2, -2, 'A'),
        ('north-west', -3, -3, 'S'),
        ('north',  0,  0, 'X'),
        ('north',  0, -1, 'M'),
        ('north',  0, -2, 'A'),
        ('north',  0, -3, 'S'),
        ('north-east',  0,  0, 'X'),
        ('north-east',  1, -1, 'M'),
        ('north-east',  2, -2, 'A'),
        ('north-east',  3, -3, 'S'),
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
    having 4 = count_if(letter_found)
)
select count(*)
;
