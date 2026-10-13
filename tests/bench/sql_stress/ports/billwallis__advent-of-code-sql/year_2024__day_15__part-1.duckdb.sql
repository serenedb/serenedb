with recursive

input(data) as (
    select *, row_number() over () as row_id,
    from read_csv('advent_of_code/solutions/year_2024/day_15/sample-1.data', header=false)
),

directions(move, direction) as (
    values
        ('^', [ 0, -1]),
        ('>', [ 1,  0]),
        ('v', [ 0,  1]),
        ('<', [-1,  0]),
),

warehouse as (
    from (
        from input
        select data
        where row_id < (select row_id from input where data is null)
    )
    select
        generate_subscripts(split(data, NULL), 1) as x,
        row_number() over () as y,
        unnest(split(data, NULL)) as tile,
),

moves as (
    from (
        from (
            from input
            select string_agg(data, '') as moves_
            where row_id > (select row_id from input where data is null)
        )
        select
            generate_subscripts(split(moves_, NULL), 1) as move_id,
            unnest(split(moves_, NULL)) as move,
    ) natural inner join directions
    select move_id, direction
),

walk as (
        select 0 as i, x, y, tile,
        from warehouse
    -- union all (
    --     /* bruh there's a lot to do inside here :melt: */
    -- )
)

from walk
;


/*
    The issue with recursive CTEs is that every intermediate result is
    _always_ appended to the CTE

    This is often helpful, but in a case like this, it just balloons the
    memory usage because I'd only care about the _latest_ intermediate
    result

    There's probably a smart way around this, but alas, I'm not smart
    enough
*/
