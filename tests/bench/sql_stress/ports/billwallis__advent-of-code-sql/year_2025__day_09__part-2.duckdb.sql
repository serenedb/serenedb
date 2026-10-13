set memory_limit = '64GB';
with

/*
    0,0  1,0  ...
    0,1  1,1  ...
    ...  ...  ...
*/

input(p, n) as (
    from read_csv('advent_of_code/solutions/year_2025/day_09/sample.data', header=false) as i(x, y)
    select {x: x, y: y}, row_number() over ()
),

vertices as (
        from input
    union all
        select p, 1 + (select max(n) from input)
        from input
        where n = 1
),

edges as (
    select
        if(l.p < r.p, l.p, r.p) as v1,
        if(l.p > r.p, l.p, r.p) as v2,
        if(l.p.x = r.p.x, 'x', 'y') as axis,
        if(l.p.x = r.p.x, l.p.x, l.p.y) as axis_coord,
    from vertices as l
        inner join vertices as r
            on l.n + 1 = r.n
),

squares as (
    select
        l.p as p_ll,
        r.p as p_rr,
        {x: l.p.x, y: r.p.y} as p_lr,
        {x: r.p.x, y: l.p.y} as p_rl,
        (1
            * (1 + abs(l.p.x - r.p.x))
            * (1 + abs(l.p.y - r.p.y))
        ) as area,
    from vertices as l
        inner join vertices as r
            on l.p < r.p
),

/*
    A vertex is bounded if, either:

    - it is already on an edge
    - it is not on an edge and has an odd number of edges in each direction
*/
all_vertices(p) as (
        select p_ll from squares
    union
        select p_rr from squares
    union
        select p_lr from squares
    union
        select p_rl from squares
),
vertices_on_edges as (
        from all_vertices as v
            semi join edges
                on  edges.axis = 'x'
                and v.p.x = edges.axis_coord
                and v.p.y between edges.v1.y and edges.v2.y
    union
        from all_vertices as v
            semi join edges
                on  edges.axis = 'y'
                and v.p.y = edges.axis_coord
                and v.p.x between edges.v1.x and edges.v2.x
),
strictly_bounded_vertices as (
        select v.p
        from all_vertices as v
            anti join vertices_on_edges using (p)
            inner join lateral (
                select count(*) as north_edges
                -- select list({v1: v1, v2: v2}) as north_edges
                from edges
                where 0=1
                    or (1=1
                        and edges.axis = 'y'
                        and v.p.y > edges.axis_coord
                        and v.p.x between least(edges.v1.x, edges.v2.x)
                                      and greatest(edges.v1.x, edges.v2.x)
                    )
                    or (1=1
                        and edges.axis = 'x'
                        and v.p.x = edges.axis_coord
                        and v.p.y > greatest(edges.v1.y, edges.v2.y)
                    )
            ) as north
                on north.north_edges % 2 = 1
            inner join lateral (
                select count(*) as south_edges
                -- select list({v1: v1, v2: v2}) as south_edges
                from edges
                where 0=1
                    or (1=1
                        and edges.axis = 'y'
                        and v.p.y < edges.axis_coord
                        and v.p.x between least(edges.v1.x, edges.v2.x)
                                      and greatest(edges.v1.x, edges.v2.x)
                    )
                    or (1=1
                        and edges.axis = 'x'
                        and v.p.x = edges.axis_coord
                        and v.p.y < least(edges.v1.y, edges.v2.y)
                    )
            ) as south
                on south.south_edges % 2 = 1
            inner join lateral (
                select count(*) as east_edges
                -- select list({v1: v1, v2: v2}) as east_edges
                from edges
                where 0=1
                    or (1=1
                        and edges.axis = 'x'
                        and v.p.x < edges.axis_coord
                        and v.p.y between least(edges.v1.y, edges.v2.y)
                                      and greatest(edges.v1.y, edges.v2.y)
                    )
                    or (1=1
                        and edges.axis = 'y'
                        and v.p.y = edges.axis_coord
                        and v.p.x < least(edges.v1.x, edges.v2.x)
                    )
            ) as east
                on east.east_edges % 2 = 1
            inner join lateral (
                select count(*) as west_edges
                -- select list({v1: v1, v2: v2}) as west_edges
                from edges
                where 0=1
                    or (1=1
                        and edges.axis = 'x'
                        and v.p.x > edges.axis_coord
                        and v.p.y between least(edges.v1.y, edges.v2.y)
                                      and greatest(edges.v1.y, edges.v2.y)
                    )
                    or (1=1
                        and edges.axis = 'y'
                        and v.p.y = edges.axis_coord
                        and v.p.x > greatest(edges.v1.x, edges.v2.x)
                    )
            ) as west
                on west.west_edges % 2 = 1
),
bounded_vertices as (
        from vertices_on_edges
    union
        from strictly_bounded_vertices
)

select max(area)
from squares
    /* All vertices are bounded */
    semi join bounded_vertices as v_ll on squares.p_ll = v_ll.p
    semi join bounded_vertices as v_rr on squares.p_rr = v_rr.p
    semi join bounded_vertices as v_lr on squares.p_lr = v_lr.p
    semi join bounded_vertices as v_rl on squares.p_rl = v_rl.p
where 1=1
    /* No edges intersect the perimeter */
    and not exists(
        from edges
        where if(
            squares.p_ll.x = squares.p_lr.x,
            (1=1
                and edges.axis = 'y'
                and squares.p_ll.x > least(edges.v1.x, edges.v2.x)
                and squares.p_ll.x < greatest(edges.v1.x, edges.v2.x)
                and edges.axis_coord > least(squares.p_ll.y, squares.p_lr.y)
                and edges.axis_coord < greatest(squares.p_ll.y, squares.p_lr.y)
            ),
            (1=1
                and edges.axis = 'x'
                and squares.p_ll.y > least(edges.v1.y, edges.v2.y)
                and squares.p_ll.y < greatest(edges.v1.y, edges.v2.y)
                and edges.axis_coord > least(squares.p_ll.x, squares.p_lr.x)
                and edges.axis_coord < greatest(squares.p_ll.x, squares.p_lr.x)
            )
        )
    )
    and not exists(
        from edges
        where if(
            squares.p_ll.x = squares.p_rl.x,
            (1=1
                and edges.axis = 'y'
                and squares.p_ll.x > least(edges.v1.x, edges.v2.x)
                and squares.p_ll.x < greatest(edges.v1.x, edges.v2.x)
                and edges.axis_coord > least(squares.p_ll.y, squares.p_rl.y)
                and edges.axis_coord < greatest(squares.p_ll.y, squares.p_rl.y)
            ),
            (1=1
                and edges.axis = 'x'
                and squares.p_ll.y > least(edges.v1.y, edges.v2.y)
                and squares.p_ll.y < greatest(edges.v1.y, edges.v2.y)
                and edges.axis_coord > least(squares.p_ll.x, squares.p_rl.x)
                and edges.axis_coord < greatest(squares.p_ll.x, squares.p_rl.x)
            )
        )
    )
    and not exists(
        from edges
        where if(
            squares.p_rr.x = squares.p_lr.x,
            (1=1
                and edges.axis = 'y'
                and squares.p_rr.x > least(edges.v1.x, edges.v2.x)
                and squares.p_rr.x < greatest(edges.v1.x, edges.v2.x)
                and edges.axis_coord > least(squares.p_rr.y, squares.p_lr.y)
                and edges.axis_coord < greatest(squares.p_rr.y, squares.p_lr.y)
            ),
            (1=1
                and edges.axis = 'x'
                and squares.p_rr.y > least(edges.v1.y, edges.v2.y)
                and squares.p_rr.y < greatest(edges.v1.y, edges.v2.y)
                and edges.axis_coord > least(squares.p_rr.x, squares.p_lr.x)
                and edges.axis_coord < greatest(squares.p_rr.x, squares.p_lr.x)
            )
        )
    )
    and not exists(
        from edges
        where if(
            squares.p_rr.x = squares.p_rl.x,
            (1=1
                and edges.axis = 'y'
                and squares.p_rr.x > least(edges.v1.x, edges.v2.x)
                and squares.p_rr.x < greatest(edges.v1.x, edges.v2.x)
                and edges.axis_coord > least(squares.p_rr.y, squares.p_rl.y)
                and edges.axis_coord < greatest(squares.p_rr.y, squares.p_rl.y)
            ),
            (1=1
                and edges.axis = 'x'
                and squares.p_rr.y > least(edges.v1.y, edges.v2.y)
                and squares.p_rr.y < greatest(edges.v1.y, edges.v2.y)
                and edges.axis_coord > least(squares.p_rr.x, squares.p_rl.x)
                and edges.axis_coord < greatest(squares.p_rr.x, squares.p_rl.x)
            )
        )
    )
;


-- 4600181596 is too high  ["{x=15543, y=83949}","{x=83871, y=16626}","{x=15543, y=16626}","{x=83871, y=83949}"]
--  176823432 is too low   ["{x=2032, y=50331}","{x=94803, y=48426}","{x=2032, y=48426}","{x=94803, y=50331}"]
