with recursive

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_12/sample-3.data', header=false)
    -- from read_csv('advent_of_code/solutions/year_2024/day_12/input.data', header=false)
),

grid as (
    from input
    select
        generate_subscripts(split(data, NULL), 1) as x,
        row_number() over () as y,
        unnest(split(data, NULL)) as plant,
),

directions(direction) as (
    values
        ([ 0, -1]),
        ([ 1,  0]),
        ([ 0,  1]),
        ([-1,  0]),
),

/* Loop to compute the perimeter */
perimeters(x, y, plant, perimeter) as (
    from (
        from grid as o  /* outer */
            cross join directions
        select
            x,
            y,
            plant,
            direction,
            1 - exists(
                select *
                from grid as i  /* inner */
                where (o.x + direction[1], o.y + direction[2], o.plant) = (i.x, i.y, i.plant)
            )::int as perimeter_contribution
    )
    select x, y, any_value(plant), sum(perimeter_contribution)
    group by x, y
),

region_edges__right_down as (
        select
            grid.plant,
            'right' as edge_type,
            [grid.x, grid.y] as point_1,
            [r.x, r.y] as point_2,
        from grid
            inner join grid as r
                on  grid.x + 1 = r.x
                and grid.y = r.y
                and grid.plant = r.plant
    union all
        select
            grid.plant,
            'down' as edge_type,
            [grid.x, grid.y] as point_1,
            [d.x, d.y] as point_2,
        from grid
            inner join grid as d
                on  grid.x = d.x
                and grid.y + 1 = d.y
                and grid.plant = d.plant
),

region_edges as (
        select plant, edge_type, point_1, point_2
        from region_edges__right_down
    union all
        select plant, if(edge_type = 'down', 'up', 'left'), point_2, point_1
        from region_edges__right_down
),

/*
    TODO  Two recursive CTEs: on regions (pick any point), then on the point to fill the region
*/
regions(region_id, region, region_points, seen) as materialized (
        select
            null::uuid,
            null::varchar,
            []::int[][],
            []::int[][],
    union all (
        with recursive

        next_region as materialized (
            select
                uuid() as region_id,
                grid.plant as region,
                [grid.x, grid.y] as point,
                regions.seen as regions_seen,
            from grid, regions
            where not list_contains(regions.seen, [grid.x, grid.y])
            limit 1
        ),

        region_points as (
                select
                    region_id,
                    point,
                    [point] as seen,
                from next_region
            union all
                select
                    region_points.region_id,
                    region_edges.point_2,
                    (region_points.seen
                        .list_concat(list(region_edges.point_2) over (
                            partition by region_points.region_id
                        ))
                        .list_distinct()
                        .list_sort()
                    ),
                from region_points
                    inner join region_edges
                        on region_points.point = region_edges.point_1
                where not list_contains(region_points.seen, region_edges.point_2)
        )

        select
            region_id,
            next_region.region,
            points.region_points,
            list_concat(next_region.regions_seen, points.region_points),
        from next_region
            inner join (
                select region_id, list(distinct point) as region_points
                from region_points
                group by region_id
            ) as points
                using (region_id)
    )
),

region_areas as (
    select
        region_id,
        region,
        region_points,
        len(region_points) as area,
    from regions
    where region_id is not null
),

region_perimeters as (
    from (
        from (
            from region_areas
            select
                region_id,
                unnest(region_points) as point,
        )
        select
            region_id,
            point[1] as x,
            point[2] as y,
    ) inner join perimeters using (x, y)
    select
        region_id,
        sum(perimeter) as perimeter,
    group by region_id
)

select sum(area * perimeter)
from region_areas
    inner join region_perimeters
        using (region_id)
;
