with recursive

input(n, p) as (
    from read_csv('advent_of_code/solutions/year_2025/day_08/sample.data', header=false, filename=true) as i(x, y, z)
    select
        if(filename.split('/')[-1] = 'sample.data', 10, 1000),
        {x: x, y: y, z: z}
),

distances as (
    select
        l.p,
        r.p as q,
        sqrt(0
            + power(l.p.x - r.p.x, 2)
            + power(l.p.y - r.p.y, 2)
            + power(l.p.z - r.p.z, 2)
        ) as distance,
        row_number() over (order by distance) as distance_id,
    from input as l
        inner join input as r
            on l.p < r.p
    qualify distance_id <= l.n
),

/* `n` is just a dummy ID which allows us to merge the CTE results, rather than append them */
circuits(n, i, circuit) using key (n) as (
        select row_number() over (), 0, [p]
        from (select p from distances union select q from distances)
    union (
        with

        next_connection as (
            select p, q
            from distances
            where distance_id = (select circuits.i + 1 from circuits limit 1)
        ),

        new_circuits as (
                /* Circuits not connected in this iteration */
                select i, circuit
                from circuits
                where not exists(
                    from next_connection
                    where 0=1
                        or list_contains(circuits.circuit, next_connection.p)
                        or list_contains(circuits.circuit, next_connection.q)
                )
            union all
                /* Circuits connected in this iteration */
                select
                    any_value(i),
                    flatten(list(circuit)),
                from circuits
                where exists(
                    from next_connection
                    where 0=1
                        or list_contains(circuits.circuit, next_connection.p)
                        or list_contains(circuits.circuit, next_connection.q)
                )
        )

        from (
            select distinct
                i + 1 as i,
                circuit.list_sort() as circuit,
            from new_circuits
            where i < (select n from input limit 1)
        )
        select row_number() over (), i, circuit
    )
)

from (
    select len(circuit) as circuit_length
    from circuits
    where i = (select max(i) from circuits)
    order by circuit_length desc
    limit 3
)
select product(circuit_length)::int
;
