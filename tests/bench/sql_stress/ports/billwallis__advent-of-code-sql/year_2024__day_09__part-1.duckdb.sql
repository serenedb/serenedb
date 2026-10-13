with

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_09/sample.data', header=false, types=['varchar'])
),

blocks as (
    from (
        from input
        select
            split(data::varchar, NULL) as blocks,
            unnest(split(data::varchar, NULL)) as block,
            generate_subscripts(split(data::varchar, NULL), 1) as block_id,
            block_id % 2 = 1 as is_file,
    )
    select
        *,
        case when is_file then -1 + row_number() over (
            partition by is_file order by block_id
        ) end as file_id,
),

uplift(block) as (
    select r.n
    from generate_series(1, 9) as l(n)
        inner join generate_series(1, 9) as r(n)
            on l.n <= r.n
),

positions as (
    from (
        from uplift inner join blocks using (block)
        select
            *,
            row_number() over (partition by is_file order by block_id) as free_space_position_id,
            row_number() over (partition by is_file order by block_id desc) as block_position_id,
            if(is_file, block_position_id, free_space_position_id) as pos_id,
    )
    select
        *,
        -1 + row_number() over (
            order by
                block_id,
                if(is_file, pos_id, null) desc,
                if(is_file, null, pos_id)
        ) as row_id,
)

from (
    select
        spaces.row_id,
        case when spaces.row_id < count(spaces.file_id) over ()
            then coalesce(spaces.file_id, blocks.file_id)
        end as shuffled_file_id
    from positions as spaces
        left join positions as blocks
            on spaces.pos_id = blocks.pos_id
            and not spaces.is_file
            and blocks.is_file
)
select sum(row_id * shuffled_file_id)
;
