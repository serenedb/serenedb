/*
    Works for the sample, but not sure about the input yet -- ran for 2+ hours!
*/


with recursive

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_09/sample.data', header=false, types=['varchar'])
),

blocks as (
    from (
        from input
        select
            split(data::varchar, NULL) as blocks,
            unnest(split(data::varchar, NULL))::int as block_size,
            generate_subscripts(split(data::varchar, NULL), 1) as block_id,
            block_id % 2 = 1 as is_file,
    )
    select
        *,
        case when is_file then -1 + row_number() over (
            partition by is_file order by block_id
        ) end as file_id,
        is_file = true as try_to_move,
),

shuffle as (
        select
            block_id,
            block_size,
            1 as i,
            try_to_move,
            if(is_file, [{'id': file_id, 'size': block_size}], []::struct(id int, size int)[]) as files,
            if(is_file, 0, block_size) as remaining_space,
        from blocks
    union all (
        with

        current_block as (
            select
                block_size,
                block_id,
                try_to_move,
                files,
                remaining_space,
                i,
            from shuffle
            qualify 1=1
                and i = max(i) over ()
                and max(try_to_move) over ()
        ),

        block_to_move as (
            select
                block_id as block_to_move,
                block_size as block_to_move_size,
                (files[1]->>'id')::int as block_to_move_file_id,
            from current_block
            where try_to_move
            order by block_id desc
            limit 1
        )

        from (
            from current_block, block_to_move
            select
                *,
                (
                    select min(block_id)
                    from current_block as innr
                    where innr.remaining_space > 0
                      and current_block.i = innr.i
                      and block_to_move.block_to_move_size <= innr.remaining_space
                      and block_to_move.block_to_move > innr.block_id
                ) as block_to_move_to
        )
        select
            block_id,
            block_size,

            i + 1,
            if(block_id = block_to_move, false, try_to_move) as try_to_move,
            case when block_to_move_to is null
                then files
                else case block_id
                    when block_to_move    then null
                    when block_to_move_to then list_append(files, {'id': block_to_move_file_id, 'size': block_to_move_size})
                                          else files
                end
            end as files,
            case when block_to_move_to is null
                then remaining_space
                else case block_id
                    when block_to_move    then block_size
                    when block_to_move_to then remaining_space - block_to_move_size
                                          else remaining_space
                end
            end as remaining_space,
    )
),

shuffled_blocks as (
    select block_id, block_size, files
    from shuffle
    qualify i = max(i) over ()
    order by i, block_id
),

unpacked as (
    from shuffled_blocks
    select
        block_id,
        block_size,
        unnest(files, recursive:=true),
        generate_subscripts(files, 1) as file_index,
),

uplift(block_size) as (
    select r.n
    from generate_series(1, 9) as l(n)
        inner join generate_series(1, 9) as r(n)
            on l.n <= r.n
),

uplifted_blocks as (
    from shuffled_blocks
        inner join uplift using (block_size)
    select
        block_id,
        row_number() over (partition by block_id) as block_sub_id
    order by all
),

uplifted_files as (
    from unpacked
        inner join uplift on unpacked.size = uplift.block_size
    select
        unpacked.*,
        row_number() over (partition by block_id order by file_index) as block_sub_id,
    order by
        block_id,
        file_index
)

from (
    from uplifted_blocks
        left join uplifted_files
            using (block_id, block_sub_id)
    select
        block_id,
        block_sub_id,
        uplifted_files.id,
        uplifted_files.size,
        -1 + row_number() over (order by block_id, block_sub_id) as row_id,
    order by block_id, block_sub_id
)
select sum(id * row_id)
;
