with recursive

input(content) as (
    select content.rtrim(e'\n')
    from read_text('advent_of_code/solutions/year_2025/day_11/sample-1.data')
),

cables as (
    from (
        from input
        select
            generate_subscripts(content.split(e'\n'), 1) as cable_id,
            unnest(content.split(e'\n')) as cable_input_output,
    )
    select
        cable_id,
        cable_input_output.split_part(': ', 1) as device_in,
        unnest(cable_input_output.split_part(': ', 2).split(' ')) as device_out,
),

paths(path, current_device, i) as (
        select
            [device_in, device_out] as path,
            device_out as current_device,
            1 as i,
        from cables
        where device_in = 'you'
    union all
        select
            list_append(paths.path, cables.device_out),
            cables.device_out,
            paths.i + 1,
        from paths
            inner join cables
                on  paths.current_device = cables.device_in
                and not list_contains(paths.path, cables.device_out)
                and cables.device_out != 'you'
        where paths.current_device != 'out'
)

select count(*) as paths
from paths
where current_device = 'out'
;
