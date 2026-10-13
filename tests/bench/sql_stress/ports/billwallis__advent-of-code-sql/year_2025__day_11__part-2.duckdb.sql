with recursive

input(content) as (
    select content.rtrim(e'\n')
    from read_text('advent_of_code/solutions/year_2025/day_11/sample-2.data')
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

_paths_fft_dac(path, current_device) as (
        select
            [device_in, device_out] as path,
            device_out as current_device,
        from cables
        where device_in = 'fft'
    union all
        select
            list_append(paths.path, cables.device_out),
            cables.device_out,
        from _paths_fft_dac as paths
            inner join cables
                on  paths.current_device = cables.device_in
                and not list_contains(paths.path, cables.device_out)
        where paths.current_device != 'dac'
),
paths_fft_dac as (
    from _paths_fft_dac
    where current_device = 'dac'
),

_paths_dac_fft(path, current_device) as (
        select
            [device_in, device_out] as path,
            device_out as current_device,
        from cables
        where device_in = 'dac'
    union all
        select
            list_append(paths.path, cables.device_out),
            cables.device_out,
        from _paths_dac_fft as paths
            inner join cables
                on  paths.current_device = cables.device_in
                and not list_contains(paths.path, cables.device_out)
        where paths.current_device != 'fft'
),
paths_dac_fft as (
    from _paths_dac_fft
    where current_device = 'fft'
),

valid_paths as (
        from paths_fft_dac
    union
        from paths_dac_fft
),

paths(path, device_l, device_r) as (
        select
            path,
            path[1] as device_l,
            path[-1] as device_r,
        from valid_paths
    union (
        from (
            select
                if(cables_l.device_in is null, paths.path, list_prepend(cables_l.device_in, paths.path)) as _path,
                if(cables_r.device_out is null, _path, list_append(_path, cables_r.device_out)) as path,
                cables_l.device_in,
                cables_r.device_out,
            from paths
                left join cables as cables_l
                    on  paths.device_l = cables_l.device_out
                    and not list_contains(paths.path, cables_l.device_in)
                left join cables as cables_r
                    on  paths.device_r = cables_r.device_in
                    and not list_contains(paths.path, cables_r.device_out)
            where not (paths.path[1] = 'svr' and paths.path[-1] = 'out')
        )
        select
            path,
            device_in,
            device_out,
    )
)

-- from paths_fft_dac
-- from paths_dac_fft

select count(*) as paths
from paths
where 1=1
    and path[1] = 'svr'
    and path[-1] = 'out'
;
