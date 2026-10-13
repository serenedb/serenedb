with

input(data, row_id) as (
    select *, row_number() over() as row_id
    from read_csv('advent_of_code/solutions/year_2024/day_05/sample.data', header=false)
),

page_order as (
    select
        split(data, '|')[1]::int as page_before,
        split(data, '|')[2]::int as page_after,
    from input
    where row_id < (select row_id from input where data is null)
),

print_queue as (
    select
        row_id as print_id,
        split(data, ',')::int[] as pages,
        unnest(split(data, ',')::int[])::int as page,
        generate_subscripts(split(data, ',')::int[], 1) AS page_order,
    from input
    where row_id > (select row_id from input where data is null)
),

pages_after as (
    from (
              select page_before from page_order
        union select page_after  from page_order
    ) as pages(page)
        natural full join (
            select page_before, list(page_after)
            from page_order
            group by page_before
        ) as after(page, pages_after)
    select
        page,
        coalesce(after.pages_after, []) as pages_after,
),

ordered_print_queue as (
    from (
        select
            print_queue.print_id,
            print_queue.pages,
            print_queue.page,
            len(list_filter(
                pages_after.pages_after,
                p -> list_contains(print_queue.pages, p)
            )) as pages_after_len
        from print_queue
            left join pages_after
                using (page)
    )
    select
        print_id,
        any_value(pages) as original_pages,
        list(page order by pages_after_len desc) as ordered_pages,
        original_pages = ordered_pages as good_order_flag,
    group by print_id
)

select sum(ordered_pages[((len(ordered_pages) + 1) / 2)::int])
from ordered_print_queue
where not good_order_flag
;
