SET integer_division = true;

/*
  Day 3: Rucksack Reorganization
  https://adventofcode.com/2022/day/3
*/

with input(elf, items) as (
  select row_number() over () as elf
       , *
    from read_csv_auto('input/03.csv', header=false)
)

/* Part 1: Split items into compartments using string slicing. */
, compartments as (
  select *
       , length(items) as len
       , string_split(items[1 : len / 2], NULL) as compartment_1
       , string_split(items[len / 2 + 1 : len], NULL) as compartment_2
    from input
)

/* Part 1: Filter for common item. */
, common_by_compartment as (
  select elf
       , list_filter(compartment_1, x -> contains(compartment_2, x))[1] as item
    from compartments
)

/* Part 2: Assign a group number for each group of three elves. */
, elf_groups as (
  select row_number() over (partition by elf % 3 order by elf) as elf_group
       , *
    from input
)

/* Part 2: Find the distinct items carried by each elf. */
, distinct_items_by_group as (
  select distinct
         elf_group
       , elf
       , unnest(string_split(items, NULL)) as item
    from elf_groups
)

/* Part 2: Find the single item found in the rucksack of all three elves per group. */
, common_by_group as (
  select elf_group
       , item
    from distinct_items_by_group
   group by 1, 2
  having count(*) = 3
)

, answer_set as (
  select 1 as part
       , item
    from common_by_compartment
   union all
  select 2 as part
       , item
    from common_by_group
)

/* Apply the priority logic by finding the offset of ASCII character code. */
select part
     , sum(case
           when ord(item) between 65 and 90 then ord(item) - 38 /* A-Z */
           else ord(item) - 96 /* a-z */
       end) as answer
  from answer_set
 group by 1