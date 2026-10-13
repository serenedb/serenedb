SET integer_division = true;

/*
  Day 4: Camp Cleanup
  https://adventofcode.com/2022/day/4
*/

with section_assignments as (
  from read_csv(
        'input/04.csv',
        columns={
          'elf_1': 'text',
          'elf_2': 'text'
        }
       , header=false)
)

, split as (
  select string_split(elf_1, '-') as elf_1_assignment_ids
       , string_split(elf_2, '-') as elf_2_assignment_ids
       , elf_1_assignment_ids[1]::int as elf_1_start
       , elf_1_assignment_ids[2]::int as elf_1_end
       , elf_2_assignment_ids[1]::int as elf_2_start
       , elf_2_assignment_ids[2]::int as elf_2_end
    from section_assignments
)

, part_1 as (
  select 1 as part
       , count(*) as answer
    from split
   where (elf_1_start between elf_2_start and elf_2_end
          and elf_1_end between elf_2_start and elf_2_end)
      or (elf_2_start between elf_1_start and elf_1_end
          and elf_2_end between elf_1_start and elf_1_end)
)

, part_2 as (
  select 2 as part
       , count(*) as answer
    from split
   where elf_1_start between elf_2_start and elf_2_end
      or elf_1_end between elf_2_start and elf_2_end
      or elf_2_start between elf_1_start and elf_1_end
      or elf_2_end between elf_1_start and elf_1_end
)

 from part_1
union all
 from part_2