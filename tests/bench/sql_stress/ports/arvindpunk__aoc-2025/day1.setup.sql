CREATE TABLE "table" AS SELECT unnest(string_split(rtrim(content, chr(10)), chr(10))) AS line FROM read_text('ref/input/example_day1.txt');
