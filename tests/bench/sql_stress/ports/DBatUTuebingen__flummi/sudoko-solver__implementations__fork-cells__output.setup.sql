DROP TABLE IF EXISTS sudoku;
CREATE TABLE sudoku (
  cells int[]
);


INSERT INTO sudoku(cells)
  SELECT apply(split(replace(txt.content[:-2], E'\n', ''), NULL), lambda c: if(c = '.', 0, c :: int))
  FROM   read_text('sudokus/crafted/*.txt') AS txt;
