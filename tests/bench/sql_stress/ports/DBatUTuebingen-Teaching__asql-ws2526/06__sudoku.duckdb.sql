-- Brute force solver for Sudoku
-- (adapted from http://www.sqlite.org/lang_with.html#recursivecte)
--
-- Returns all possible solutions in about 100ms (a good Suduko board
-- has a unique solution).  Takes 10s for the 17-digit variant.
--
-- Invariant for table sudoku:
--
-- If (board, blank) ∊ sudoku, then board is a 9×9=81-integer array of a (partially solved)
-- valid Sudoku board in which blank ∊ {0,...,80} is the offset of the first unknown digit
-- (represented by 0).  If blank is NULL, then board is a complete solution.
--
-- (Note: list_position(xs, x) yields NULL if x is not found in xs)

-- Sample Sudoku instances

-- sudoku1.txt:
--
-- 53..7....
-- 6..195...
-- .98....6.
-- 8...6...3
-- 4..8.3..1
-- 7...2...6
-- .6....28.
-- ...419..5
-- ....8..79

-- sudoku2.txt:
--
-- ...6...75
-- 4...5.8.1
-- .3..7..2.
-- ..6..1...
-- ...7..58.
-- .9..3...6
-- .4...9...
-- ..18..2..
-- .......3.

-- sudoku3.txt (⚠️ 17 given digits only, extremely hard):
--
-- ........2
-- .6.....71
-- ..93.8...
-- .....5...
-- ........7
-- 8.3..4...
-- ....1....
-- .2..7....
-- ......85.


CREATE OR REPLACE MACRO sudoku() AS 'material/06/sudoku3.txt';

DROP TABLE IF EXISTS puzzle;
CREATE TEMPORARY TABLE puzzle (
  row    int PRIMARY KEY,
  digits text NOT NULL
);

INSERT INTO puzzle(row,digits)
  SELECT rows.row, rows.digits
  FROM   unnest((SELECT string_split(txt.content[:-2], E'\n')
                 FROM   read_text(sudoku()) AS txt))
         WITH ORDINALITY AS rows(digits,row);

TABLE puzzle;

-----------------------------------------------------------------------

WITH RECURSIVE
-- encode Sudoku board as one-dimensional array in row-major order
input(board) AS (
  SELECT string_split(string_agg(replace(p.digits, '.', '0'), '' ORDER BY p.row), NULL) :: int[] AS board
  FROM   puzzle AS p
),
-- solve Sudoko board by a recursive generate-and-test process
sudoku(board, blank) AS (
  SELECT i.board, list_position(i.board, 0)-1 AS blank
  FROM   input AS i
      UNION ALL
  SELECT s.bd[1:s.b] || [fill] || s.bd[s.b+2:81] AS board,
         list_position(board, 0)-1               AS blank
  FROM  sudoku AS s(bd,b), generate_series(1,9) AS _(fill)
  WHERE s.b IS NOT NULl
    AND NOT EXISTS (
      SELECT NULL
      FROM   generate_series(1,9) AS __(o)
      WHERE  fill IN (s.bd[(s.b//9) * 9                            + o],               --  row of blank (offset o)
                      s.bd[s.b%9                                   + (o-1)*9 + 1],     --  column of blank (offset o)
                      s.bd[((s.b//3) % 3) * 3 + (s.b//27) * 27 + o + ((o-1)//3) * 6])  --  box of blank (offset o)
   )
),
-- (recursive) post-processing only: generate formatted board output
output(board, row, digits, rest) AS (
  SELECT row_number() OVER () AS board,
         0 AS row,
         left( list_aggregate(s.board, 'string_agg', ''),  9) AS digits,
         right(list_aggregate(s.board, 'string_agg', ''), -9) AS rest
  FROM   sudoku AS s
  WHERE  s.blank IS NULL
    UNION ALL
  SELECT o.board,
         o.row + 1 AS row,
         left(o.rest,   9) AS digits,
         right(o.rest, -9) AS rest
  FROM   output AS o
  WHERE  o.rest <> ''
)
-- ➊ Raw input Sudoku board in array-encoding
-- TABLE input;
-- ➋ Complete progress towards solved board (⚠ huge)
-- TABLE sudoku;
-- ➌ Raw solved Sudoku board
-- SELECT s.board
-- FROM   sudoku AS s
-- WHERE  s.blank IS NULL;
-- ➍ Formatted solved Sudoku board
SELECT o.board AS number, o.digits AS solution
FROM   output AS o
ORDER BY o.board, o.row;


-- Computing offsets (with blank ∊ {0,...,80}):
--
-- (blank//9) * 9 ∊ {0,9,18,27,36,45,54,63,72}:                         beginning (left) of row containing blank
-- blank%9 ∊ {0,1,2,3,4,5,6,7,8}:                                       beginning (top)  of column containing blank
-- ((blank//3) % 3) * 3 + (blank//27) * 27 ∊ {0,3,6,27,30,33,54,57,60}: beginning (top left) of box containing blank
