\set MODE RECURSIVE
\set invocations 256
\set iterations 256
-- Virtual machine (VM) featuring three-address opcodes

-- # of VM runs (~ invocations)
\set N :invocations
--
DROP TYPE IF EXISTS opcode CASCADE;
CREATE TYPE opcode AS ENUM (
  'lod',  -- lod t, x       load literal x into target register Rt
  'mov',  -- mov t, s       move from source register Rs to target register Rt
  'jeq',  -- jeq t, s, @a   if Rt = Rs, jump to location a, else fall through
  'jmp',  -- jmp @a         jump to location a
  'add',  -- add t, s1, s2  Rt ← Rs1 + Rs2
  'sub',  -- add t, s1, s2  Rt ← Rs1 - Rs2
  'mul',  -- mul t, s1, s2  Rt ← Rs1 * Rs2
  'div',  -- div t, s1, s2  Rt ← Rs1 / Rs2
  'mod',  -- mod t, s1, s2  Rt ← Rs1 mod Rs2
  'hlt'   -- hlt s          halt program, result is register Rs
);

-- A single VM instruction
--
DROP TYPE IF EXISTS instruction CASCADE;
CREATE TYPE instruction AS (
  loc   int,     -- location
  opc   opcode,  -- opcode
  reg1  numeric, -- ┐
  reg2  numeric, -- │ up to three work registers
  reg3  numeric  -- ┘
);

-- A program is a table of instructions
--
DROP TABLE IF EXISTS program CASCADE;
CREATE TABLE program (loc int, opc opcode, reg1 numeric, reg2 numeric, reg3 numeric);

CREATE INDEX ip ON program (loc);

-----------------------------------------------------------------------
-- Program to compute the length of the Collatz sequence (also known as
-- the "3N + 1 problem") for the value N held in register R1.  Program
-- entry is at location 0.
--
-- INSERT INTO program(loc, opc, reg1, reg2, reg3) VALUES
--   ( 0, 'lod', 4, 0   , NULL),
--   ( 1, 'lod', 5, 1   , NULL),
--   ( 2, 'lod', 6, 2   , NULL),
--   ( 3, 'lod', 7, 3   , NULL),
--   ( 4, 'mov', 2, 4   , NULL),
--   ( 5, 'jeq', 1, 5   , 14  ),
--   ( 6, 'add', 2, 2   , 5   ),
--   ( 7, 'mod', 3, 1   , 6   ),
--   ( 8, 'jeq', 3, 5   , 11  ),
--   ( 9, 'div', 1, 1   , 6   ),
--   (10, 'jmp', 5, NULL, NULL),
--   (11, 'mul', 1, 1   , 7   ),
--   (12, 'add', 1, 1   , 5   ),
--   (13, 'jmp', 5, NULL, NULL),
--   (14, 'hlt', 2, NULL, NULL);

-- Program to compute the Nth element of the Padovan sequence
-- (see https://oeis.org/A000931) for value N held in register R1.
-- Program entry is at location 0.

INSERT INTO program(loc, opc, reg1, reg2, reg3) VALUES
  ( 0, 'lod', 2, 1,    NULL),
  ( 1, 'lod', 3, 0,    NULL),
  ( 2, 'lod', 4, 0,    NULL),
  ( 3, 'lod', 6, 0,    NULL),
  ( 4, 'lod', 7, 1,    NULL),
  ( 5, 'jeq', 1, 6,    15  ),
  ( 6, 'jeq', 1, 7,    16  ),
  ( 7, 'sub', 1, 1,    7   ),
  ( 8, 'sub', 1, 1,    7   ),
  ( 9, 'jeq', 1, 6,    17  ),
  (10, 'add', 5, 2,    3   ),
  (11, 'mov', 2, 3,    NULL),
  (12, 'mov', 3, 4,    NULL),
  (13, 'mov', 4, 5,    NULL),
  (14, 'jmp', 8, NULL, NULL),
  (15, 'hlt', 2, NULL, NULL),
  (16, 'hlt', 3, NULL, NULL),
  (17, 'hlt', 4, NULL, NULL);

-----------------------------------------------------------------------

