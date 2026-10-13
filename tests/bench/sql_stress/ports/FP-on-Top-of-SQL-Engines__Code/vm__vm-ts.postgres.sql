-----------------------------------------------------------------------
-- Translation into a UDF using trampolined style
-- The second parameter represents the machine's register file (each register hold one integer).

DROP FUNCTION IF EXISTS runTS(instruction, int[]);

CREATE FUNCTION runTS(ins instruction, regs int[]) RETURNS int AS
$$
WITH RECURSIVE tramp(fn, ins, regs) AS (
  SELECT 'run', ins :: instruction, regs
    UNION ALL
  SELECT _.*
  FROM   tramp AS t, LATERAL
          (SELECT 'run', (SELECT p FROM program AS p WHERE p.loc = (t.ins :: instruction).loc+1),
                  array_append(t.regs[:(t.ins :: instruction).reg1-1], (t.ins :: instruction).reg2) ||
                  t.regs[(t.ins :: instruction).reg1+1:]
          WHERE t.fn = 'run' AND (t.ins :: instruction).opc = 'lod'
            UNION ALL
          SELECT 'run', (SELECT p FROM program AS p WHERE p.loc = (t.ins :: instruction).loc+1),
                 array_append(t.regs[:(t.ins :: instruction).reg1-1], t.regs[(t.ins :: instruction).reg2]) ||
                 t.regs[(t.ins :: instruction).reg1+1:]
          WHERE t.fn = 'run' AND (t.ins :: instruction).opc = 'mov'
            UNION ALL
          SELECT 'run', (SELECT p FROM program AS p WHERE p.loc = (t.ins :: instruction).reg3), t.regs
          WHERE t.fn = 'run' AND (t.ins :: instruction).opc = 'jeq' AND
                t.regs[(t.ins :: instruction).reg1] = t.regs[(t.ins :: instruction).reg2]
            UNION ALL
          SELECT 'run', (SELECT p FROM program AS p WHERE p.loc = (t.ins :: instruction).loc + 1), t.regs
          WHERE t.fn = 'run' AND (t.ins :: instruction).opc = 'jeq' AND
                t.regs[(t.ins :: instruction).reg1] <> t.regs[(t.ins :: instruction).reg2]
            UNION ALL
          SELECT 'run', (SELECT p FROM program AS p WHERE p.loc = (t.ins :: instruction).reg1), t.regs
          WHERE t.fn = 'run' AND (t.ins :: instruction).opc = 'jmp'
            UNION ALL
          SELECT 'run', (SELECT p FROM program AS p WHERE p.loc = (t.ins :: instruction).loc+1),
                 array_append(t.regs[:(t.ins :: instruction).reg1-1],
                 t.regs[(t.ins :: instruction).reg2] + t.regs[(t.ins :: instruction).reg3]) ||
                 t.regs[(t.ins :: instruction).reg1+1:]
          WHERE t.fn = 'run' AND (t.ins :: instruction).opc = 'add'
            UNION ALL
          SELECT 'run', (SELECT p FROM program AS p WHERE p.loc = (t.ins :: instruction).loc+1),
                 array_append(t.regs[:(t.ins :: instruction).reg1-1],
                 t.regs[(t.ins :: instruction).reg2] * t.regs[(t.ins :: instruction).reg3]) ||
                 t.regs[(t.ins :: instruction).reg1+1:]
          WHERE t.fn = 'run' AND (t.ins :: instruction).opc = 'mul'
            UNION ALL
          SELECT 'run', (SELECT p FROM program AS p WHERE p.loc = (t.ins :: instruction).loc+1),
                 array_append(t.regs[:(t.ins :: instruction).reg1-1],
                 t.regs[(t.ins :: instruction).reg2] / t.regs[(t.ins :: instruction).reg3]) ||
                 t.regs[(t.ins :: instruction).reg1+1:]
          WHERE t.fn = 'run' AND (t.ins :: instruction).opc = 'div'
            UNION ALL
          SELECT 'run', (SELECT p FROM program AS p WHERE p.loc = (t.ins :: instruction).loc+1),
                 array_append(t.regs[:(t.ins :: instruction).reg1-1],
                 t.regs[(t.ins :: instruction).reg2] % t.regs[(t.ins :: instruction).reg3]) ||
                 t.regs[(t.ins :: instruction).reg1+1:]
          WHERE t.fn = 'run' AND (t.ins :: instruction).opc = 'mod'
            UNION ALL
          SELECT 'finish', t.ins, ARRAY[t.regs[(t.ins :: instruction).reg1]]
          WHERE t.fn = 'run' AND (t.ins :: instruction).opc = 'hlt'
          ) AS _
) SELECT t.regs[1] FROM tramp AS t WHERE t.fn = 'finish';
$$
LANGUAGE SQL STABLE STRICT;

-----------------------------------------------------------------------
-- Compute the length of the Collatz sequence (also known as
-- the "3N + 1 problem") for the value N held in register R1.

SELECT num,
       runTS((SELECT p FROM program AS p WHERE p.loc = 0), -- program entry instruction
              ARRAY[num::int,0,0,0,0,0,0]) AS collatz        -- initial register file
FROM generate_series(1, :N) AS _(num);
