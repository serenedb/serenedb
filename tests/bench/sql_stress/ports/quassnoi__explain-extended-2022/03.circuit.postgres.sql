WITH	RECURSIVE
	settings AS
	(
	SELECT	9 AS qubits,
		ARRAY[0, 1, 2, 3] AS measurements,
		'
H: 8;
Z: 8;
H: 0;
H: 1;
H: 2;
H: 3;

CX: 0,4;
CX: 1,4;
CX: 0,5;
CX: 2,5;
CX: 1,6;
CX: 3,6;
CX: 2,7;
CX: 3,7;
CCCCX: 4,5,6,7,8;
CX: 0,4;
CX: 1,4;
CX: 0,5;
CX: 2,5;
CX: 1,6;
CX: 3,6;
CX: 2,7;
CX: 3,7;

H: 0;
H: 1;
H: 2;
H: 3;
X: 0;
X: 1;
X: 2;
X: 3;
H: 3;
CCCX: 0,1,2,3;
H: 3;
X: 0;
X: 1;
X: 2;
X: 3;
H: 0;
H: 1;
H: 2;
H: 3;

CX: 0,4;
CX: 1,4;
CX: 0,5;
CX: 2,5;
CX: 1,6;
CX: 3,6;
CX: 2,7;
CX: 3,7;
CCCCX: 4,5,6,7,8;
CX: 0,4;
CX: 1,4;
CX: 0,5;
CX: 2,5;
CX: 1,6;
CX: 3,6;
CX: 2,7;
CX: 3,7;

H: 0;
H: 1;
H: 2;
H: 3;
X: 0;
X: 1;
X: 2;
X: 3;
H: 3;
CCCX: 0,1,2,3;
H: 3;
X: 0;
X: 1;
X: 2;
X: 3;
H: 0;
H: 1;
H: 2;
H: 3;
' AS program
	),
	circuit AS MATERIALIZED
	(
	SELECT	step, parts[1] AS opcode, inputs::INT[]
	FROM	settings
	CROSS JOIN
		UNNEST(REGEXP_SPLIT_TO_ARRAY(program, E'\\s*;\\s*')) WITH ORDINALITY instructions(instruction, step)
	CROSS JOIN LATERAL
		(SELECT REGEXP_MATCH(instruction, E'(\\w+)\\s*:\\s(.*)') WHERE instruction ~ E'(\\w+)\\s*:\\s(.*)') lm (parts)
	CROSS JOIN LATERAL
		(SELECT REGEXP_SPLIT_TO_ARRAY(parts[2], E'\\s*,\\s*')) ls (inputs)
	)
SELECT	*
FROM	circuit
ORDER BY
	step
LIMIT 10
