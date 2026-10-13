-- Demonstrate the use of a recursive SQL UDF to drive a finite state
-- machine (derived from a regular expression) to parse chemical compound
-- formulae.

-- SQL UDF to parse string 'input'. Finite state machine currently is in state 'state'.

-- Translation into a UDF using trampolined style

DROP FUNCTION IF EXISTS parse(int, text);

CREATE FUNCTION parse(state int, input text) RETURNS boolean AS
$$
WITH RECURSIVE tramp(fn, state, input, x) AS (
  SELECT 'parse', state, input, NULL :: boolean
    UNION ALL
  SELECT _.*
  FROM tramp AS t, LATERAL
       (SELECT 'finish', t.state, t.input,
               (SELECT DISTINCT edge.final
	              FROM fsm AS edge
		            WHERE t.state = edge.source)
        WHERE t.fn = 'parse' AND length(t.input) = 0 AND
	           (SELECT DISTINCT edge.final
              FROM fsm AS edge
	            WHERE t.state = edge.source) IS NOT NULL
          UNION ALL
        SELECT 'finish', t.state, t.input, false
        WHERE t.fn = 'parse' AND length(t.input) = 0 AND
             (SELECT DISTINCT edge.final
	            FROM fsm AS edge
	            WHERE t.state = edge.source) IS NULL
          UNION ALL
	      SELECT 'parse', (SELECT edge.target
		    	              FROM fsm AS edge
			                  WHERE t.state = edge.source
			                  AND strpos(edge.labels, left(t.input, 1)) > 0),
		            right(t.input, -1), t.x
	      WHERE t.fn = 'parse' AND length(t.input) <> 0
       ) AS _
) SELECT t.x FROM tramp AS t WHERE t.fn = 'finish';
$$ LANGUAGE SQL;

-----------------------------------------------------------------------
-- Parse chemical compounds and validate their formulae

SELECT c.*, parse(0, c.formula)
FROM compounds AS c
LIMIT :iterations;
