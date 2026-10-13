CREATE OR REPLACE FUNCTION turn(cornerPosition INT[], edgePosition INT[], cornerOrientation INT[], edgeOrientation INT[], cornerMove INT[], edgeMove INT[], cornerTurn INT[], edgeTurn INT[])
RETURNS TABLE
        (
        cornerPosition INT[],
        edgePosition INT[],
        cornerOrientation INT[],
        edgeOrientation INT[]
        )
AS
$$

SELECT  ARRAY
        (
        SELECT  cornerPosition[source + 1]
        FROM    UNNEST(cornerMove) AS move (source)
        ),
        ARRAY
        (
        SELECT  edgePosition[source + 1]
        FROM    UNNEST(edgeMove) AS move (source)
        ),
        ARRAY
        (
        SELECT  (cornerOrientation[source + 1] + cornerTurn[destination] + 3) % 3
        FROM    UNNEST(cornerMove) WITH ORDINALITY AS move (source, destination)
        ),
        ARRAY
        (
        SELECT  (edgeOrientation[source + 1] + edgeTurn[destination] + 2) % 2
        FROM    UNNEST(edgeMove) WITH ORDINALITY AS move (source, destination)
        );
$$
LANGUAGE sql
STRICT
IMMUTABLE
PARALLEL SAFE
ROWS 1