WITH    RECURSIVE
        basicMoves (initial, cornerMove, edgeMove, cornerTurn, edgeTurn) AS
        (
        VALUES
                (
                'U',
                ARRAY[ 4,  1,  2,  7,  3,  5,  6,  0]::INT[],  ARRAY[ 8,  1,  2, 11,  4,  5,  6,  7,  3,  9, 10,  0]::INT[],
                ARRAY[ 2,  0,  0,  2,  1,  0,  0,  1]::INT[],  ARRAY[ 1,  0,  0,  1,  0,  0,  0,  0,  1,  0,  0,  1]::INT[]
                ),
                (
                'D',
                ARRAY[ 0,  5,  6,  3,  4,  2,  1,  7]::INT[],  ARRAY[ 0, 10,  9,  3,  4,  5,  6,  7,  8,  1,  2, 11]::INT[],
                ARRAY[ 0,  2,  2,  0,  0,  1,  1,  0]::INT[],  ARRAY[ 0,  1,  1,  0,  0,  0,  0,  0,  0,  1,  1,  0]::INT[]
                ),
                (
                'F',
                ARRAY[ 0,  6,  2,  4,  1,  5,  3,  7]::INT[],  ARRAY[ 0,  1,  2,  3,  4,  9,  8,  7,  5,  6, 10, 11]::INT[],
                ARRAY[ 0,  1,  0,  1,  2,  0,  2,  0]::INT[],  ARRAY[ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0]::INT[]
                ),
                (
                'B',
                ARRAY[ 7,  1,  5,  3,  4,  0,  6,  2]::INT[],  ARRAY[ 0,  1,  2,  3, 11,  5,  6, 10,  8,  9,  4,  7]::INT[],
                ARRAY[ 1,  0,  1,  0,  0,  2,  0,  2]::INT[],  ARRAY[ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0]::INT[]
                ),
                (
                'L',
                ARRAY[ 5,  4,  2,  3,  0,  1,  6,  7]::INT[],  ARRAY[ 4,  5,  2,  3,  1,  0,  6,  7,  8,  9, 10, 11]::INT[],
                ARRAY[ 0,  0,  0,  0,  0,  0,  0,  0]::INT[],  ARRAY[ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0]::INT[]
                ),
                (
                'R',
                ARRAY[ 0,  1,  7,  6,  4,  5,  2,  3]::INT[],  ARRAY[ 0,  1,  7,  6,  4,  5,  2,  3,  8,  9, 10, 11]::INT[],
                ARRAY[ 0,  0,  0,  0,  0,  0,  0,  0]::INT[],  ARRAY[ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0]::INT[]
                )
        ),
        moves (turns, initial, move, reverse, cornerMove, edgeMove, cornerTurn, edgeTurn) AS MATERIALIZED
        (
        SELECT  1 AS turns, initial, initial, LOWER(initial),
                cornerMove, edgeMove, cornerTurn, edgeTurn
        FROM    basicMoves bm
        UNION ALL
        SELECT  turns + 1, initial,
                CASE turns WHEN 1 THEN initial || '2' WHEN 2 THEN LOWER(initial) END,
                CASE turns WHEN 1 THEN initial || '2' WHEN 2 THEN initial END,
                t.*
        FROM    moves am
        JOIN    basicMoves bm
        USING   (initial)
        CROSS JOIN LATERAL
                turn(am.cornerMove, am.edgeMove, am.cornerTurn, am.edgeTurn, bm.cornerMove, bm.edgeMove, bm.cornerTurn, bm.edgeTurn) t
        WHERE   turns <= 2
        ),
        solved (cornerPosition, edgePosition, cornerOrientation, edgeOrientation) AS MATERIALIZED
        (
        VALUES
                (
                ARRAY(SELECT v::int FROM GENERATE_SERIES(0, 7) v), ARRAY(SELECT v::int FROM GENERATE_SERIES(0, 11) v),
                ARRAY(SELECT 0 FROM GENERATE_SERIES(0, 7)), ARRAY(SELECT 0 FROM GENERATE_SERIES(0, 11))
                )
        ),
        sequence (turns) AS
        (
        VALUES
        ('L2 d U F2 U F2 D B f D f l B2 f R2 F2 r F D2 l U L R2 f D2 F d')
        ),
        turns (move, i) AS MATERIALIZED
        (
        SELECT  turn.*
        FROM    sequence
        CROSS JOIN LATERAL
                unnest(string_to_array(turns, ' ')) WITH ORDINALITY AS turn (move, i)
        ),
        scramble (cornerPosition, edgePosition, cornerOrientation, edgeOrientation, currentMove, i) AS MATERIALIZED
        (
        SELECT  cornerPosition, edgePosition, cornerOrientation, edgeOrientation,
                NULL::TEXT,
                1::BIGINT AS i
        FROM    solved
        UNION ALL
        SELECT  newValues.*,
                move,
                turns.i + 1
        FROM    scramble e
        JOIN    turns
        USING   (i)
        JOIN    moves
        USING   (move)
        CROSS JOIN LATERAL
                turn(cornerPosition, edgePosition, cornerOrientation, edgeOrientation, cornerMove, edgeMove, cornerTurn, edgeTurn) AS newValues
        ),
        unsolved AS MATERIALIZED
        (
        SELECT  cornerPosition, edgePosition, cornerOrientation, edgeOrientation
        FROM    scramble
        ORDER BY
                i DESC
        LIMIT   1
        ),
        g1s AS MATERIALIZED
        (
        SELECT  0 AS depth, 0 AS distance,
                '000000000000' AS state,
                ARRAY[]::TEXT[] AS moves
        UNION ALL
        (
        WITH    q AS
                (
                SELECT  depth + 1 AS depth, distance, state, moves
                FROM    g1s
                )
        SELECT  *
        FROM    q
        WHERE   depth <= 7
        UNION ALL
        SELECT  DISTINCT ON (newState)
                depth, distance + 1, newState, array_append(moves, move)
        FROM    q
        CROSS JOIN
                moves
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG(((SUBSTRING(state FROM source + 1 FOR 1)::INT + edgeTurn[destination] + 2) % 2)::TEXT, '')
                FROM    UNNEST(edgeMove) WITH ORDINALITY AS move (source, destination)
                ) AS state (newState)
        WHERE   distance = depth - 1
                AND newState NOT IN
                (
                SELECT  state
                FROM    q q2
                )
                AND depth <= 7
        )
        ),
        g2s AS MATERIALIZED
        (
        SELECT  0 AS depth, 0 AS distance,
                '00000000000000001111' AS state,
                ARRAY[]::TEXT[] AS moves
        UNION ALL
        (
        WITH    q AS
                (
                SELECT  depth + 1 AS depth, distance, state, moves
                FROM    g2s
                )
        SELECT  *
        FROM    q
        WHERE   depth <= 10
        UNION ALL
        SELECT  DISTINCT ON (newState)
                depth, distance + 1, newState, array_append(moves, move)
        FROM    q
        JOIN    moves
        ON      move NOT IN ('U', 'u', 'D', 'd')
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG(((SUBSTRING(state FROM source + 1 FOR 1)::INT + cornerTurn[destination] + 3) % 3)::TEXT, '')
                FROM    UNNEST(cornerMove) WITH ORDINALITY AS move (source, destination)
                ) AS q1 (newCornerOrientation)
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG(SUBSTRING(state FROM source + 9 FOR 1), '')
                FROM    UNNEST(edgeMove) AS move (source)
                ) AS q2 (newMiddleEdgePosition)
        CROSS JOIN LATERAL
                (
                SELECT  newCornerOrientation || newMiddleEdgePosition
                ) AS q3 (newState)
        WHERE   distance = depth - 1
                AND depth <= 10
                AND newState NOT IN
                (
                SELECT  state
                FROM    q
                )
        )
        ),
        g3s AS MATERIALIZED
        (
        SELECT  0 AS depth, 0 AS distance,
                corners || '000011112222' AS state,
                ARRAY[]::TEXT[] AS moves
        FROM    (
                VALUES
                ('01234567'),
                ('01324576'),
                ('02134657'),
                ('02314675'),
                ('03124756'),
                ('03214765')
                ) o2 (corners)
        UNION ALL
        (
        WITH    q AS
                (
                SELECT  depth + 1 AS depth, distance, state, moves
                FROM    g3s
                )
        SELECT  *
        FROM    q
        WHERE   depth <= 13
        UNION ALL
        SELECT  DISTINCT ON (newState)
                depth, distance + 1, newState, array_append(moves, move)
        FROM    q
        JOIN    moves
        ON      move NOT IN ('U', 'u', 'D', 'd', 'F', 'f', 'B', 'b')
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG(SUBSTRING(state FROM source + 1 FOR 1), '')
                FROM    UNNEST(cornerMove) AS source
                ) AS q1 (newCornerPosition)
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG(SUBSTRING(state FROM source + 9 FOR 1), '')
                FROM    UNNEST(edgeMove) AS source
                ) AS q2 (newEdgeLayerPosition)
        CROSS JOIN LATERAL
                (SELECT twist(newCornerPosition)) AS tw (twistedCornerPosition)
        CROSS JOIN LATERAL
                (
                SELECT  twistedCornerPosition || newEdgeLayerPosition
                ) q3 (newState)
        WHERE   distance = depth - 1
                AND depth <= 13
                AND newState NOT IN
                (
                SELECT  q.state
                FROM    q
                )
        )
        ),
        g4s (depth, distance, state, moves) AS MATERIALIZED
        (
        SELECT  0, 0, '012345670123456789AB', ARRAY[]::TEXT[]
        UNION ALL
        (
        WITH    q AS
                (
                SELECT  depth + 1 AS depth, distance, state, moves
                FROM    g4s
                )
        SELECT  *
        FROM    q
        WHERE   depth <= 15
        UNION ALL
        SELECT  DISTINCT ON (newState)
                depth, distance + 1, newState, array_append(moves, move)
        FROM    q
        JOIN    moves
        ON      turns = 2
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG(SUBSTRING(state FROM source + 1 FOR 1), '')
                FROM    UNNEST(cornerMove) AS source
                ) AS q1 (newCornerPositions)
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG(SUBSTRING(state FROM source + 9 FOR 1), '')
                FROM    UNNEST(edgeMove) AS source
                ) AS q2 (newEdgePositions)
        CROSS JOIN LATERAL
                (
                SELECT  newCornerPositions || newEdgePositions
                ) AS q3 (newState)
        WHERE   distance = depth - 1
                AND depth <= 15
                AND newState NOT IN
                (
                SELECT  q.state
                FROM    q
                )
        )
        ),
        g1 AS MATERIALIZED
        (
        SELECT  state, moves
        FROM    g1s
        WHERE   depth = 7
        ),
        g2 AS MATERIALIZED
        (
        SELECT  state, moves
        FROM    g2s
        WHERE   depth = 10
        ),
        g3 AS MATERIALIZED
        (
        SELECT  state, moves
        FROM    g3s
        WHERE   depth = 13
        ),
        g4 AS MATERIALIZED
        (
        SELECT  state, moves
        FROM    g4s
        WHERE   depth = 15
        ),
        g1Entry AS
        (
        SELECT  g1.state, moves
        FROM    (
                SELECT  STRING_AGG(edge::TEXT, '') AS state
                FROM    unsolved
                CROSS JOIN LATERAL
                        UNNEST(edgeOrientation) edge
                ) q
        JOIN    g1
        USING   (state)
        ),
        g1Moves AS MATERIALIZED
        (
        SELECT  u.*, moves, NULL::TEXT AS move
        FROM    unsolved u
        CROSS JOIN
                g1Entry
        UNION ALL
        SELECT  newValues.*, moves[:(ARRAY_LENGTH(moves, 1) - 1)], moves.move
        FROM    g1Moves
        JOIN    moves
        ON      reverse = moves[ARRAY_LENGTH(moves, 1)]
        CROSS JOIN LATERAL
                turn(cornerPosition, edgePosition, cornerOrientation, edgeOrientation, cornerMove, edgeMove, cornerTurn, edgeTurn) AS newValues
        ),
        g2Entry AS
        (
        SELECT  g1Moves.cornerPosition, g1Moves.edgePosition, g1Moves.cornerOrientation, g1Moves.edgeOrientation,
                g2.moves
        FROM    (
                SELECT  *
                FROM    g1Moves
                WHERE   moves = ARRAY[]::TEXT[]
                ) g1Moves
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG(value::TEXT, '')
                FROM    UNNEST(cornerOrientation) AS value
                ) AS q (corners)
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG((value >= 8)::INT::TEXT, '')
                FROM    UNNEST(edgePosition) AS value
                ) AS q2 (middleEdges)
        CROSS JOIN LATERAL
                (
                SELECT  corners || middleEdges
                ) AS q3 (state)
        JOIN    g2
        USING   (state)
        ),
        g2Moves AS MATERIALIZED
        (
        SELECT  *, NULL::TEXT AS move
        FROM    g2Entry
        UNION ALL
        SELECT  newValues.*, moves[:(ARRAY_LENGTH(moves, 1) - 1)], moves.move
        FROM    g2Moves
        JOIN    moves
        ON      reverse = moves[ARRAY_LENGTH(moves, 1)]
        CROSS JOIN LATERAL
                turn(cornerPosition, edgePosition, cornerOrientation, edgeOrientation, cornerMove, edgeMove, cornerTurn, edgeTurn) AS newValues
        ),
        g3Entry AS
        (
        SELECT  g2Last.cornerPosition, g2Last.edgePosition, g2Last.cornerOrientation, g2Last.edgeOrientation,
                g3.moves
        FROM    (
                SELECT  *
                FROM    g2Moves
                WHERE   moves = ARRAY[]::TEXT[]
                ) g2Last
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG(value::TEXT, '')
                FROM    UNNEST(cornerPosition) AS value
                ) AS q (corners)
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG((value / 4)::INT::TEXT, '')
                FROM    UNNEST(edgePosition) AS value
                ) AS q2 (layerEdges)
        CROSS JOIN LATERAL
                (SELECT twist(corners)) AS tw (twistedCorners)
        CROSS JOIN LATERAL
                (
                SELECT  twistedCorners || layerEdges
                ) AS q3 (state)
        JOIN    g3
        USING   (state)
        ),
        g3Moves AS MATERIALIZED
        (
        SELECT  *, NULL::TEXT AS move
        FROM    g3Entry
        UNION ALL
        SELECT  newValues.*, moves[:(ARRAY_LENGTH(moves, 1) - 1)], moves.move
        FROM    g3Moves
        JOIN    moves
        ON      reverse = moves[ARRAY_LENGTH(moves, 1)]
        CROSS JOIN LATERAL
                turn(cornerPosition, edgePosition, cornerOrientation, edgeOrientation, cornerMove, edgeMove, cornerTurn, edgeTurn) AS newValues
        ),
        g4Entry AS
        (
        SELECT  g3Last.cornerPosition, g3Last.edgePosition, g3Last.cornerOrientation, g3Last.edgeOrientation,
                g4.moves
        FROM    (
                SELECT  *
                FROM    g3Moves
                WHERE   moves = ARRAY[]::TEXT[]
                ) g3Last
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG(value::TEXT, '')
                FROM    UNNEST(cornerPosition) AS value
                ) AS q (corners)
        CROSS JOIN LATERAL
                (
                SELECT  STRING_AGG(UPPER(TO_HEX(value::INT)), '')
                FROM    UNNEST(edgePosition) AS value
                ) AS q2 (edges)
        CROSS JOIN LATERAL
                (
                SELECT  corners || edges
                ) AS q3 (state)
        JOIN    g4
        USING   (state)
        ),
        g4Moves AS MATERIALIZED
        (
        SELECT  *, NULL::TEXT AS move
        FROM    g4Entry
        UNION ALL
        SELECT  newValues.*, moves[:(ARRAY_LENGTH(moves, 1) - 1)], moves.move
        FROM    g4Moves
        JOIN    moves
        ON      reverse = moves[ARRAY_LENGTH(moves, 1)]
        CROSS JOIN LATERAL
                turn(cornerPosition, edgePosition, cornerOrientation, edgeOrientation, cornerMove, edgeMove, cornerTurn, edgeTurn) AS newValues
        )
SELECT  STRING_AGG(move, ' ') AS result
FROM    (
        SELECT  *
        FROM    g1Moves
        UNION ALL
        SELECT  *
        FROM    g2Moves
        UNION ALL
        SELECT  *
        FROM    g3Moves
        UNION ALL
        SELECT  *
        FROM    g4Moves
        ) q
WHERE   move <> ''
