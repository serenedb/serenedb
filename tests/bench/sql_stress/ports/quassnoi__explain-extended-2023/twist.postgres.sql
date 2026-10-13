CREATE OR REPLACE FUNCTION twist(state TEXT)
RETURNS TEXT
AS
$$

WITH    perms (perm, i) AS
        (
        VALUES
        (ARRAY[0, 1, 2, 3], 1),
        (ARRAY[1, 0, 3, 2], 2),
        (ARRAY[2, 3, 0, 1], 3),
        (ARRAY[3, 2, 1, 0], 4)
        )
SELECT  targetState
FROM    (
        VALUES
                (
                ARRAY
                (
                SELECT  value::INT
                FROM    unnest(string_to_array(state, NULL)) value
                WHERE   value::INT <= 3
                ),
                ARRAY
                (
                SELECT  value::INT
                FROM    unnest(string_to_array(state, NULL)) value
                WHERE   value::INT > 3
                )
                )
        ) orbits (orbit1, orbit2)
CROSS JOIN LATERAL
        (
        SELECT  ARRAY_AGG(orbit1[index + 1])
        FROM    perms
        CROSS JOIN LATERAL
                UNNEST(perm) index
        WHERE   i = ARRAY_POSITION(orbit1, 0)
        ) c1 (permuted1)
CROSS JOIN LATERAL
        (
        SELECT  ARRAY_AGG(orbit2[index + 1])
        FROM    perms
        CROSS JOIN LATERAL
                UNNEST(perm) index
        WHERE   i = ARRAY_POSITION(orbit2, 4)
        ) c2 (permuted2)
CROSS JOIN LATERAL
        (
        SELECT  STRING_AGG(corner::TEXT, NULL ORDER BY slot)
        FROM    (
                SELECT  corner, position
                FROM    UNNEST(permuted1) WITH ORDINALITY u (corner, position)
                UNION ALL
                SELECT  corner, position + 4
                FROM    UNNEST(permuted2) WITH ORDINALITY u (corner, position)
                ) corners (corner, cornerPosition)
        JOIN    (
                SELECT  position - 1 AS slot,
                        ROW_NUMBER() OVER (ORDER BY corner::INT / 4, position) AS cornerPosition
                FROM    unnest(string_to_array(state, NULL)) WITH ORDINALITY st (corner, position)
                ) slots (slot, cornerPosition)
        USING   (cornerPosition)
        ) ts (targetState)

$$
LANGUAGE sql
IMMUTABLE
STRICT
