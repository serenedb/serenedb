WITH points AS (
    SELECT column0 as x, column1 as y, column2 as z, ROW_NUMBER() OVER() as rowid
    FROM read_csv_auto('08/example.csv', header = false, delim=',')
),
all_edges AS (
    SELECT
        a.rowid AS src,
        b.rowid AS dst,
        (a.x - b.x)*(a.x - b.x)
      + (a.y - b.y)*(a.y - b.y)
      + (a.z - b.z)*(a.z - b.z) AS dist2
           ,a.x, a.y, a.z, b.x AS x_1, b.y AS y_2, b.z AS z_3
    FROM points a
    JOIN points b ON a.rowid < b.rowid
),
directed_edges AS (
    SELECT dst, src, x, y, z, x_1, y_2, z_3
    FROM all_edges
    ORDER BY dist2
    LIMIT 10
),
edges AS (
    SELECT dst, src FROM directed_edges
    UNION ALL
    SELECT src, dst FROM directed_edges
),
nodes AS (
    SELECT DISTINCT node
    FROM (
        SELECT src as node
        FROM all_edges
        UNION ALL
        SELECT dst as node
        FROM all_edges
    )
),
weakly_components AS (
    WITH RECURSIVE walks(node, front) AS (
        SELECT node, node AS front
        FROM nodes
        UNION
        SELECT walks.node, edges.dst AS front
        FROM walks, edges
        WHERE walks.front = edges.src
    ),

    components AS (
        SELECT node, MIN(front) AS component
        FROM walks
        GROUP BY node
    )

    SELECT *
    FROM components
    ORDER BY component, node
),
top_3_components AS(
    SELECT component, COUNT(component) as number
    FROM weakly_components
    GROUP BY component
    ORDER BY COUNT(component) DESC
    LIMIT 3
)
SELECT *
FROM weakly_components w
JOIN points p ON w.node = p.rowid

