--================================================
-- Penrose P3 Tiling created by inflation, initialized by single L triangle
-- Author: Martin Davis  2021

-- Initial triangle is an L triangle ABC with long edge A-C of length 200
-- lying along X axis and centred at the origin
-- L triangle sides are in the ratio 1:1:phi.
-- Thus the height of the B vertex is 100 * sin( phi/2 )

-- The number of output tiles is determined by the depth of recursion,
-- which is specified by the LEVEL value.
--================================================

-- psql -A -t -o penrose3L.svg  < penrose3L-svg.sql

WITH RECURSIVE
init(type, ax,ay, bx,"by", cx,cy) AS (
	SELECT * FROM (VALUES
		( 'L',
			-100::float8, 0::float8,
			0::float8   , 100 * sin( (sqrt(5)+1.0)/4.0 ),
			100::float8, 0::float8
		) ) AS t (type, ax,ay, bx,"by", cx,cy)
),
tri(i, type,  ax,ay, bx,"by", cx,cy, psi, psi2) AS (
	SELECT 0, *,
		-- psi = 1/phi. phi is the Golden Ratio (sqrt(5) + 1)/2
		(sqrt(5)-1)/2 AS psi,
		-- psi^2 = 1 - psi
		1 - (sqrt(5)-1)/2 AS psi2
		FROM init
	UNION ALL
	SELECT i+1, trimap.subtype AS type,
		CASE split
		WHEN 1 THEN ax*psi2 + cx*psi
		WHEN 2 THEN ax*psi2 + bx*psi
		WHEN 3 THEN cx
		WHEN 4 THEN ax*psi + bx*psi2
		WHEN 5 THEN cx END AS ax,
		CASE split
		WHEN 1 THEN ay*psi2 + cy*psi
		WHEN 2 THEN ay*psi2 + "by"*psi
		WHEN 3 THEN cy
		WHEN 4 THEN ay*psi + "by"*psi2
		WHEN 5 THEN cy END AS ay,

		CASE split
		WHEN 1 THEN ax*psi2 + bx*psi
		WHEN 2 THEN ax*psi2 + cx*psi
		WHEN 3 THEN ax*psi2 + cx*psi
		WHEN 4 THEN cx
		WHEN 5 THEN ax*psi + bx*psi2 END AS bx,
		CASE split
		WHEN 1 THEN ay*psi2 + "by"*psi
		WHEN 2 THEN ay*psi2 + cy*psi
		WHEN 3 THEN ay*psi2 + cy*psi
		WHEN 4 THEN cy
		WHEN 5 THEN ay*psi + "by"*psi2 END AS "by",

		CASE split
		WHEN 1 THEN ax
		WHEN 2 THEN bx
		WHEN 3 THEN bx
		WHEN 4 THEN ax
		WHEN 5 THEN bx END AS cx,
		CASE split
		WHEN 1 THEN ay
		WHEN 2 THEN "by"
		WHEN 3 THEN "by"
		WHEN 4 THEN ay
		WHEN 5 THEN "by" END AS cy,

		psi, psi2
	FROM tri INNER JOIN (VALUES ( 'L', 1, 'L' ),
		( 'L', 2, 'S' ),
		( 'L', 3, 'L' ),
		( 'S', 4, 'S'),
		( 'S', 5, 'L') ) AS trimap(type, split, subtype)
		ON tri.type = trimap.type
	WHERE i <= 5 ),  -- LEVEL
toptri AS (
	SELECT * FROM tri WHERE i = 5  -- LEVEL
),
conjugate AS (
	SELECT type,ax,ay,bx,"by",cx,cy FROM toptri
	UNION ALL
	SELECT type,ax,-ay,bx,-"by",cx,-cy FROM toptri
),
rhombs AS (
	SELECT type,ax,ay,bx,"by",cx,cy,
		ax + (cx-ax)/2 AS midx,
		ay + (cy-ay)/2 AS midy
	FROM conjugate
),
tiling AS (
	SELECT DISTINCT ON (midx, midy) type,ax,ay,bx,"by",cx,cy,
		midx - (bx - midx) AS dx,
		midy - ("by" - midy) AS dy,
		CASE type WHEN 'L' THEN 'burlywood' WHEN 'S' THEN 'brown' END AS clr
	FROM rhombs
)
SELECT '<svg viewBox="-110 -80 220 160" '
    || 'style="stroke-width:0.4 ;stroke:#ffffff" xmlns="http://www.w3.org/2000/svg">'
    || E'\n'
    || string_agg(
        '<polygon style="fill:' || clr || ';"  '
        || ' points="'
			|| ax || ',' || ay || ' '
			|| bx || ',' || "by" || ' '
			|| cx || ',' || cy || ' '
			|| dx || ',' || dy
        || '" />', E'\n' )
    || E'\n' || '</svg>' || E'\n' AS svg
  FROM tiling;
