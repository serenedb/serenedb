WITH	RECURSIVE
	bin AS
	(
	SELECT	DECODE(data, 'hex') AS buffer
	FROM	input
	),
	header AS
	(
	SELECT	h.*, io.*
	FROM	bin
	CROSS JOIN LATERAL
		(
		SELECT	GET_BYTE(buffer, 10)::BIT(8) AS flags
		) q
	CROSS JOIN LATERAL
		(
		SELECT	CONVERT_FROM(SUBSTR(buffer, 1, 6), 'LATIN1') AS version,
			(GET_BYTE(buffer, 7)::BIT(8) || GET_BYTE(buffer, 6)::BIT(8))::BIT(16)::INT AS width,
			(GET_BYTE(buffer, 9)::BIT(8) || GET_BYTE(buffer, 8)::BIT(8))::BIT(16)::INT AS height,
			flags::BIT(1) = B'1' AS gct,
			(flags << 1)::BIT(3)::INT + 1 AS depth,
			(flags << 4) = B'1' AS color_sort,
			2 << (flags << 5)::BIT(3)::INT AS gct_size,
			GET_BYTE(buffer, 11)::INT AS bci,
			GET_BYTE(buffer, 12)::INT AS aspect
		) h
	CROSS JOIN LATERAL
		(
		SELECT	13 + 3 * gct_size AS blocks_offset
		) io
	),
	blocks AS
	(
	SELECT	blocks_offset AS current,
		GET_BYTE(buffer, blocks_offset) AS intro
	FROM	bin
	CROSS JOIN
		header
	UNION ALL
	SELECT	current,
		GET_BYTE(buffer, current) AS intro
	FROM	(
		SELECT	current AS previous,
			intro AS previous_intro
		FROM	blocks
		) b
	CROSS JOIN
		bin
	CROSS JOIN LATERAL
		(
		SELECT	previous + GET_BYTE(buffer, previous + 2) + 4 AS current
		) q
	WHERE	previous_intro = x'21'::INT
	),
	image_offset AS
	(
	SELECT	current AS image_offset
	FROM	blocks
	WHERE	intro = x'2C'::INT
	),
	image_header AS
	(
	SELECT	q.*, l.*, l2.*
	FROM	image_offset
	CROSS JOIN
		bin
	CROSS JOIN
		header h
	CROSS JOIN LATERAL
		(
		SELECT	GET_BYTE(buffer, image_offset + 9)::BIT(8) AS flags
		) f
	CROSS JOIN LATERAL
		(
		SELECT	(GET_BYTE(buffer, image_offset + 2)::BIT(8) || GET_BYTE(buffer, image_offset + 1)::BIT(8))::BIT(16)::INT AS left,
			(GET_BYTE(buffer, image_offset + 4)::BIT(8) || GET_BYTE(buffer, image_offset + 3)::BIT(8))::BIT(16)::INT AS top,
			(GET_BYTE(buffer, image_offset + 6)::BIT(8) || GET_BYTE(buffer, image_offset + 5)::BIT(8))::BIT(16)::INT AS width,
			(GET_BYTE(buffer, image_offset + 8)::BIT(8) || GET_BYTE(buffer, image_offset + 7)::BIT(8))::BIT(16)::INT AS height,
			flags::BIT(1) = b'1' AS has_lct,
			(flags << 1)::BIT(1) = b'1' AS interlace,
			(flags << 2)::BIT(1) = b'1' AS sort
		) q
	CROSS JOIN LATERAL
		(
		SELECT	CASE WHEN has_lct THEN 2 << (flags << 5)::BIT(3)::INT ELSE 0 END AS lct_size
		) l
	CROSS JOIN LATERAL
		(
		SELECT	image_offset + 10 + lct_size AS image_data_offset
		) l2
	)
SELECT	*
FROM	image_header;
