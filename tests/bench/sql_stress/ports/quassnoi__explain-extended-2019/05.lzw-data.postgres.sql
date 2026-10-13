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
	),
	image_data AS
	(
	SELECT	GET_BYTE(buffer, image_data_offset) AS code_size,
		image_data_offset + 1 AS image_first_block_offset
	FROM	image_header
	CROSS JOIN
		bin
	),
	image_blocks AS
	(
	SELECT	image_first_block_offset AS block_offset,
		block_size,
		SUBSTR(buffer, image_first_block_offset + 2, block_size) AS block_data
	FROM	image_data
	CROSS JOIN
		bin
	CROSS JOIN LATERAL
		(
		SELECT	GET_BYTE(buffer, image_first_block_offset) AS block_size
		) l
	UNION ALL
	SELECT	new_offset AS block_offset,
		new_block_size AS block_size,
		SUBSTR(buffer, new_offset + 2, new_block_size) AS block_data
	FROM	image_blocks
	CROSS JOIN LATERAL
		(
		SELECT	block_offset + block_size + 1 AS new_offset
		) no
	CROSS JOIN
		bin
	CROSS JOIN LATERAL
		(
		SELECT	GET_BYTE(buffer, new_offset) AS new_block_size
		) nbs
	WHERE	new_block_size > 0
	),
	lzw_data AS
	(
	SELECT	code_size,
		(1 << code_size) AS clear_code,
		(1 << code_size) + 1 AS eof_code,
		compressed
	FROM	(
		SELECT	STRING_AGG(block_data, '' ORDER BY block_offset) compressed
		FROM	image_blocks
		) i
	CROSS JOIN
		image_data
	)
SELECT	code_size, ENCODE(compressed, 'hex')
FROM	lzw_data;
