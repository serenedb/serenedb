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
	),
	lzw_bits AS
	(
	SELECT	current_code_size,
		clear_code AS code,
		ARRAY[]::INT[] AS output_chunk,
		0 AS next_bit_offset,
		NULL::HSTORE AS codes,
		0 AS next_table_key,
		0 AS next_index
	FROM	lzw_data
	CROSS JOIN LATERAL
		(
		SELECT	code_size + 1 AS current_code_size
		) cc
	UNION ALL
	SELECT	next_code_size,
		code,
		output_chunk,
		bit_offset + current_code_size,
		new_codes AS codes,
		next_table_key,
		next_index
	FROM	(
		SELECT	code AS previous_code, current_code_size, next_bit_offset AS bit_offset, codes, next_table_key AS current_table_key,
			next_index + COALESCE(ARRAY_UPPER(output_chunk, 1), 0) AS next_index,
			ld.*
		FROM	lzw_bits
		CROSS JOIN
			(
			SELECT	code_size AS initial_code_size,
				clear_code,
				eof_code,
				compressed
			FROM	lzw_data
			) ld
		) lb
	CROSS JOIN LATERAL
		(
		SELECT	bit_offset / 8 AS byte_offset
		) bo
	CROSS JOIN LATERAL
		(
		SELECT	(
			CASE WHEN byte_offset < LENGTH(compressed) - 2 THEN GET_BYTE(compressed, byte_offset + 2) ELSE 0 END::BIT(8) ||
			CASE WHEN byte_offset < LENGTH(compressed) - 1 THEN GET_BYTE(compressed, byte_offset + 1) ELSE 0 END::BIT(8) ||
			GET_BYTE(compressed, byte_offset)::BIT(8)
			)::BIT(24) AS cut
		) cc
	CROSS JOIN LATERAL
		(
		SELECT	SUBSTRING(cut, 25 - current_code_size - bit_offset % 8, current_code_size)::INT AS code
		) l
	CROSS JOIN LATERAL
		(
		SELECT	*
		FROM	(
			SELECT	ARRAY[]::INT[] AS output_chunk,
				HSTORE(ARRAY[]::TEXT[][]) AS new_codes,
				eof_code + 1 AS next_table_key,
				initial_code_size + 1 AS next_code_size
			WHERE	code = clear_code
			UNION ALL
			SELECT	CASE WHEN code < clear_code THEN ARRAY[code] ELSE (codes->(code::TEXT))::INT[] END AS output_chunk,
				codes AS new_codes,
				current_table_key AS next_table_key,
				current_code_size AS next_code_size
			WHERE	previous_code = clear_code
			UNION ALL
			SELECT	output_chunk,
				CASE current_table_key WHEN 4095 THEN codes ELSE codes || HSTORE(current_table_key::TEXT, next_table_chunk::TEXT) END AS new_codes,
				next_table_key,
				CASE next_table_key WHEN (1 << current_code_size) THEN current_code_size + 1 ELSE current_code_size END AS next_code_size
			FROM	(
				SELECT	CASE WHEN previous_code < clear_code THEN ARRAY[previous_code] ELSE (codes->(previous_code::TEXT))::INT[] END AS previous_chunk,
					LEAST(current_table_key + 1, 4095) AS next_table_key,
					code < clear_code OR codes ? (code::TEXT) AS code_in_table
				) pc
			CROSS JOIN LATERAL
				(
				SELECT	output_chunk,
					previous_chunk || output_chunk[1] AS next_table_chunk
				FROM	(
					SELECT	CASE WHEN code < clear_code THEN ARRAY[code] ELSE (codes->(code::TEXT))::INT[] END AS output_chunk
					) q
				WHERE	code_in_table
				UNION ALL
				SELECT	output_chunk, output_chunk AS next_table_chunk
				FROM	(
					SELECT	previous_chunk || previous_chunk[1] AS output_chunk
					) q
				WHERE	NOT code_in_table
					AND code <> eof_code
				) q
			) q
		LIMIT 1
		) ns
	WHERE	previous_code IS DISTINCT FROM eof_code
		AND bit_offset < LENGTH(compressed) * 8 
	),
	indices AS
	(
	SELECT	idx, next_index + position - 1 AS rn
	FROM	lzw_bits
	CROSS JOIN LATERAL
		UNNEST(output_chunk) WITH ORDINALITY q (idx, position)
	),
	pixels AS
	(
	SELECT	idx,
		rn % width AS x,
		rn / width AS y,
		luma
	FROM	indices
	CROSS JOIN
		bin
	CROSS JOIN
		header
	CROSS JOIN LATERAL
		(
		SELECT	255 max_value,
			2.2 gamma,
			GET_BYTE(buffer, 13 + idx * 3 + 0) r,
			GET_BYTE(buffer, 13 + idx * 3 + 1) g,
			GET_BYTE(buffer, 13 + idx * 3 + 2) b,
			.2126 rw,
			.7152 gw,
			.0722 bw
		) c
	CROSS JOIN LATERAL
		(
		SELECT	((r::FLOAT / max_value) ^ gamma) * rw +
			((g::FLOAT / max_value) ^ gamma) * gw +
			((b::FLOAT / max_value) ^ gamma) * bw AS luma
		) l
	)
SELECT	*
FROM	pixels;

