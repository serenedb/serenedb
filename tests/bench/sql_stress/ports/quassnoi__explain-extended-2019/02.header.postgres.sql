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
			flags::BIT(1) = B'1' AS gtc,
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
	)
SELECT	*
FROM	header;

