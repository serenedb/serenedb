WITH	RECURSIVE
	bin AS
	(
	SELECT	DECODE(data, 'hex') AS buffer
	FROM	input
	)
SELECT	ENCODE(buffer, 'hex')
FROM	bin;

