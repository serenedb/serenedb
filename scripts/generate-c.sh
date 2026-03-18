#!/bin/sh

TOOL="$1"
OUTPUT="$2"
INPUT="$3"

if test "x$TOOL" = x -o "x$OUTPUT" = x -o "x$INPUT" = x; then
	echo "usage: $0 <tool> <output> <input>"
	exit 1
fi

PREFIX=$(echo ${OUTPUT} | sed -e 's:\.cpp$::')

# clean up after ourselves
trap "rm -f ${PREFIX}.tmp" EXIT TERM HUP INT

TOOL_NAME=$(basename "${TOOL}")

case "${TOOL_NAME}" in
bison*)
	MAJOR_VER=$(${TOOL} --version | grep bison | sed -e "s;.* ;;" -e "s;\..*;;")
	if test "${MAJOR_VER}" -ge "3"; then
		TOOL_OPTS="--warnings=deprecated,other,error=conflicts-sr,error=conflicts-rr"
	fi
	${TOOL} -d "${TOOL_OPTS}" -o "${OUTPUT}" "${INPUT}"
	test -f "${PREFIX}.hpp" || exit 1
	test -f "${PREFIX}.cpp" || exit 1
	echo "/* clang-format off */" | cat - "${PREFIX}.hpp" >"${PREFIX}.tmp"
	cp "${PREFIX}.tmp" "${PREFIX}.hpp"
	;;
flex*)
	${TOOL} -L -o "${OUTPUT}" "${INPUT}"
	test -f "${PREFIX}.cpp" || exit 1
	;;
*)
	echo "Unknown tool: ${TOOL_NAME}"
	exit 1
	;;
esac

echo "/* clang-format off */" | cat - "${PREFIX}.cpp" | sed 's/[[:space:]]*$//' >"${PREFIX}.tmp"
# ensure file ends with exactly one newline
printf '%s\n' "$(cat "${PREFIX}.tmp")" >"${PREFIX}.cpp"
