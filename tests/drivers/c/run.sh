#!/bin/bash
# C / libpq driver harness. The harness is a single C program that
# loads tests/drivers/spec/types.yaml via a tiny Python helper (the
# alternative is a YAML parser in C, which is overkill).

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

if ! command -v cc >/dev/null 2>&1; then
	echo "[c] no C compiler" >&2
	exit 0
fi
if ! pkg-config --exists libpq 2>/dev/null; then
	echo "[c] libpq-dev not installed; skipping" >&2
	exit 0
fi

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

# Build (cached on subsequent runs)
if [[ harness.c -nt harness ]] || [[ ! -x harness ]]; then
	cc -O0 -g -Wall -Wextra -o harness harness.c \
		$(pkg-config --cflags --libs libpq) || exit 1
fi

./harness "$JUNIT/tests-drivers-c-junit.xml"
