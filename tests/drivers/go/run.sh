#!/bin/bash
# Go driver harness: jackc/pgx (D2) and lib/pq (D3).

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if ! command -v go >/dev/null 2>&1; then
	echo "[go] go not found" >&2
	exit 1
fi

cd "$SCRIPT_DIR"

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

set -o pipefail
final=0
if command -v go-junit-report >/dev/null 2>&1; then
	go test -v ./... 2>&1 | tee "$JUNIT/tests-drivers-go.log" |
		go-junit-report -set-exit-code \
			>"$JUNIT/tests-drivers-go-junit.xml" || final=1
else
	go test -v ./... 2>&1 | tee "$JUNIT/tests-drivers-go.log"
	final=${PIPESTATUS[0]}
fi
exit "$final"
