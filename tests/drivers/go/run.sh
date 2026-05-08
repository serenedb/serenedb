#!/bin/bash
# Go driver harness: jackc/pgx (D2) and lib/pq (D3).

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if ! command -v go >/dev/null 2>&1; then
	echo "[go] go not found; skipping" >&2
	exit 0
fi

cd "$SCRIPT_DIR"

# Resolve module deps + go.sum on first run inside the container.
GOFLAGS="-mod=mod" go mod tidy >/dev/null 2>&1 || true

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

# go test does not natively emit JUnit XML. Try to install go-junit-report
# on demand; if that fails we still run go test and let its exit code drive
# the suite, just without the XML artifact.
GOBIN="${GOBIN:-${GOPATH:-$HOME/go}/bin}"
PATH="$GOBIN:$PATH"
if ! command -v go-junit-report >/dev/null 2>&1; then
	GOFLAGS="-mod=mod" go install github.com/jstemmer/go-junit-report/v2@latest >/dev/null 2>&1 || true
fi

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
