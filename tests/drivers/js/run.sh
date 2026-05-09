#!/bin/bash
# JS driver harness: node-postgres (`pg`) and `postgres.js`. Vitest is the test
# runner. Reads SDB_DRV_* env from tests/drivers/run.sh.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if ! command -v node >/dev/null 2>&1; then
	echo "[js] node not found; skipping" >&2
	exit 0
fi
if ! command -v npm >/dev/null 2>&1; then
	echo "[js] npm not found; skipping" >&2
	exit 0
fi

cd "$SCRIPT_DIR"

if [[ ! -d node_modules ]]; then
	npm install --silent --no-fund --no-audit
fi

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

JUNIT_DIR="$JUNIT" npx vitest run --reporter=junit \
	--outputFile="$JUNIT/tests-drivers-js-junit.xml" || exit 1

# postgres.js suite (D3): runs only when the test file is present.
if [[ -f test/postgres-js.test.js ]]; then
	JUNIT_DIR="$JUNIT" npx vitest run --reporter=junit \
		--outputFile="$JUNIT/tests-drivers-js-postgres-js-junit.xml" \
		test/postgres-js.test.js || exit 1
fi
