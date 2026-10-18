#!/bin/bash
# JS driver harness: node-postgres (`pg`) and `postgres.js`. Vitest is the test
# runner. Reads SDB_DRV_* env from tests/drivers/run.sh.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if ! command -v node >/dev/null 2>&1; then
	echo "[js] node not found" >&2
	exit 1
fi

cd "$SCRIPT_DIR"

if [[ ! -e node_modules && -n "${SDB_DRIVERS_DEPS:-}" ]]; then
	ln -s "$SDB_DRIVERS_DEPS/js/node_modules" node_modules
fi
if [[ ! -x node_modules/.bin/vitest ]]; then
	echo "[js] node_modules is missing; run npm ci in $SCRIPT_DIR" >&2
	exit 1
fi

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

JUNIT_DIR="$JUNIT" node_modules/.bin/vitest run --reporter=junit \
	--outputFile="$JUNIT/tests-drivers-js-junit.xml" || exit 1

# postgres.js suite (D3): runs only when the test file is present.
if [[ -f test/postgres-js.test.js ]]; then
	JUNIT_DIR="$JUNIT" node_modules/.bin/vitest run --reporter=junit \
		--outputFile="$JUNIT/tests-drivers-js-postgres-js-junit.xml" \
		test/postgres-js.test.js || exit 1
fi
