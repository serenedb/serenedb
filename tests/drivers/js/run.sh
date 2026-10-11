#!/bin/bash
# JS driver harness: node-postgres (`pg`), `postgres.js` and the ORM / schema
# tool suites (Prisma, TypeORM, Sequelize, Knex, Drizzle, Kysely,
# postgres-meta). Vitest is the test runner. Reads SDB_DRV_* env from
# tests/drivers/run.sh.

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

# One vitest run per suite so JUnit output stays per-suite and a failing
# suite does not hide the others.
final=0
for test_file in test/*.test.js; do
	name=$(basename "$test_file" .test.js)
	echo "[js][$name] running"
	if ! node_modules/.bin/vitest run --reporter=junit \
		--outputFile="$JUNIT/tests-drivers-js-${name}-junit.xml" \
		"$test_file"; then
		final=1
	fi
done
exit "$final"
