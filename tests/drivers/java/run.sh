#!/bin/bash
# Java driver harness: pgjdbc.
# Maven is the build/test runner. Reads SDB_DRV_* env from tests/drivers/run.sh.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if ! command -v mvn >/dev/null 2>&1; then
	echo "[java] mvn not found; skipping" >&2
	exit 0
fi

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

cd "$SCRIPT_DIR"

mvn_args=(-q -B -Dtest.host="${SDB_DRV_HOST}"
	-Dtest.port="${SDB_DRV_PORT}"
	-Dtest.database="${SDB_DRV_DATABASE}"
	-Dtest.user="${SDB_DRV_USER}"
	-Dtest.protocols="${SDB_DRV_PROTOCOLS}"
	-Dtest.types="${SDB_DRV_TYPES}"
	-Dtest.run_id="${SDB_DRV_RUN_ID}"
	-Dtest.spec="${SDB_DRV_SPEC:-${SCRIPT_DIR}/../spec}"
)
[[ "${SDB_DRV_DEBUG:-false}" == "true" ]] && mvn_args+=(-X)

if ! mvn "${mvn_args[@]}" test; then
	cp -f target/surefire-reports/TEST-*.xml "$JUNIT/" 2>/dev/null || true
	exit 1
fi
cp -f target/surefire-reports/TEST-*.xml "$JUNIT/" 2>/dev/null || true
exit 0
