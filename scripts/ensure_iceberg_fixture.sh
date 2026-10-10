#!/bin/bash
# Generates resources/tests/iceberg on demand (the fixture is not checked
# in). Idempotent and concurrency-safe: a stamp inside the fixture skips
# regeneration until gen_iceberg_fixture.py changes, and an flock
# serializes parallel test runners.
set -euo pipefail

REPO=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
GEN="$REPO/scripts/gen_iceberg_fixture.py"
OUT="$REPO/resources/tests/iceberg"
STAMP="$OUT/.generated"
# NOT under .cache: the gtest-cache bind of 042/043 makes the daemon create
# a root-owned .cache on the host, unwritable for us.
LOCK="$REPO/resources/tests/iceberg-fixture.lock"

exec 9>"$LOCK"
flock 9

if [[ -f "$STAMP" && "$STAMP" -nt "$GEN" ]]; then
	exit 0
fi

echo "Generating iceberg test fixture ($OUT)..."
if python3 -c 'import fastavro, pyarrow, pyiceberg' 2>/dev/null; then
	work=$(mktemp -d)
	ICE_FIXTURE_WORK="$work" python3 "$GEN"
	rm -rf "$work"
else
	docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -e ICE_FIXTURE_WORK=/tmp/ice_fixture_work \
		-v "$REPO:/serenedb" "${BUILD_IMAGE:-serenedb/serenedb-build-ubuntu:latest}" \
		python3 /serenedb/scripts/gen_iceberg_fixture.py
fi
touch "$STAMP"
