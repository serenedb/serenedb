#!/bin/bash
# Regenerates resources/tests/iceberg (the fixture IS checked in -- rerun
# this after changing gen_iceberg_fixture.py and commit the result).
# Idempotent and concurrency-safe: a stamp inside the fixture skips
# regeneration until gen_iceberg_fixture.py changes, and an flock
# serializes parallel runs. Runs in a throwaway python container, so the
# host needs nothing but docker.
set -euo pipefail

REPO=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
GEN="$REPO/scripts/gen_iceberg_fixture.py"
OUT="$REPO/resources/tests/iceberg"
STAMP="$OUT/.generated"
LOCK="$REPO/.cache/iceberg-fixture.lock"

mkdir -p "$REPO/.cache"
exec 9>"$LOCK"
flock 9

if [[ -f "$STAMP" && "$STAMP" -nt "$GEN" ]]; then
	exit 0
fi

echo "Generating iceberg test fixture ($OUT) in docker..."
docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp \
	-v "$REPO:/serenedb" python:3.12-slim bash -c \
	"pip -q install --target /tmp/deps 'pyiceberg[sql-sqlite]==0.11.1' pyarrow==25.0.1 fastavro==1.12.2 && \
	 PYTHONPATH=/tmp/deps ICE_FIXTURE_WORK=/tmp/ice_fixture_work python /serenedb/scripts/gen_iceberg_fixture.py"
touch "$STAMP"
