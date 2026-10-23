#!/bin/bash

set -euo pipefail

WORKSPACE="${WORKSPACE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$WORKSPACE"
# shellcheck source=scripts/ci/config.sh
source scripts/ci/config.sh
sdb_config "$1"
DIGEST="$2"
RUN="$3"
BASE="$4"

bundle=$(python3 scripts/ci/ci.py fetch --config "$CONFIG" --arch "$ARCH" --digest "$DIGEST")
out="out/coverage-${CONFIG}"
rm -rf "$out"
mkdir -p "$out/profiles"
python3 scripts/ci/ci.py download "profdata/$RUN/" "$CONFIG-$ARCH-" "$out/profiles"

docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
	-v "$WORKSPACE:$WORKSPACE" -w "$WORKSPACE" -v "$bundle:$bundle" "$BUILD_IMAGE" \
	python3 scripts/coverage/diff.py --build-dir "$bundle" --profiles "$out/profiles" --base "$BASE" \
	--label "coverage $CONFIG/$ARCH" --out "$out"
rm -rf "$out/profiles" "$out/merged.profdata"
python3 scripts/ci/ci.py nearest --coverage "$out/coverage.json" --arch "$ARCH" ||
	echo "warning: no nearest tests for the uncovered lines" >&2
