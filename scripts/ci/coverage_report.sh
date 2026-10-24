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
SHARDS="${5:-}"

bundle=$(python3 scripts/ci/ci.py fetch --config "$CONFIG" --arch "$ARCH" --digest "$DIGEST")
out="out/coverage-${CONFIG}"
rm -rf "$out"
mkdir -p "$out/profiles"
python3 scripts/ci/ci.py download profdata "$RUN" "$CONFIG-$ARCH-" "$out/profiles"
label="coverage $CONFIG/$ARCH"
got=$(find "$out/profiles" -type f | wc -l)
if [[ -n "$SHARDS" ]] && ((got < SHARDS)); then
	label+=" (partial: profiles of $got of $SHARDS shards)"
fi

docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
	-v "$WORKSPACE:$WORKSPACE" -w "$WORKSPACE" -v "$bundle:$bundle" "$BUILD_IMAGE" \
	python3 scripts/coverage/diff.py --build-dir "$bundle" --profiles "$out/profiles" --base "$BASE" \
	--label "$label" --out "$out"
rm -rf "$out/profiles" "$out/merged.profdata"
python3 scripts/ci/ci.py nearest --coverage "$out/coverage.json" --spans "$out/spans.json" --arch "$ARCH" ||
	echo "warning: no nearest tests for the uncovered lines" >&2

if [[ "$CONFIG" == dev && -n "${SDB_PR:-}" ]]; then
	read -ra test_args <<<"${TEST_ARGS:-}"
	python3 scripts/ci/ci.py timing --config "$CONFIG" --arch "$ARCH" --out "$out/timing.json"
	docker run --rm --user "$(id -u):$(id -g)" --group-add "$(stat -c '%g' /var/run/docker.sock)" \
		-e HOME=/tmp -e SDB_PR -e SDB_STORE_HOST -v /var/run/docker.sock:/var/run/docker.sock \
		-v "$WORKSPACE:$WORKSPACE" -w "$WORKSPACE" -v "$bundle:$bundle" "$BUILD_IMAGE" \
		python3 scripts/ci/test.py --build-dir "$bundle" --config dev --timing "$out/timing.json" \
		--list "${test_args[@]}" --changed "$BASE" --selection-report "$out/selection.json" \
		--selection-spans "$out/spans.json" >/dev/null ||
		echo "warning: no selection report" >&2
fi
