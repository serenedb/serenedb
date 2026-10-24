#!/bin/bash

set -euo pipefail

WORKSPACE="${WORKSPACE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$WORKSPACE"
# shellcheck source=scripts/ci/config.sh
source scripts/ci/config.sh
sdb_config dev
RUN="$1"
SHARDS="$2"

rm -rf out/selection-map
mkdir -p out/selection-map
docker run --rm --user "$(id -u):$(id -g)" --group-add "$(stat -c '%g' /var/run/docker.sock)" \
	-e HOME=/tmp -v /var/run/docker.sock:/var/run/docker.sock \
	-v "$WORKSPACE:$WORKSPACE" -w "$WORKSPACE" "$BUILD_IMAGE" \
	python3 scripts/ci/ci.py selection-map --run "$RUN" --config "$CONFIG" --arch "$ARCH" \
	--work out/selection-map --shards "$SHARDS"
