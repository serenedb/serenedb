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
: "${BUILD_IMAGE:=serenedb/serenedb-build-ubuntu:latest}"

bundle=$(python3 scripts/ci/ci.py fetch --config "$CONFIG" --arch "$ARCH" --digest "$DIGEST")
out="out/coverage-${CONFIG}"
rm -rf "$out"
mkdir -p "$out/profiles"
python3 - "$RUN" "$CONFIG-$ARCH" "$out/profiles" <<'EOF'
import json, sys, urllib.request
sys.path.insert(0, "scripts/ci")
from testlib import store
run, prefix, dest = sys.argv[1:]
with urllib.request.urlopen(store.url(f"profdata/{run}/"), timeout=60) as r:
    entries = json.load(r)
for e in entries:
    if e["type"] == "file" and e["name"].startswith(prefix):
        store.get(f"profdata/{run}/{e['name']}", f"{dest}/{e['name']}")
        print(e["name"])
EOF

docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
	-v "$WORKSPACE:$WORKSPACE" -w "$WORKSPACE" -v "$bundle:$bundle" "$BUILD_IMAGE" bash -c '
		set -euo pipefail
		out=$0 bundle=$1 base=$2
		llvm-profdata-23 merge -sparse -failure-mode=any -o "$out/merged.profdata" "$out"/profiles/*.profdata
		objects=()
		for b in "$bundle"/bin/* "$bundle"/third_party/duckdb/test/unittest; do
			[[ -f "$b" ]] && llvm-readelf-23 -S "$b" 2>/dev/null | grep -q __llvm_covmap && objects+=("$b")
		done
		python3 scripts/coverage/diff.py --profdata "$out/merged.profdata" --objects "${objects[@]}" \
			--base "$base" --label "coverage $(basename "$out" | cut -d- -f2)/$(uname -m)" --out "$out"
	' "$out" "$bundle" "$BASE"
rm -rf "$out/profiles"
