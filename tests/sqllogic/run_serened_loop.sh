#!/bin/bash

# Runs serened in a restart loop. Recovery tests intentionally crash serened,
# and this script ensures it comes back up automatically.
#
# Usage (local dev):
#   ./run_serened_loop.sh                    # defaults, temp datadir
#   ./run_serened_loop.sh /path/to/datadir   # custom data dir

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

: "${BUILD_DIR:=build}"
: "${WORKSPACE:=$(realpath "$SCRIPT_DIR/../../")}"
: "${PORT:=7777}"

if [[ -n "$1" ]]; then
	DATADIR="$1"
	mkdir -p "$DATADIR"
else
	DATADIR=$(mktemp -d)
	trap 'rm -rf "$DATADIR"' EXIT INT TERM
fi

SERENED="$WORKSPACE/$BUILD_DIR/bin/serened"

echo "Binary: $SERENED"
echo "Data directory: $DATADIR"
echo "Listening on port: $PORT"

if [[ ! -x "$SERENED" ]]; then
	echo "ERROR: serened binary not found at $SERENED"
	exit 1
fi

while true; do
	"$SERENED" "$DATADIR" \
		--server_endpoints "pgsql+tcp://0.0.0.0:$PORT"
	echo "serened exited with code $?, restarting..."
done
