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

CLEANUP_DATADIR=0
if [[ -n "$1" ]]; then
	DATADIR="$1"
	mkdir -p "$DATADIR"
else
	DATADIR=$(mktemp -d)
	CLEANUP_DATADIR=1
fi

SERENED="$WORKSPACE/$BUILD_DIR/bin/serened"

echo "Binary: $SERENED"
echo "Data directory: $DATADIR"
echo "Listening on port: $PORT"

if [[ ! -x "$SERENED" ]]; then
	echo "ERROR: serened binary not found at $SERENED"
	exit 1
fi

STOP=0
CHILD=
on_term() {
	STOP=1
	[[ -n "$CHILD" ]] && kill -TERM "$CHILD" 2>/dev/null
}
on_exit() {
	[[ -n "$CHILD" ]] && kill -TERM "$CHILD" 2>/dev/null
	[[ "$CLEANUP_DATADIR" -eq 1 ]] && rm -rf "$DATADIR"
}
trap on_term INT TERM
trap on_exit EXIT

while [[ "$STOP" -eq 0 ]]; do
	"$SERENED" "$DATADIR" \
		--server_endpoints "pgsql+tcp://0.0.0.0:$PORT" &
	CHILD=$!
	[[ "$STOP" -eq 1 ]] && kill -TERM "$CHILD" 2>/dev/null
	wait "$CHILD"
	code=$?
	[[ "$STOP" -eq 0 ]] && echo "serened exited with code $code, restarting..."
done
