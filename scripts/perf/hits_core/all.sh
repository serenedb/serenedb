#!/usr/bin/env bash
set -uo pipefail
if [ $# -ne 1 ]; then
	echo "usage: all.sh <validate|full>" >&2
	exit 2
fi
MODE=$1
source "$(dirname "$0")/lib.sh"
for side in main_duck main_search branch; do
	echo "=== $side $(date +%T)"
	bash "$HB_DIR/run.sh" "$side" "$MODE" >"$HB_OUT/${MODE}_$side.log" 2>&1
	echo "exit $? $(date +%T)"
	tail -2 "$HB_OUT/${MODE}_$side.log"
done
echo "=== small $(date +%T)"
bash "$HB_DIR/small_all.sh" "$MODE" >"$HB_OUT/${MODE}_small.log" 2>&1
echo "exit $? $(date +%T)"
python3 "$HB_DIR/compare.py" "$MODE" "$HB_RESULTS"
