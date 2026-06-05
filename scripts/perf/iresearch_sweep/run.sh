#!/usr/bin/env bash
# run.sh -- drives the full sweep across {binary} x {case} x {size}.
#
# Usage:
#   ./run.sh medium                # 9 cases x medium sizes x {mine, main}
#   ./run.sh large                 # 9 cases x large sizes  x {mine, main}
#   ./run.sh both                  # medium then large
#   CASES="uuid integer" ./run.sh medium    # subset
#
# Output: scripts/perf/results/sweep_<ts>/
#   raw/<binary>_<case>_<rows>_<phase>.{wall,perf,out,err}
#   raw/<binary>_<case>_<rows>.csv
#   summary.md   (after summarize.py)
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../../.." && pwd)
MINE_BIN="$REPO/build_perf/bin/serened"
MAIN_BIN="$REPO/../serenedb-main/build_perf/bin/serened"

[[ -x "$MINE_BIN" ]] || {
	echo "missing $MINE_BIN" >&2
	exit 1
}
[[ -x "$MAIN_BIN" ]] || {
	echo "missing $MAIN_BIN" >&2
	exit 1
}

MODE="${1:-medium}"
TS=$(date +%Y%m%dT%H%M%S)
OUT="$REPO/scripts/perf/results/sweep_${TS}"
mkdir -p "$OUT/raw"
echo "Output: $OUT"

# CPU isolation: pin everything to cores 0-15 so 16-31 stays free for other devs.
TASKSET="taskset -c 0-15"

declare -a CASES_ALL=(english_icu english_simple ngram uuid url integer bigint boolean hnsw)
CASES=${CASES:-${CASES_ALL[*]}}

sizes_for_mode() {
	case "$1" in
	medium) echo medium ;;
	large) echo large ;;
	both) echo medium large ;;
	*)
		echo "unknown mode $1" >&2
		return 2
		;;
	esac
}

rows_for_case_size() {
	local case="$1" size="$2"
	if [[ -n "${ROWS:-}" ]]; then
		echo "$ROWS"
		return
	fi
	awk -F'\t' -v c="$case" -v s="$size" '
    $1==c { if (s=="medium") print $5; else print $6; exit }
  ' "$(dirname "$0")/cases.manifest"
}

run_combo() {
	local bin="$1" label="$2" case="$3" rows="$4" port="$5"
	local cmd=("$REPO/scripts/perf/iresearch_sweep/run_one.sh" "$bin" "$case" "$rows" "$OUT/raw" "$port")
	echo "== $label $case rows=$rows port=$port =="
	# don't abort the whole sweep if one combo fails
	BIN_LABEL="$label" $TASKSET "${cmd[@]}" 2>&1 | tee -a "$OUT/run.log" ||
		echo "!! $label $case FAILED, continuing" | tee -a "$OUT/run.log"
}

PORT_MINE=6171
PORT_MAIN=6172

for size in $(sizes_for_mode "$MODE"); do
	for case in $CASES; do
		rows=$(rows_for_case_size "$case" "$size")
		[[ -n "$rows" ]] || {
			echo "no rows for $case/$size" >&2
			continue
		}
		# Run main first (baseline), then mine -- same machine, separate ports
		# serialised (each phase has serened restarts).
		run_combo "$MAIN_BIN" main "$case" "$rows" "$PORT_MAIN"
		run_combo "$MINE_BIN" mine "$case" "$rows" "$PORT_MINE"
	done
done

# Summarise
python3 "$(dirname "$0")/summarize.py" "$OUT" >"$OUT/summary.md"
echo "Summary: $OUT/summary.md"
