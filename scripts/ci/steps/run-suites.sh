#!/usr/bin/env bash

set -uo pipefail

: "${WORKSPACE:=$(pwd)}"
: "${BUILD_DIR:=build}"
CONFIG="${CONFIG:?CONFIG is required}"
STEPS="${WORKSPACE}/scripts/ci/steps"

rc=0
run() {
	echo "::group::$*"
	"$@"
	local r=$?
	echo "::endgroup::"
	[[ $r -ne 0 ]] && {
		echo "FAILED ($r): $*" >&2
		rc=$r
	}
	return 0
}

declare -a BG_PIDS=() BG_NAMES=()
run_bg() {
	echo "(parallel start) $*"
	"$@" &
	BG_PIDS+=("$!")
	BG_NAMES+=("$*")
}
wait_bg() {
	local i
	for i in "${!BG_PIDS[@]}"; do
		if ! wait "${BG_PIDS[$i]}"; then
			echo "FAILED: ${BG_NAMES[$i]}" >&2
			rc=1
		fi
	done
	BG_PIDS=()
	BG_NAMES=()
}

run_iresearch_suites() {
	# export (not $GITHUB_ENV) so the run_bg subprocess sees CORPUS_PATH; $GITHUB_ENV only reaches later steps.
	local corpus
	echo "::group::iresearch-load-fetch-corpus.bash"
	if ! corpus="$(bash "${STEPS}/iresearch-load-fetch-corpus.bash")"; then
		echo "FAILED: iresearch-load-fetch-corpus.bash" >&2
		echo "::endgroup::"
		rc=1
		# 046 hard-requires the corpus; 042 would burn a docker run the failed job discards.
		return
	fi
	export CORPUS_PATH="$corpus"
	echo "::endgroup::"
	run_bg bash "${STEPS}/042-ci-in-docker-run-iresearch-tests.bash"
	run_bg bash "${STEPS}/046-ci-in-docker-run-iresearch-load-test.bash"
	wait_bg
}

# gtest + (iresearch) + (unittest/extension) -- need the unit-test binaries, so
# these never run on perf (which doesn't build them).
run_test_suites() {
	run bash "${STEPS}/043-ci-in-docker-run-serenedb-tests.bash"
	if [[ "${RUN_IRESEARCH:-false}" == "true" ]]; then
		run_iresearch_suites
	fi
	if [[ "${RUN_EXTENSION:-false}" == "true" ]]; then
		run bash "${STEPS}/048-ci-in-docker-run-extension-tests.bash"
	fi
}

# The serened-backed smoke that every config runs: sqllogic ours + drivers.
run_serened_core() {
	run env SDB_SQLLOGIC_SCOPE=ours bash "${STEPS}/044-ci-in-docker-run-sqllogic-tests.bash"
	run bash "${STEPS}/047-ci-in-docker-run-driver-tests.bash"
}

# Diff-gated heavy suites: sqlite subtree + sqlsmith fuzzing (+ the slow r driver).
run_sqlite_sqlsmith() {
	if [[ "${RUN_SQLITE:-false}" == "true" ]]; then
		run env SDB_SQLLOGIC_SCOPE=sqlite bash "${STEPS}/044-ci-in-docker-run-sqllogic-tests.bash"
	fi
	if [[ "${RUN_SQLSMITH:-false}" == "true" ]]; then
		# r driver is slow, so it rides the sqlsmith gate instead of the hot driver run.
		run env SDB_DRV_LANG=sqlsmith,r bash "${STEPS}/047-ci-in-docker-run-driver-tests.bash"
	fi
}

case "$CONFIG" in
perf)
	# Optimized build: no unit-test binaries, no fault injection -> no gtest /
	# unittest / iresearch and no recovery. Just the serened smoke + heavy suites.
	run_serened_core
	run_sqlite_sqlsmith
	;;
dev | coverage)
	# Everything, with asserts (coverage also instruments the build).
	run_test_suites
	run_serened_core
	run bash "${STEPS}/045-ci-in-docker-run-recovery-tests.bash"
	run_sqlite_sqlsmith
	;;
asan | tsan | msan | ubsan)
	# Default: ours + drivers only. Recovery is disabled under sanitizers for now
	# (doesn't pass yet -- will join the default soon). RUN_EXTRA widens to the
	# full in-scope set: gtest + iresearch + extension + sqlite + sqlsmith.
	run_serened_core
	if [[ "${RUN_EXTRA:-false}" == "true" ]]; then
		run_test_suites
		run_sqlite_sqlsmith
	fi
	;;
*)
	echo "Unknown CONFIG '$CONFIG'" >&2
	exit 1
	;;
esac

exit $rc
