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

# Like run(), but a failure is reported and NOT folded into rc. For a suite that
# is still earning trust: it must be visible in the log and the artifacts without
# being able to turn the whole nightly red.
run_soft() {
	echo "::group::$* (soft-fail)"
	"$@"
	local r=$?
	echo "::endgroup::"
	[[ $r -ne 0 ]] && echo "SOFT-FAILED ($r): $*" >&2
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

# The iresearch load test (046) is the long pole (~10 min). Unlike 042/043 it
# does not touch the gtest-parallel cache and conflicts with no other suite, so
# start it in the background here and let it overlap everything; the matching
# wait_bg runs after all suites complete. 046 hard-requires the corpus -- on a
# fetch failure we skip it (the job still fails via rc); 042 needs no corpus.
start_iresearch_load_bg() {
	[[ "${RUN_IRESEARCH:-false}" == "true" ]] || return 0
	# export (not $GITHUB_ENV) so the run_bg subprocess sees CORPUS_PATH; $GITHUB_ENV only reaches later steps.
	local corpus
	echo "::group::iresearch-load-fetch-corpus.bash"
	if ! corpus="$(bash "${STEPS}/iresearch-load-fetch-corpus.bash")"; then
		echo "FAILED: iresearch-load-fetch-corpus.bash" >&2
		echo "::endgroup::"
		rc=1
		return
	fi
	export CORPUS_PATH="$corpus"
	echo "::endgroup::"
	run_bg bash "${STEPS}/046-ci-in-docker-run-iresearch-load-test.bash"
}

# gtest + (iresearch) + (duckdb suites) -- need the unit-test binaries, so
# these never run on perf (which doesn't build them). 042 and 043 share the
# gtest-parallel cache, so they stay sequential (foreground); the load test runs
# in the background via start_iresearch_load_bg.
run_test_suites() {
	run bash "${STEPS}/043-ci-in-docker-run-serenedb-tests.bash"
	run bash "${STEPS}/049-ci-in-docker-run-network-tests.bash"
	if [[ "${RUN_IRESEARCH:-false}" == "true" ]]; then
		run bash "${STEPS}/042-ci-in-docker-run-iresearch-tests.bash"
	fi
	if [[ -n "${DUCKDB_SUITES:-}" ]]; then
		run bash "${STEPS}/048-ci-in-docker-run-duckdb-tests.bash"
	fi
}

# The serened-backed smoke that every config runs: sqllogic + drivers.
#
# One sqllogic invocation covers both scopes: `all` is ours + the sqlite subtree
# in a single runner run. Splitting it in two cost a second serened stack
# bring-up and, worse, silently lost results -- both scopes emit the same
# tests-serenedb-*-junit.xml names, so the sqlite run overwrote the ours run's
# report in the uploaded artifact.
run_serened_core() {
	local scope=ours
	[[ "${RUN_SQLITE:-false}" == "true" ]] && scope=all
	run env SDB_SQLLOGIC_SCOPE="$scope" bash "${STEPS}/044-ci-in-docker-run-sqllogic-tests.bash"
	run bash "${STEPS}/047-ci-in-docker-run-driver-tests.bash"
}

# Diff-gated heavy suite: sqlsmith fuzzing.
run_sqlsmith() {
	if [[ "${RUN_SQLSMITH:-false}" == "true" ]]; then
		run env SDB_DRV_LANG=sqlsmith bash "${STEPS}/047-ci-in-docker-run-driver-tests.bash"
	fi
}

# Diff-gated heavy suite: catalog stress. Runs last and against a serened of its
# own, because the failure it looks for wedges the whole process -- the same
# reason the network tests are a standalone launcher rather than a --host client.
run_stress() {
	[[ "${RUN_STRESS:-false}" == "true" ]] || return 0
	if [[ "${STRESS_SOFT_FAIL:-true}" == "true" ]]; then
		run_soft bash "${STEPS}/051-ci-in-docker-run-stress-tests.bash"
	else
		run bash "${STEPS}/051-ci-in-docker-run-stress-tests.bash"
	fi
}

# Sanitizer configs run ours + drivers by default; RUN_EXTRA is what widens them
# to the full in-scope set. Fold that into the diff gates here so the bodies
# below only ever read RUN_* -- in particular run_serened_core needs RUN_SQLITE
# to already be false, or the default sanitizer run would pick up the sqlite
# subtree through the merged scope.
case "$CONFIG" in
asan | tsan | msan | ubsan)
	if [[ "${RUN_EXTRA:-false}" != "true" ]]; then
		RUN_IRESEARCH=false
		DUCKDB_SUITES=""
		RUN_SQLITE=false
		RUN_SQLSMITH=false
	fi
	;;
esac

case "$CONFIG" in
perf)
	# Optimized build: no unit-test binaries, no fault injection -> no gtest /
	# unittest / iresearch and no recovery. Just the serened smoke + heavy suites.
	run_serened_core
	run_sqlsmith
	;;
dev | coverage)
	# Everything, with asserts (coverage also instruments the build).
	start_iresearch_load_bg
	run_test_suites
	run_serened_core
	run bash "${STEPS}/045-ci-in-docker-run-recovery-tests.bash"
	run_sqlsmith
	;;
asan | tsan | msan | ubsan)
	# Default: ours + drivers only. Recovery is disabled under sanitizers for now
	# (doesn't pass yet -- will join the default soon). RUN_EXTRA widens to the
	# full in-scope set; the normalization above already forced every RUN_* gate
	# off when it isn't set, so the merged sqllogic scope stays `ours` too.
	run_serened_core
	if [[ "${RUN_EXTRA:-false}" == "true" ]]; then
		start_iresearch_load_bg
		run_test_suites
	fi
	run_sqlsmith
	;;
*)
	echo "Unknown CONFIG '$CONFIG'" >&2
	exit 1
	;;
esac

# Join the backgrounded iresearch load test (no-op when none was started).
wait_bg

# After wait_bg on purpose: the stress suite saturates the box and looks for a
# wedge, so it must not share the machine with the iresearch load test or read
# its contention as a hang.
run_stress

exit $rc
