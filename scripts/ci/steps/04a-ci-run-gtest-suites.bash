#!/bin/bash
# Runs gtest suites (vpack, iresearch, serenedb-tests) in parallel.
# Expects: BUILD_DIR, WORKSPACE, BUILD_IMAGE, GTEST_PARALLEL_CACHE_DIR

set -e

if [[ -n "$SANITIZERS" ]]; then
	mkdir -p "$WORKSPACE/sanitizers"
fi

SCRIPTS=(
	"BUILD_DIR=${BUILD_DIR:-build_gtest} ./scripts/ci/steps/041-ci-in-docker-run-vpack-tests.bash"
	"BUILD_DIR=${BUILD_DIR:-build_gtest} ./scripts/ci/steps/042-ci-in-docker-run-iresearch-tests.bash"
	"BUILD_DIR=${BUILD_DIR:-build_gtest} ./scripts/ci/steps/043-ci-in-docker-run-serenedb-tests.bash"
)

JOBLOG=$(mktemp)
PARALLEL_RC=0

echo "Running ${#SCRIPTS[@]} gtest suite(s) in parallel:"
printf '%s\n' "${SCRIPTS[@]}"

parallel --jobs 4 --tagstring '{/.}' --line-buffer --joblog "$JOBLOG" --halt now,fail=1 \
	'bash -c {}' \
	::: "${SCRIPTS[@]}" || PARALLEL_RC=$?

# Print summary
echo ""
echo "========================================"
echo "  GTEST RESULTS SUMMARY"
echo "========================================"
if [[ -s "$JOBLOG" ]]; then
	while IFS=$'\t' read -r seq host starttime jobruntime send receive exitval signal command; do
		[[ "$seq" == "Seq" ]] && continue
		name=$(basename "$command" .bash | sed 's/^[0-9]*-ci-in-docker-run-//')
		if [[ "$exitval" -eq 0 ]]; then
			echo "  PASSED  ${name} (${jobruntime}s)"
		elif [[ "$signal" -ne 0 ]]; then
			echo "  KILLED  ${name} (signal ${signal})"
		else
			echo "  FAILED  ${name} (${jobruntime}s, exit code ${exitval})"
		fi
	done <"$JOBLOG"
fi
rm -f "$JOBLOG"

if [[ $PARALLEL_RC -ne 0 ]]; then
	echo "========================================"
	echo "  SOME GTEST SUITES FAILED"
	echo "========================================"
	exit 1
fi
echo "========================================"
echo "  ALL GTEST SUITES PASSED"
echo "========================================"
