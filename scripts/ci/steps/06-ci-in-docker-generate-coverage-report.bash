#!/bin/bash
set -euo pipefail

# Generate coverage reports inside Docker container
# Outputs: HTML report + LCOV file

# BUILD_DIRS_STR: space-separated list of build directories
# Example: "build build2" or just "build"
# Default to "build" if not set
BUILD_DIRS_STR="${BUILD_DIRS_STR:-build}"

CONTAINER_SCRIPT='
set -o pipefail
cd /serenedb

# Parse BUILD_DIRS_STR (space-separated) into array
read -ra BUILD_DIRS <<< "${BUILD_DIRS_STR}"

echo ":: Processing build directories: ${BUILD_DIRS[*]}"

# Collect all binaries
ALL_BINARIES=()
for dir in "${BUILD_DIRS[@]}"; do
    if [ -d "${dir}/bin" ]; then
        while IFS= read -r -d "" bin; do
            ALL_BINARIES+=("${bin}")
        done < <(find "${dir}/bin" -type f -executable -print0 2>/dev/null)
    else
        echo "WARNING: Directory ${dir}/bin not found, skipping..."
    fi
done

if [ ${#ALL_BINARIES[@]} -eq 0 ]; then
    echo "ERROR: No binaries found in any build directory!"
    exit 1
fi

echo ":: Found ${#ALL_BINARIES[@]} binaries:"
printf "   %s\n" "${ALL_BINARIES[@]}"

PROFILE_COUNT=$(find ./coverage/profiles -name "*.profraw" 2>/dev/null | wc -l)
echo ":: Found ${PROFILE_COUNT} profile files in ./coverage/profiles"

if [ "${PROFILE_COUNT}" -eq 0 ]; then
    echo "ERROR: No profile data found!"
    exit 1
fi

# Generate HTML report
echo ":: Generating HTML coverage report..."
./scripts/prepare_coverage.py \
    --unified-report \
    --compilation-dir . \
    llvm-profdata-21 \
    llvm-cov-21 \
    ./coverage/profiles \
    ./coverage/llvm_html \
    "${ALL_BINARIES[@]}"

echo ":: Coverage reports generated successfully!"
echo "   HTML: ./coverage/llvm_html"
'

echo "=========================================="
echo "Coverage Generation"
echo "=========================================="
echo "Build directories: ${BUILD_DIRS_STR}"
echo "=========================================="

if docker run --rm \
	--user "$(id -u):$(id -g)" \
	-e HOME=/serenedb \
	--ulimit core=-1 \
	--ulimit nofile=16384:16384 \
	--cap-add=SYS_PTRACE \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-e "BUILD_DIRS_STR=${BUILD_DIRS_STR}" \
	-v "${WORKSPACE}:/serenedb" \
	"${BUILD_IMAGE}" \
	bash -c "${CONTAINER_SCRIPT}"; then
	echo "GENERATE_COVERAGE=PASSED"
else
	echo "GENERATE_COVERAGE=FAILED"
	exit 123
fi
