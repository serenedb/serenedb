#!/bin/bash
set -euo pipefail

# Generate coverage reports inside Docker container
# Outputs: HTML report + LCOV file

# BUILD_DIR: build directory to collect binaries and profiles from
# Default to "build" if not set
BUILD_DIR="${BUILD_DIR:-build}"

CONTAINER_SCRIPT='
set -o pipefail
cd /serenedb

echo ":: Build directory: ${BUILD_DIR}"

# Collect all binaries
ALL_BINARIES=()
if [ -d "${BUILD_DIR}/bin" ]; then
    while IFS= read -r -d "" bin; do
        ALL_BINARIES+=("${bin}")
    done < <(find "${BUILD_DIR}/bin" -type f -executable -print0 2>/dev/null)
else
    echo "ERROR: Directory ${BUILD_DIR}/bin not found!"
    exit 1
fi

if [ ${#ALL_BINARIES[@]} -eq 0 ]; then
    echo "ERROR: No binaries found in ${BUILD_DIR}/bin!"
    exit 1
fi

echo ":: Found ${#ALL_BINARIES[@]} binaries:"
printf "   %s\n" "${ALL_BINARIES[@]}"

PROFILE_COUNT=$(find ./out/coverage/profiles -name "*.profraw" 2>/dev/null | wc -l)
echo ":: Found ${PROFILE_COUNT} profile files in ./out/coverage/profiles"

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
    ./out/coverage/profiles \
    ./out/coverage/llvm_html \
    "${ALL_BINARIES[@]}"

echo ":: Coverage reports generated successfully!"
echo "   HTML: ./out/coverage/llvm_html"
'

echo "=========================================="
echo "Coverage Generation"
echo "=========================================="
echo "Build directory: ${BUILD_DIR}"
echo "=========================================="

if docker run --rm \
	--user "$(id -u):$(id -g)" \
	-e HOME=/serenedb \
	--ulimit core=-1 \
	--ulimit nofile=16384:16384 \
	--cap-add=SYS_PTRACE \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-e "BUILD_DIR=${BUILD_DIR}" \
	-v "${WORKSPACE}:/serenedb" \
	"${BUILD_IMAGE}" \
	bash -c "${CONTAINER_SCRIPT}"; then
	echo "GENERATE_COVERAGE=PASSED"
else
	echo "GENERATE_COVERAGE=FAILED"
	exit 123
fi
