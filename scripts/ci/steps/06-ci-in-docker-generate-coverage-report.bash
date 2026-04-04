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

# Merge all profile data into combined directory
mkdir -p ./combined_coverage/profiles
for dir in "${BUILD_DIRS[@]}"; do
    if [ -d "${dir}/coverage/profiles" ]; then
        echo ":: Copying profiles from ${dir}/coverage/profiles"
        find "${dir}/coverage/profiles" -name "*.profraw" -exec cp {} ./combined_coverage/profiles/ \; 2>/dev/null || true
    else
        echo "WARNING: Profile directory ${dir}/coverage/profiles not found, skipping..."
    fi
done

# Count collected profiles
PROFILE_COUNT=$(find ./combined_coverage/profiles -name "*.profraw" 2>/dev/null | wc -l)
echo ":: Collected ${PROFILE_COUNT} profile files"

if [ "${PROFILE_COUNT}" -eq 0 ]; then
    echo "ERROR: No profile data found!"
    exit 1
fi

# Build object arguments for llvm-cov
OBJECT_ARGS="${ALL_BINARIES[0]}"
for ((i=1; i<${#ALL_BINARIES[@]}; i++)); do
    OBJECT_ARGS="${OBJECT_ARGS} -object ${ALL_BINARIES[$i]}"
done

# Generate HTML report
echo ":: Generating HTML coverage report..."
./scripts/prepare-code-coverage-artifact.py \
    --unified-report \
    --compilation-dir . \
    llvm-profdata-21 \
    llvm-cov-21 \
    ./combined_coverage/profiles \
    ./combined_coverage/llvm_html \
    "${ALL_BINARIES[@]}"

echo ":: Coverage reports generated successfully!"
echo "   HTML: ./combined_coverage/llvm_html"
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
