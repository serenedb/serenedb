#!/bin/bash
set -euo pipefail

# Generate coverage reports inside Docker container

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
        if llvm-readelf-23 -S "${bin}" 2>/dev/null | grep -q __llvm_covmap; then
            ALL_BINARIES+=("${bin}")
        fi
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

echo ":: Merging profiles..."
llvm-profdata-23 merge -sparse -failure-mode=any \
    -o ./out/coverage/coverage.profdata ./out/coverage/profiles/*.profraw || exit 1

OBJECTS=()
for bin in "${ALL_BINARIES[@]:1}"; do
    OBJECTS+=(-object "${bin}")
done
llvm-cov-23 report -instr-profile=./out/coverage/coverage.profdata \
    "${ALL_BINARIES[0]}" "${OBJECTS[@]}" >./out/coverage/summary.txt || exit 1
tail -n 1 ./out/coverage/summary.txt
'

echo "=========================================="
echo "Coverage Generation"
echo "=========================================="
echo "Build directory: ${BUILD_DIR}"
echo "=========================================="

if docker run --rm \
	${SDB_DOCKER_OWNER:+--label "serenedb.owner=$SDB_DOCKER_OWNER"} \
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
