#!/bin/bash

BUILD_IMAGE=$(python3 "$(dirname "${BASH_SOURCE[0]}")/ci.py" image)
export BUILD_IMAGE

sdb_test_targets() {
	local suites=()
	while (($#)); do
		[[ "$1" == --suite ]] && suites+=("$2") && shift
		shift
	done
	((${#suites[@]})) || suites=(gtest iresearch iresearch-load duckdb re2)
	local suite
	for suite in "${suites[@]}"; do
		case "$suite" in
		gtest) echo serenedb-tests ;;
		iresearch) echo iresearch-tests ;;
		iresearch-load) echo iresearch-load-tests ;;
		duckdb) echo unittest ;;
		re2) echo re2-tests ;;
		esac
	done
}

sdb_config() {
	CONFIG="$1"
	shift
	BUILDMODE=RelWithDebInfo
	SDB_DEBUG_INFO=STACK
	SDB_LTO=Off
	SDB_VTUNE=Off
	SDB_ALLOC=JE
	SDB_COVERAGE=Off
	SANITIZERS=None
	SDB_DEV=On
	SDB_GTEST=On
	SDB_FAULT_INJECTION=On
	EXTRA_CMAKE_FLAGS="-DSDB_BUILD_DUCKDB_UNITTESTS=ON"
	TARGETS=(serened)
	case "$CONFIG" in
	perf)
		SDB_DEV=Off
		SDB_GTEST=Off
		SDB_FAULT_INJECTION=Off
		EXTRA_CMAKE_FLAGS="-DSDB_BUILD_DUCKDB_UNITTESTS=OFF"
		;;
	dev) SDB_COVERAGE=On ;;
	asan) SANITIZERS=UndefinedAddress ;;
	tsan) SANITIZERS=Thread ;;
	msan) SANITIZERS=MemoryWithOrigins ;;
	*)
		echo "unknown config '$CONFIG'" >&2
		return 1
		;;
	esac
	if [[ "$CONFIG" == perf || "$CONFIG" == dev ]]; then
		TARGETS+=(iresearch-examples serenedb-bench-micro search-benchmark-game)
	fi
	if [[ "$SDB_GTEST" == On ]]; then
		mapfile -t -O "${#TARGETS[@]}" TARGETS < <(sdb_test_targets "$@" | sort -u)
	fi
	BUILD_DIR="build_${CONFIG}"
	ARCH="$(uname -m)"
	BUILD_FLAGS="BUILDMODE=$BUILDMODE SDB_DEBUG_INFO=$SDB_DEBUG_INFO SDB_LTO=$SDB_LTO SDB_VTUNE=$SDB_VTUNE SDB_ALLOC=$SDB_ALLOC"
	BUILD_FLAGS+=" SDB_COVERAGE=$SDB_COVERAGE SANITIZERS=$SANITIZERS SDB_DEV=$SDB_DEV SDB_GTEST=$SDB_GTEST"
	BUILD_FLAGS+=" SDB_FAULT_INJECTION=$SDB_FAULT_INJECTION EXTRA=$EXTRA_CMAKE_FLAGS TARGETS=${TARGETS[*]}"
	export CONFIG BUILDMODE SDB_DEBUG_INFO SDB_LTO SDB_VTUNE SDB_ALLOC SDB_COVERAGE SANITIZERS SDB_DEV
	export SDB_GTEST SDB_FAULT_INJECTION EXTRA_CMAKE_FLAGS BUILD_DIR ARCH BUILD_FLAGS
}
