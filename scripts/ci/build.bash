#!/bin/bash
set -eo pipefail

print_banner() {
	local title="$1"
	local targets="$2"

	echo ""
	echo "╔════════════════════════════════════════════════════════╗"
	echo "║  ____  _____ ____  _____ _   _ _____ ____  ____        ║"
	echo "║ / ___|| ____|  _ \| ____| \\ | | ____|  _ \\| __ )       ║"
	echo "║ \\___ \\|  _| | |_) |  _| |  \\| |  _| | | | |  _ \\       ║"
	echo "║  ___) | |___|  _ <| |___| |\\  | |___| |_| | |_) |      ║"
	echo "║ |____/|_____|_| \\_\\_____|_| \\_|_____|____/|____/       ║"
	echo "║                                                        ║"
	echo "╠════════════════════════════════════════════════════════╣"
	printf "║  %-54s║\n" "$(date '+%Y-%m-%d %H:%M:%S')"
	echo "╚════════════════════════════════════════════════════════╝"
	echo ">> $title"
	echo "Targets: $targets"
	echo ""
}

if [ "$#" -eq 0 ]; then
	echo "Usage: $0 <target1> [target2 ...]"
	exit 1
fi

TARGETS=("$@")
LOG_SUFFIX="${TARGETS[*]}"
LOG_SUFFIX="${LOG_SUFFIX// /_}"

: "${BUILD_DIR:=build}"
: "${SDB_DEV:=On}"
: "${USE_IPO:=Off}"
: "${SDB_GTEST:=Off}"
: "${ENSURE_VTUNE_SYMBOLS:=Off}"
: "${SDB_ALLOC:=JE}"
: "${SANITIZERS:=None}"
: "${STATIC_EXECUTABLES:=On}"
: "${SDB_FAULT_INJECTION:=On}"

cd /serenedb
git config --global --add safe.directory '*'

mkdir -p $BUILD_DIR
cd $BUILD_DIR

CMAKE_FLAGS=(
	"-GNinja"
	"-DCMAKE_BUILD_TYPE=$BUILDMODE"
	"-DUSE_DEBUG_INFO=$USE_DEBUG_INFO"
	"-DCMAKE_C_COMPILER=/usr/local/bin/clang"
	"-DCMAKE_CXX_COMPILER=/usr/local/bin/clang++"
	"-DCMAKE_EXE_LINKER_FLAGS=-fuse-ld=lld"
	"-DCMAKE_SHARED_LINKER_FLAGS=-fuse-ld=lld"
	"-DAUTO_UPDATE_MODULES=Off"
	"-DSDB_DEV=$SDB_DEV"
	"-DSDB_CLUSTER=$SDB_CLUSTER"
	"-DSDB_FAULT_INJECTION=$SDB_FAULT_INJECTION"
	"-DSDB_GTEST=$SDB_GTEST"
	"-DUSE_IPO=$USE_IPO"
	"-DUSE_COVERAGE=$USE_COVERAGE"
	"-DENSURE_VTUNE_SYMBOLS=$ENSURE_VTUNE_SYMBOLS"
)

if [[ -n "${SERENEDB_VERSION_PATCH:-}" && "${SERENEDB_VERSION_PATCH}" != "0" ]]; then
	CMAKE_FLAGS+=("-DSERENEDB_VERSION_PATCH=${SERENEDB_VERSION_PATCH}")
fi

if [[ "$SANITIZERS" == "None" || -z "$SANITIZERS" ]]; then
	CMAKE_FLAGS+=("-DSTATIC_EXECUTABLES=$STATIC_EXECUTABLES" "-DSDB_ALLOC=$SDB_ALLOC")
else
	CMAKE_FLAGS+=("-DSTATIC_EXECUTABLES=Off" "-DSDB_ALLOC=SYS" "-DSDB_IOURING=Off" "-DSDB_SANITIZE=$SANITIZERS")
fi

JEMALLOC_TARGET=""
if [[ "$SDB_ALLOC" == "JE" && ("$SANITIZERS" == "None" || -z "$SANITIZERS") ]]; then
	JEMALLOC_TARGET="jemalloc_build"
fi

print_banner "CMAKE CONFIGURATION" "${TARGETS[*]}"
echo "nproc=$(nproc) cmake ${CMAKE_FLAGS[*]} .." | tee /serenedb/cmake_${LOG_SUFFIX}.log
cmake "${CMAKE_FLAGS[@]}" .. 2>&1 | tee -a /serenedb/cmake_${LOG_SUFFIX}.log || exit 1

export CC=/usr/local/bin/clang
export CXX=/usr/local/bin/clang++

if [[ -n "$JEMALLOC_TARGET" ]]; then
	print_banner "BUILDING JEMALLOC" "$JEMALLOC_TARGET"
	ninja "$JEMALLOC_TARGET" 2>&1 | tee /serenedb/make_${LOG_SUFFIX}.log || exit 1
	ccache -s | tee /serenedb/ccache_${LOG_SUFFIX}.log
fi

print_banner "BUILDING TARGETS" "${TARGETS[*]}"
ninja "${TARGETS[@]}" 2>&1 | tee -a /serenedb/make_${LOG_SUFFIX}.log || exit 1
ccache -s | tee -a /serenedb/ccache_${LOG_SUFFIX}.log
