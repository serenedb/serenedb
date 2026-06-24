#!/usr/bin/env bash
# One-knob sweep over hnsw.cpp tunables (chunk size, chunk-cache slots,
# row-group cache slots). Runs vector_search.test_slow under a fresh
# serened for each config; prints elapsed ms per config.
#
# Requires:
#   - a built RelWithDebInfo binary at ./build_perf/bin/serened
#   - paranoid serened on port 6161 idle (we'll restart it)
#   - sqllogic test runner at ./tests/sqllogic/run.sh
#
# Restores the defaults in hnsw.cpp at the end.

set -euo pipefail

REPO=/home/mironov/projects/serenedb/serenedb
HNSW="$REPO/libs/iresearch/include/iresearch/columnstore/hnsw.cpp"
BUILDDIR="$REPO/build_perf"
TEST='tests/sqllogic/sdb/pg/index/vector_search.test_slow'

run_test() {
	local label="$1"
	local chunk_size="$2"
	local cache_slots="$3"
	local rg_slots="$4"

	sed -i \
		-e "s/^constexpr uint64_t kChunkSizeFloats = .*/constexpr uint64_t kChunkSizeFloats = ${chunk_size};/" \
		-e "s/^constexpr size_t kChunkCacheSlots = .*/constexpr size_t kChunkCacheSlots = ${cache_slots};/" \
		-e "s/^constexpr size_t kRgCacheSlots = .*/constexpr size_t kRgCacheSlots = ${rg_slots};/" \
		"$HNSW"
	(cd "$BUILDDIR" && ninja serened >/dev/null 2>&1)

	pgrep -af 'serened' | grep -v claude | grep -v clangd |
		awk '{print $1}' | xargs -r kill 2>/dev/null || true
	sleep 1
	rm -rf /tmp/serened_perf_6161
	cd "$REPO"
	./build_perf/bin/serened /tmp/serened_perf_6161 \
		--listen postgres://0.0.0.0:6161 \
		sleep 4

	local out
	out=$(./tests/sqllogic/run.sh --single-port 6161 --test "$TEST" \
		--database serenedb --engines pg-wire-simple 2>&1 |
		grep -oE 'in [0-9]+ ms' | head -1)
	printf '%-20s chunk=%5d cache=%4d rg=%2d  %s\n' \
		"$label" "$chunk_size" "$cache_slots" "$rg_slots" "$out"
}

run_test "baseline" 8192 64 8

# Vary chunk-cache slots.
run_test "cache=16" 8192 16 8
run_test "cache=32" 8192 32 8
run_test "cache=128" 8192 128 8
run_test "cache=256" 8192 256 8

# Vary batch (chunk_size_floats).
run_test "batch=1024" 1024 64 8
run_test "batch=2048" 2048 64 8
run_test "batch=4096" 4096 64 8

# Vary row-group cache slots.
run_test "rg=4" 8192 64 4
run_test "rg=16" 8192 64 16
run_test "rg=32" 8192 64 32

# Restore defaults.
sed -i \
	-e "s/^constexpr uint64_t kChunkSizeFloats = .*/constexpr uint64_t kChunkSizeFloats = 4 * STANDARD_VECTOR_SIZE;/" \
	-e "s/^constexpr size_t kChunkCacheSlots = .*/constexpr size_t kChunkCacheSlots = 64;/" \
	-e "s/^constexpr size_t kRgCacheSlots = .*/constexpr size_t kRgCacheSlots = 8;/" \
	"$HNSW"
(cd "$BUILDDIR" && ninja serened >/dev/null 2>&1)
