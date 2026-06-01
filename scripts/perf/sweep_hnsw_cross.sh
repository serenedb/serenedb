#!/usr/bin/env bash
# Cross-product sweep of hnsw.cpp tunables. Iterates chunk_size x
# cache_slots; rg_slots is fixed because the one-knob sweep showed it
# saturates past 8.
#
# Output is one line per config: `chunk=... cache=... rg=...  N ms`.
# Restores the defaults at the end.

set -euo pipefail

REPO=/home/mironov/projects/serenedb/serenedb
HNSW="$REPO/libs/iresearch/include/iresearch/columnstore/hnsw.cpp"
BUILDDIR="$REPO/build_perf"
TEST='tests/sqllogic/sdb/pg/index/vector_search.test_slow'

run_test() {
	local chunk_size="$1"
	local cache_slots="$2"
	local rg_slots="$3"

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
		--server_endpoint pgsql+tcp://0.0.0.0:6161 \
		sleep 4

	local out
	out=$(./tests/sqllogic/run.sh --single-port 6161 --test "$TEST" \
		--database serenedb --engines pg-wire-simple 2>&1 |
		grep -oE 'in [0-9]+ ms' | head -1 | awk '{print $2}')
	printf 'chunk=%5d cache=%4d rg=%2d  %5d ms\n' \
		"$chunk_size" "$cache_slots" "$rg_slots" "$out"
}

CHUNKS=(${CHUNKS:-1024 2048 4096 8192})
CACHES=(${CACHES:-16 32 64 128 256})
RGS=(${RGS:-8})

for chunk in "${CHUNKS[@]}"; do
	for cache in "${CACHES[@]}"; do
		for rg in "${RGS[@]}"; do
			run_test "$chunk" "$cache" "$rg"
		done
	done
done

# Restore defaults.
sed -i \
	-e "s/^constexpr uint64_t kChunkSizeFloats = .*/constexpr uint64_t kChunkSizeFloats = 4 * STANDARD_VECTOR_SIZE;/" \
	-e "s/^constexpr size_t kChunkCacheSlots = .*/constexpr size_t kChunkCacheSlots = 64;/" \
	-e "s/^constexpr size_t kRgCacheSlots = .*/constexpr size_t kRgCacheSlots = 8;/" \
	"$HNSW"
(cd "$BUILDDIR" && ninja serened >/dev/null 2>&1)
