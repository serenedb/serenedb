#!/usr/bin/env bash
# Downloads the ClickHouse "hits" benchmark dataset (~12-15 GB parquet) into
# scripts/perf/data/hits.parquet. Cached: re-run is a no-op once the file is
# present and matches the upstream size.

set -euo pipefail

DATA_DIR="${PERF_DATA_DIR:-${HOME}/data}"
URL="https://datasets.clickhouse.com/hits_compatible/hits.parquet"
OUT="${DATA_DIR}/hits.parquet"

mkdir -p "${DATA_DIR}"

if [[ -f "${OUT}" ]]; then
	remote_size=$(curl -sIL "${URL}" | awk -F': ' 'tolower($1)=="content-length"{print $2+0}' | tail -1)
	local_size=$(stat -c%s "${OUT}")
	if [[ "${remote_size}" -gt 0 && "${remote_size}" -eq "${local_size}" ]]; then
		echo "hits.parquet already present (${local_size} bytes) -- skipping"
		exit 0
	fi
	echo "size mismatch (local=${local_size}, remote=${remote_size}) -- re-downloading"
fi

echo "downloading ${URL} -> ${OUT}"
curl -fL --progress-bar "${URL}" -o "${OUT}.tmp"
mv "${OUT}.tmp" "${OUT}"
echo "done: ${OUT} ($(stat -c%s "${OUT}") bytes)"
