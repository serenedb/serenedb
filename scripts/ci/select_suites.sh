#!/bin/bash

set -euo pipefail

args=()
if [[ "${CORPUS:-false}" == true ]]; then
	args+=(--corpus)
fi
if [[ "${EVERYTHING:-false}" == true ]]; then
	echo "${args[*]}"
	exit 0
fi

args+=(--suite sqllogic --suite recovery --suite gtest --suite drivers --suite stress)
if [[ "${IRESEARCH:-false}" == true ]]; then
	args+=(--suite iresearch --suite iresearch-load)
fi
if [[ "${RE2:-false}" == true ]]; then
	args+=(--suite re2)
fi
if [[ -n "${DUCKDB_SUITES:-}" ]]; then
	args+=(--suite duckdb)
	IFS=, read -ra suites <<<"$DUCKDB_SUITES"
	for s in "${suites[@]}"; do
		args+=(--duckdb-suite "$s")
	done
fi
if [[ ",${DUCKDB_SUITES:-}," == *,core,* ]]; then
	args+=(--suite sqlsmith)
fi
echo "${args[*]}"
