#!/bin/bash

set -euo pipefail

ICEBERG_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../third_party/duckdb_iceberg" && pwd)

if [[ -n "${SDB_SPARK_JAVA_HOME:-}" ]]; then
	export JAVA_HOME="$SDB_SPARK_JAVA_HOME"
fi
if [[ -n "${SDB_DUCKDB_HOME:-}" ]]; then
	export HOME="$SDB_DUCKDB_HOME"
fi
if ! getent passwd "$(id -u)" >/dev/null; then
	passwd=$(mktemp)
	trap 'rm -f "$passwd"' EXIT
	{
		cat /etc/passwd
		echo "serenedb:x:$(id -u):$(id -g)::${HOME:-/tmp}:/bin/sh"
	} >"$passwd"
	export LD_PRELOAD=libnss_wrapper.so NSS_WRAPPER_PASSWD="$passwd" NSS_WRAPPER_GROUP=/etc/group
fi

cd "$ICEBERG_DIR"
rm -rf data/generated
mkdir -p .catalogs
for catalog in "$@"; do
	echo "Generating Iceberg test data for the '$catalog' catalog..."
	echo "$catalog" >.catalogs/.active_catalog
	python3 -m pytest -q -p no:cacheprovider scripts/data_generators/test_generate_data.py
done
