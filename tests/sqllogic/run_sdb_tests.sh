#!/bin/bash

# Runs SereneDB sqllogic tests.
# Usage:
#   ./run_sdb_tests.sh --host localhost --single-port 7777
#   SKIP_SLOW_TESTS=true ./run_sdb_tests.sh --host localhost --single-port 7777

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

FAST_FLAG=""
if [[ "${SKIP_SLOW_TESTS:-false}" == "true" ]]; then
	FAST_FLAG="--fast"
fi

# sqlite subtree is under sdb/pg/any via the any/pg symlink; --skip is a path regex.
declare -a SEL
case "${SDB_SQLLOGIC_SCOPE:-all}" in
ours) SEL=(--test "sdb/**/*.test*" --skip "sqlite") ;;
sqlite) SEL=(--test "sdb/pg/any/sqlite/**/*.test*") ;;
all | *) SEL=(--test "sdb/**/*.test*") ;;
esac

exec ./run.sh \
	"${SEL[@]}" \
	--junit "tests-serenedb" \
	--engines "pg-wire-simple,pg-wire-extended" \
	--database serenedb \
	$FAST_FLAG \
	"$@"
