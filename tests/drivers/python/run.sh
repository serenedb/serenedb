#!/bin/bash
# Python driver harness: psycopg3 (D1), psycopg2 + asyncpg (D3).
# Consumes SDB_DRV_* env from tests/drivers/run.sh.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

drivers=(psycopg3 psycopg2 asyncpg)
parallel=(test_psql_mode)
extras=(test_copy test_shell_copy test_pgwire_raw test_search_params test_search_range_params test_dictionary_chains test_es_api test_mcp_api test_otel_api test_otel_startup test_docs_build test_sqlalchemy test_http_session test_http_concurrent_ingest test_http_raw test_http_timeouts test_absl_internal_flags test_commit_aborted_tag test_ddl_command_tags test_failed_checkpoint_marker test_search_wal_corrupt_deletes test_inverted_index_concurrent_writers_1 test_inverted_index_concurrent_writers_2 test_inverted_index_concurrent_writers_3 test_inverted_index_concurrent_writers_4 test_inverted_index_update_visibility test_inverted_index_concurrent_upserts test_inverted_index_crash_replay_1 test_inverted_index_crash_replay_2 test_inverted_index_crash_replay_3 test_search_table_concurrent_writers_1 test_search_table_concurrent_writers_2 test_search_table_crash_replay_1 test_search_table_crash_replay_2 test_missing_database test_startup test_prepared_alter_rebind test_procedure_body_concurrent test_progress test_drop_database_sessions)
checks=(cli_help otel_fixtures otel_schema)

if [[ -n "${SDB_DRV_PY_LIST:-}" ]]; then
	printf '%s\n' "${parallel[@]}" "${drivers[@]/#/test_}" "${extras[@]}" "${checks[@]}"
	exit 0
fi

only="${SDB_DRV_PY_ONLY:-}"
mine() {
	[[ -z "$only" || "$1" == "$only" ]]
}

if ! command -v python3 >/dev/null 2>&1; then
	echo "[python] python3 not found" >&2
	exit 1
fi

if [[ -f "$SCRIPT_DIR/.venv/bin/activate" ]]; then
	# shellcheck disable=SC1091
	. "$SCRIPT_DIR/.venv/bin/activate"
fi

for mod in pytest pytest_asyncio yaml psycopg psycopg2 asyncpg opentelemetry.proto google.protobuf sqlalchemy; do
	if ! python3 -c "import $mod" 2>/dev/null; then
		echo "[python] module $mod is missing; install $SCRIPT_DIR/requirements.txt" >&2
		exit 1
	fi
done

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

# Each driver gets its own pytest invocation so JUnit output stays per-driver.
# Failures in one driver should not prevent the others from being reported.
final=0
if [[ "${SDB_DRV_DEBUG:-false}" == "true" ]]; then
	pytest_args=(-v -s)
else
	pytest_args=(-q)
fi

declare -A parallel_pid=() parallel_log=()
for name in "${parallel[@]}"; do
	test_file="${SCRIPT_DIR}/${name}.py"
	mine "$name" && [[ -f "$test_file" ]] || continue
	parallel_log[$name]="$(mktemp)"
	python3 -m pytest "${pytest_args[@]}" \
		--junitxml="${JUNIT}/tests-drivers-python-${name}-junit.xml" \
		"$test_file" >"${parallel_log[$name]}" 2>&1 &
	parallel_pid[$name]=$!
done

for driver in "${drivers[@]}"; do
	# D1 ships psycopg3 only; psycopg2 and asyncpg are present in D3.
	test_file="${SCRIPT_DIR}/test_${driver}.py"
	mine "test_${driver}" && [[ -f "$test_file" ]] || continue
	echo "[python][$driver] running"
	if ! python3 -m pytest "${pytest_args[@]}" \
		--junitxml="${JUNIT}/tests-drivers-python-${driver}-junit.xml" \
		"$test_file"; then
		final=1
	fi
done

for extra in "${extras[@]}"; do
	test_file="${SCRIPT_DIR}/${extra}.py"
	mine "$extra" && [[ -f "$test_file" ]] || continue
	echo "[python][$extra] running"
	python3 -m pytest "${pytest_args[@]}" \
		--junitxml="${JUNIT}/tests-drivers-python-${extra}-junit.xml" \
		"$test_file"
	rc=$?
	if [[ $rc -ne 0 && $rc -ne 5 ]]; then
		final=1
	fi
done

for name in "${!parallel_pid[@]}"; do
	wait "${parallel_pid[$name]}"
	rc=$?
	echo "[python][$name] running"
	cat "${parallel_log[$name]}"
	rm -f "${parallel_log[$name]}"
	if [[ $rc -ne 0 && $rc -ne 5 ]]; then
		final=1
	fi
done

# CLI help reference: parse `serened --help` and diff against the committed
# fixture (consumed by the docs site). Drift fails the suite. Skips cleanly
# when the binary isn't present. Regenerate with: cli_help.py override.
if mine cli_help; then
	echo "[python][cli_help] check"
	if ! python3 "${SCRIPT_DIR}/cli_help.py" check; then
		final=1
	fi
fi

# OTLP protobuf fixtures: the committed .pb files must match what the
# official opentelemetry-proto bindings produce from the .json sources,
# so the server's decoder is checked against an independent encoder.
# Regenerate with: scripts/otel/fixtures.py generate.
if mine otel_fixtures; then
	echo "[python][otel_fixtures] check"
	if ! python3 "${SCRIPT_DIR}/../../../scripts/otel/fixtures.py" check; then
		final=1
	fi
fi

# The sqllogic include must match the canonical OTel DDL it is generated from.
# Regenerate with: scripts/otel/schema.py generate.
if mine otel_schema; then
	echo "[python][otel_schema] check"
	if ! python3 "${SCRIPT_DIR}/../../../scripts/otel/schema.py" check; then
		final=1
	fi
fi

exit "$final"
