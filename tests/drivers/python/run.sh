#!/bin/bash
# Python driver harness: psycopg3 (D1), psycopg2 + asyncpg (D3).
# Consumes SDB_DRV_* env from tests/drivers/run.sh.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if ! command -v python3 >/dev/null 2>&1; then
	echo "[python] python3 not found" >&2
	exit 1
fi

# Provision deps. Prefer the build image's pre-installed system packages.
# If anything is missing, fall back to:
#   1. a venv (if python3-venv is available), or
#   2. a system-wide pip install with --break-system-packages (last resort).
need_install=0
for mod in pytest pytest_asyncio yaml psycopg psycopg2 asyncpg; do
	if ! python3 -c "import $mod" 2>/dev/null; then
		need_install=1
		break
	fi
done

if [[ $need_install -eq 1 ]]; then
	VENV="${SCRIPT_DIR}/.venv"
	if [[ ! -d "$VENV" ]] && python3 -m venv --help >/dev/null 2>&1; then
		python3 -m venv --system-site-packages "$VENV" 2>/dev/null || true
	fi
	if [[ -f "$VENV/bin/activate" ]]; then
		# shellcheck disable=SC1091
		. "$VENV/bin/activate"
	fi
	if [[ -d "$VENV" && ! -f "$VENV/.deps-installed" ]]; then
		python3 -m pip install --quiet --upgrade pip 2>/dev/null || true
		if python3 -m pip install --quiet -r "$SCRIPT_DIR/requirements.txt"; then
			touch "$VENV/.deps-installed"
		fi
	fi
	# Final fallback: install system-wide. The build image runs as root with
	# a throwaway filesystem, so --break-system-packages is fine for CI.
	if ! python3 -c "import pytest" 2>/dev/null; then
		python3 -m pip install --quiet --break-system-packages \
			-r "$SCRIPT_DIR/requirements.txt"
	fi
fi

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

# Each driver gets its own pytest invocation so JUnit output stays per-driver.
# Failures in one driver should not prevent the others from being reported.
final=0
for driver in psycopg3 psycopg2 asyncpg; do
	# D1 ships psycopg3 only; psycopg2 and asyncpg are present in D3.
	test_file="${SCRIPT_DIR}/test_${driver}.py"
	[[ -f "$test_file" ]] || continue
	echo "[python][$driver] running"
	if [[ "${SDB_DRV_DEBUG:-false}" == "true" ]]; then
		pytest_args=(-v -s)
	else
		pytest_args=(-q)
	fi
	if ! python3 -m pytest "${pytest_args[@]}" \
		--junitxml="${JUNIT}/tests-drivers-python-${driver}-junit.xml" \
		"$test_file"; then
		final=1
	fi
done

if [[ "${SDB_DRV_DEBUG:-false}" == "true" ]]; then
	pytest_args=(-v -s)
else
	pytest_args=(-q)
fi
for extra in test_copy test_shell_copy test_psql_mode; do
	test_file="${SCRIPT_DIR}/${extra}.py"
	[[ -f "$test_file" ]] || continue
	echo "[python][$extra] running"
	if ! python3 -m pytest "${pytest_args[@]}" \
		--junitxml="${JUNIT}/tests-drivers-python-${extra}-junit.xml" \
		"$test_file"; then
		final=1
	fi
done

# CLI help reference: parse `serened --help` and diff against the committed
# fixture (consumed by the docs site). Drift fails the suite. Skips cleanly
# when the binary isn't present. Regenerate with: cli_help.py override.
echo "[python][cli_help] check"
if ! python3 "${SCRIPT_DIR}/cli_help.py" check; then
	final=1
fi

exit "$final"
