#!/bin/bash
#
# Catalog stress suite: sustained parallel DDL/DML/RBAC churn against a serened
# of its own.
#
# It starts and kills its own server (and, in chaos profiles, crashes it), so it
# cannot run against a shared docker-compose serened the way the driver tests do.
# That is why it is a standalone launcher, like tests/network/run.sh.
#
# Local:  tests/stress/run.sh --profile smoke
# CI:     invoked by scripts/ci/steps/051-ci-in-docker-run-stress-tests.bash
#
set -o pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
: "${WORKSPACE:=$(realpath "${SCRIPT_DIR}/../..")}"
: "${BUILD_DIR:=build}"
: "${SERENED:=${WORKSPACE}/${BUILD_DIR}/bin/serened}"

: "${SDB_STRESS_PROFILE:=smoke}"
: "${SDB_STRESS_SCENARIO:=}"
: "${SDB_STRESS_SECONDS:=}"
: "${SDB_STRESS_WORKERS:=}"
: "${SDB_STRESS_SEED:=}"
: "${SDB_STRESS_OUTDIR:=${WORKSPACE}/out/stress}"
: "${SDB_STRESS_JUNIT:=}"

usage() {
	cat <<-EOF
		Usage: $0 [OPTIONS]

		  --profile NAME     smoke | soak | soak-tsan | wedge-probe (default ${SDB_STRESS_PROFILE})
		  --scenario NAME    ddl_churn | ddl_dml_race | dependency_churn |
		                     serial_churn | tables_only
		  --seconds N        override the profile duration
		  --workers N        override the profile worker count
		  --seed N           fix the RNG seed for a repro
		  --outdir DIR       artifacts (default ${SDB_STRESS_OUTDIR})
		  --junit DIR        also emit tests-stress-junit.xml here

		Every flag has an SDB_STRESS_* environment equivalent.
	EOF
}

while [ $# -gt 0 ]; do
	case "$1" in
	-h | --help)
		usage
		exit 0
		;;
	--profile)
		SDB_STRESS_PROFILE="$2"
		shift 2
		;;
	--scenario)
		SDB_STRESS_SCENARIO="$2"
		shift 2
		;;
	--seconds)
		SDB_STRESS_SECONDS="$2"
		shift 2
		;;
	--workers)
		SDB_STRESS_WORKERS="$2"
		shift 2
		;;
	--seed)
		SDB_STRESS_SEED="$2"
		shift 2
		;;
	--outdir)
		SDB_STRESS_OUTDIR="$2"
		shift 2
		;;
	--junit)
		SDB_STRESS_JUNIT="$2"
		shift 2
		;;
	*)
		echo "unexpected argument: $1" >&2
		usage >&2
		exit 2
		;;
	esac
done

if [[ ! -x "$SERENED" ]]; then
	echo "[stress] serened not found at $SERENED" >&2
	exit 1
fi

PYTHON="${PYTHON:-python3}"
if ! command -v "$PYTHON" >/dev/null 2>&1; then
	echo "[stress] python3 not found; skipping" >&2
	exit 0
fi

# psycopg3 is the only third-party import. Prefer whatever the image already has,
# then the driver-test venv, then skip loudly rather than pip-installing here.
if ! "$PYTHON" -c "import psycopg" >/dev/null 2>&1; then
	VENV="${WORKSPACE}/tests/drivers/python/.venv"
	if [[ -x "${VENV}/bin/python3" ]] && "${VENV}/bin/python3" -c "import psycopg" >/dev/null 2>&1; then
		PYTHON="${VENV}/bin/python3"
	else
		echo "[stress] psycopg not importable and no usable venv at ${VENV}; skipping" >&2
		echo "[stress] provision it with: tests/drivers/python/run.sh (or pip install psycopg[binary])" >&2
		exit 0
	fi
fi

args=(--profile "$SDB_STRESS_PROFILE" --binary "$SERENED" --outdir "$SDB_STRESS_OUTDIR")
[[ -n "$SDB_STRESS_SCENARIO" ]] && args+=(--scenario "$SDB_STRESS_SCENARIO")
[[ -n "$SDB_STRESS_SECONDS" ]] && args+=(--seconds "$SDB_STRESS_SECONDS")
[[ -n "$SDB_STRESS_WORKERS" ]] && args+=(--workers "$SDB_STRESS_WORKERS")
[[ -n "$SDB_STRESS_SEED" ]] && args+=(--seed "$SDB_STRESS_SEED")
[[ -n "$SDB_STRESS_JUNIT" ]] && args+=(--junit "$SDB_STRESS_JUNIT")

echo "[stress] serened=$SERENED profile=$SDB_STRESS_PROFILE python=$PYTHON"
exec "$PYTHON" -u "${SCRIPT_DIR}/main.py" "${args[@]}"
