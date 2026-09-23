#!/bin/bash

# Catalog stress suite: sustained parallel DDL/DML churn with a consistency
# oracle and a hang detector.
#
# The suite starts, kills and (in chaos profiles) crashes a serened of its own,
# so it runs in-container like the network tests rather than against the shared
# sqllogic stack: the failure it looks for wedges the whole process, and a
# regression here must not take every other suite down with it.
#
# /dev/shm is sized explicitly because every datadir lives there, and
# SYS_PTRACE plus an unconfined seccomp profile keep /proc thread sampling and
# core dumps available for wedge triage.
STRESS_TSAN_OPTIONS="${TSAN_OPTIONS:-}"
PROFILE="${SDB_STRESS_PROFILE:-smoke}"
BACKEND="${ICEBERG_BACKEND:-local}"
OUT="out/stress/${PROFILE}"
JUNIT_OUT="out/test-results"
if [[ "${BACKEND}" != "local" ]]; then
	OUT="${OUT}-${BACKEND}"
	JUNIT_OUT="${JUNIT_OUT}/${BACKEND}"
fi

# The workload profiles replay the customer's iceberg flow against an
# iceberg-rest + MinIO pair. The stress container has no docker socket, so the
# pair starts on the host and the container joins the host network to reach it.
FIXTURE_STATE=""
FIXTURE_ARGS=()
if [[ "${PROFILE}" == biglake-reindex-* && "${BACKEND}" == "local" ]]; then
	FIXTURE_STATE="$(mktemp)"
	eval "$(python3 "${WORKSPACE}/tests/drivers/harness/iceberg_rest.py" start --state "${FIXTURE_STATE}")"
	trap 'python3 "${WORKSPACE}/tests/drivers/harness/iceberg_rest.py" stop --state "${FIXTURE_STATE}"; rm -f "${FIXTURE_STATE}"' EXIT
	FIXTURE_ARGS=(--network host
		-e MINIO_HOST -e MINIO_PORT -e MINIO_ACCESS_KEY -e MINIO_SECRET_KEY -e MINIO_BUCKET
		-e ICEBERG_REST_URL -e ICEBERG_WAREHOUSE)
fi

if ! docker run --rm \
	--user "$(id -u):$(id -g)" \
	-e HOME=/serenedb \
	--ulimit core=-1 \
	--ulimit nofile=16384:16384 \
	--shm-size=2g \
	--cap-add=SYS_PTRACE \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-e BUILD_DIR="${BUILD_DIR}" \
	-e TSAN_OPTIONS="${STRESS_TSAN_OPTIONS}" \
	-e SDB_STRESS_PROFILE="${PROFILE}" \
	-e SDB_STRESS_OUTDIR="/serenedb/${OUT}" \
	-e SDB_STRESS_JUNIT="/serenedb/${JUNIT_OUT}" \
	-e ICEBERG_BACKEND -e BIGLAKE_PROJECT -e BIGLAKE_CATALOG \
	-e BIGLAKE_CLIENT_EMAIL -e BIGLAKE_PRIVATE_KEY -e BIGLAKE_PRIVATE_KEY_ID \
	"${FIXTURE_ARGS[@]}" \
	-v "${WORKSPACE}:/serenedb" \
	"${BUILD_IMAGE}" \
	bash -c '
    set -o pipefail
    cd /serenedb
    mkdir -p /serenedb/out/logs "${SDB_STRESS_JUNIT}"
    WORKSPACE=/serenedb BUILD_DIR="${BUILD_DIR}" \
      ./tests/drivers/stress/run.sh 2>&1 | tee -a /serenedb/out/logs/stress-tests.log
  '; then
	echo "STRESS_TESTS=FAILED"
	# The summary carries the seeded repro command and the findings; the thread
	# samples carry the wedge verdict. Both are in the uploaded artifacts, but
	# echo the summary so a failure is readable from the job log alone.
	if [[ -f "${WORKSPACE}/${OUT}/summary.txt" ]]; then
		echo "--- ${OUT}/summary.txt ---" >&2
		cat "${WORKSPACE}/${OUT}/summary.txt" >&2
	fi
	exit 123
fi
echo "STRESS_TESTS=PASSED"
