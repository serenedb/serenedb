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
	-v "${WORKSPACE}:/serenedb" \
	"${BUILD_IMAGE}" \
	bash -c '
    set -o pipefail
    cd /serenedb
    mkdir -p /serenedb/out/logs /serenedb/out/test-results
    WORKSPACE=/serenedb BUILD_DIR="${BUILD_DIR}" \
      SDB_STRESS_OUTDIR=/serenedb/out/stress \
      SDB_STRESS_JUNIT=/serenedb/out/test-results \
      ./tests/stress/run.sh 2>&1 | tee -a /serenedb/out/logs/stress-tests.log
  '; then
	echo "STRESS_TESTS=FAILED"
	# The summary carries the seeded repro command and the findings; the thread
	# samples carry the wedge verdict. Both are in the uploaded artifacts, but
	# echo the summary so a failure is readable from the job log alone.
	if [[ -f "${WORKSPACE}/out/stress/summary.txt" ]]; then
		echo "--- out/stress/summary.txt ---" >&2
		cat "${WORKSPACE}/out/stress/summary.txt" >&2
	fi
	exit 123
fi
echo "STRESS_TESTS=PASSED"
