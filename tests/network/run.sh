#!/bin/bash
#
# HBA network tests: end-to-end host-based-authentication CIDR/mask matching.
#
# The mask test starts its OWN serened and connects from different 127.x source
# addresses (loopback source-binding), so it must run on the SAME host as the
# server -- it cannot use a docker-compose serened in a separate container.
# That is why it is a standalone launcher, not a --host client like the driver
# tests.
#
# Local:  tests/network/run.sh
# CI:     invoked by scripts/ci/steps/049-ci-in-docker-run-network-tests.bash
# RTA:    invoked by scripts/ci/steps/08-ci-{docker,deb,tarball}-rta.bash with
#         SERENED pointing at the released binary
#
set -o pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
: "${WORKSPACE:=$(realpath "${SCRIPT_DIR}/../..")}"
: "${BUILD_DIR:=build}"

: "${SERENED:=${WORKSPACE}/${BUILD_DIR}/bin/serened}"
if [[ ! -x "$SERENED" ]]; then
	echo "[network] serened not found at $SERENED" >&2
	exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
	echo "[network] python3 not found; skipping" >&2
	exit 0
fi
if ! command -v psql >/dev/null 2>&1; then
	echo "[network] psql not found; skipping (mask test needs SET hba over psql)" >&2
	exit 0
fi

echo "[network] running HBA mask test (serened=$SERENED)"
python3 "${SCRIPT_DIR}/hba_mask_test.py" --serened "$SERENED"
