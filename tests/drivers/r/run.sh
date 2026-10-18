#!/bin/bash
# R / RPostgres driver harness. SereneDB is an analytics target -- R is
# a meaningful BI/data-science client to verify.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

if ! command -v Rscript >/dev/null 2>&1; then
	echo "[r] Rscript not found" >&2
	exit 1
fi
if ! Rscript -e 'suppressMessages({library(DBI); library(RPostgres); library(yaml)})' >/dev/null 2>&1; then
	echo "[r] DBI, RPostgres or yaml is missing; install them with install.packages()" >&2
	exit 1
fi

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

Rscript harness.R "$JUNIT/tests-drivers-r-junit.xml"
