#!/bin/bash
# R / RPostgres driver harness. SereneDB is an analytics target -- R is
# a meaningful BI/data-science client to verify.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

if ! command -v Rscript >/dev/null 2>&1; then
	echo "[r] Rscript not found" >&2
	exit 0
fi

# The CI build image ships DBI/yaml (apt) and RPostgres (compiled at image
# build), so this block is a no-op there. It stays for local runs on a bare R:
# RPostgres has no Ubuntu package and takes ~1min to compile against libpq,
# so install under R_LIBS_USER where we don't need root.
R_USER_LIB="${R_LIBS_USER:-${HOME}/R/library}"
mkdir -p "$R_USER_LIB"
export R_LIBS_USER="$R_USER_LIB"
if ! Rscript -e 'suppressMessages(library(RPostgres))' >/dev/null 2>&1; then
	Rscript -e 'install.packages(c("DBI","RPostgres","yaml"),
	    lib=Sys.getenv("R_LIBS_USER"),
	    repos="https://cloud.r-project.org",
	    quiet=TRUE)' >/dev/null 2>&1 ||
		Rscript -e 'install.packages(c("DBI","RPostgres","yaml"),
	    lib=Sys.getenv("R_LIBS_USER"),
	    repos="https://cloud.r-project.org")'
fi

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

Rscript harness.R "$JUNIT/tests-drivers-r-junit.xml"
