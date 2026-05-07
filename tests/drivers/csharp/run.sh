#!/bin/bash
# C# driver harness: Npgsql via xunit.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if ! command -v dotnet >/dev/null 2>&1; then
	echo "[csharp] dotnet not found; skipping" >&2
	exit 0
fi

cd "$SCRIPT_DIR"

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

# JunitXml.TestLogger emits the JUnit format the rest of the suite uses.
final=0
dotnet test --logger "junit;LogFilePath=$JUNIT/tests-drivers-csharp-junit.xml" \
	--nologo --verbosity quiet || final=1
exit $final
