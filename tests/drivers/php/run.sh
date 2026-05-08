#!/bin/bash
# PHP driver harness: PDO_pgsql + ext-pgsql via PHPUnit.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if ! command -v php >/dev/null 2>&1; then
	echo "[php] php not found; skipping" >&2
	exit 0
fi
if ! command -v composer >/dev/null 2>&1; then
	echo "[php] composer not found; skipping" >&2
	exit 0
fi

cd "$SCRIPT_DIR"

if [[ ! -d vendor ]]; then
	composer install --quiet --no-interaction
fi

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

vendor/bin/phpunit --log-junit "$JUNIT/tests-drivers-php-junit.xml"
