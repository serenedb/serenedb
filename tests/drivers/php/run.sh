#!/bin/bash
# PHP driver harness: PDO_pgsql + ext-pgsql via PHPUnit.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if ! command -v php >/dev/null 2>&1; then
	echo "[php] php not found" >&2
	exit 1
fi

cd "$SCRIPT_DIR"

if [[ ! -e vendor && -n "${SDB_DRIVERS_DEPS:-}" ]]; then
	ln -s "$SDB_DRIVERS_DEPS/php/vendor" vendor
fi
if [[ ! -x vendor/bin/phpunit ]]; then
	echo "[php] vendor is missing; run composer install in $SCRIPT_DIR" >&2
	exit 1
fi

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

vendor/bin/phpunit --log-junit "$JUNIT/tests-drivers-php-junit.xml"
