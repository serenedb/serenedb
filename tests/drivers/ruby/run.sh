#!/bin/bash
# Ruby / pg (ruby-pg) driver harness.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

if ! command -v ruby >/dev/null 2>&1; then
	echo "[ruby] ruby not found" >&2
	exit 0
fi
if ! command -v gem >/dev/null 2>&1; then
	echo "[ruby] gem not found" >&2
	exit 0
fi

# Install pg gem on first run. The gem is small but compiles a libpq
# extension; libpq-dev is in the build image.
if ! ruby -e 'require "pg"' 2>/dev/null; then
	gem install --no-document --user-install pg yaml >/dev/null 2>&1 ||
		gem install --no-document --user-install pg yaml >&2
fi

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

ruby harness.rb "$JUNIT/tests-drivers-ruby-junit.xml"
