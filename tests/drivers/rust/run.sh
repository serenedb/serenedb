#!/bin/bash
# Rust driver harness: tokio-postgres (D2), sqlx (D3).

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if ! command -v cargo >/dev/null 2>&1; then
	echo "[rust] cargo not found" >&2
	exit 1
fi

cd "$SCRIPT_DIR"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${HOME:-/tmp}/.cargo-target-drivers}"
mkdir -p "$CARGO_TARGET_DIR"

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

# cargo test does not emit JUnit XML natively. We use the standard
# `--format json` output and post-process it; if the cargo2junit tool isn't
# installed, fall back to plain output and the matrix's pass/fail still
# bubbles via cargo's exit code.
final=0
if command -v cargo2junit >/dev/null 2>&1; then
	cargo test --release --offline -- -Z unstable-options --format json --report-time \
		2>&1 | cargo2junit \
		>"$JUNIT/tests-drivers-rust-junit.xml" || final=1
else
	cargo test --release --offline -- --test-threads=1 \
		2>&1 | tee "$JUNIT/tests-drivers-rust.log"
	final=${PIPESTATUS[0]}
fi
exit "$final"
