#!/bin/bash
# psql backslash meta-command parity harness.
#
# Thin shim that delegates to run.py: the harness is a Python script because
# the test file is sqllogic-style with multi-line goldens (round-tripping that
# format from bash isn't worth the complexity).

exec python3 "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)/run.py" "$@"
