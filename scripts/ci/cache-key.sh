#!/bin/bash
# Cache key from the build config (build mode, dev/opt, sanitizer, coverage) --
# scopes the gtest-parallel + sqllogic timing caches per profile, never per CI job.
set -euo pipefail

key="${BUILDMODE:?BUILDMODE must be set}"
key="${key,,}"
[[ "${SDB_DEV:?SDB_DEV must be set}" == "On" ]] && key+="-dev" || key+="-opt"
sanitizers="${SANITIZERS:-None}"
[[ -n "$sanitizers" && "$sanitizers" != "None" ]] && key+="-${sanitizers,,}"
[[ -n "${USE_COVERAGE:-}" ]] && key+="-cov"
echo "$key"
