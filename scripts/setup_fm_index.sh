#!/usr/bin/env bash
set -euo pipefail

fm_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
fm_source="${1:-${fm_root}/build_fm_deps/sdsl-lite}"
fm_revision=c32874cb2d8524119f25f3b501526fe692df29f4

if [[ ! -d "${fm_source}/.git" ]]; then
	git clone https://github.com/simongog/sdsl-lite.git "${fm_source}"
fi
if [[ "$(git -C "${fm_source}" rev-parse HEAD)" != "${fm_revision}" ]]; then
	git -C "${fm_source}" checkout "${fm_revision}"
fi
git -C "${fm_source}" submodule update --init external/libdivsufsort
for fm_patch in "${fm_root}"/third_party/sdsl-cmake/*.patch; do
	if ! git -C "${fm_source}" apply --reverse --check "${fm_patch}" 2>/dev/null; then
		git -C "${fm_source}" apply --check "${fm_patch}"
		git -C "${fm_source}" apply "${fm_patch}"
	fi
done
printf 'SDSL ready: -DSDB_FM_INDEX=ON -DSDB_SDSL_SOURCE_DIR=%s\n' "${fm_source}"
