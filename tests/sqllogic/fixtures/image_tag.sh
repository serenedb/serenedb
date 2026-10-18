#!/usr/bin/env bash

set -euo pipefail

dockerfile="$1/Dockerfile"
version=$(sed -n 's/^ARG [A-Z_]*VERSION=//p' "$dockerfile" | head -1)
echo "${version}-$(sha256sum "$dockerfile" | cut -c1-8)"
