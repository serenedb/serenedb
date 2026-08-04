#!/bin/bash

# Stamps the release version into the chart's Chart.yaml (workspace only --
# the committed file keeps placeholder versions; real values exist only in
# published packages). Chart version and appVersion are both set to the
# SereneDB version: the chart is released only as an asset of the SereneDB
# GitHub release, so the versions always match.
#
# Usage:
#   stamp_helm_version.bash VERSION
#
#   VERSION  SereneDB version being shipped, e.g. 26.07.2

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
CHART_YAML="${SCRIPT_DIR}/helm/serenedb/Chart.yaml"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
error() {
	echo "[ERROR] $*" >&2
	exit 1
}

VERSION="${1:?usage: stamp_helm_version.bash VERSION}"

[[ -f "$CHART_YAML" ]] || error "$CHART_YAML not found"
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
	error "VERSION '$VERSION' does not look like X.Y.Z"

sed -i "s/^appVersion: .*/appVersion: \"${VERSION}\"/" "$CHART_YAML"
sed -i "s/^version: .*/version: ${VERSION}/" "$CHART_YAML"

log "stamped chart version ${VERSION}, appVersion ${VERSION}"
