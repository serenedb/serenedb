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
#   VERSION  SereneDB version being shipped, e.g. 26.07.2 or 26.07.2.1
#
# A hotfix version (X.Y.Z.W) is not valid SemVer, which Helm requires for the
# chart version, so it is carried as build metadata: chart version X.Y.Z+W,
# appVersion the full X.Y.Z.W.

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
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] ||
	error "VERSION '$VERSION' does not look like X.Y.Z or X.Y.Z.W"

CHART_VERSION="$VERSION"
if [[ "$VERSION" =~ ^([0-9]+\.[0-9]+\.[0-9]+)\.([0-9]+)$ ]]; then
	CHART_VERSION="${BASH_REMATCH[1]}+${BASH_REMATCH[2]}"
fi

sed -i "s/^appVersion: .*/appVersion: \"${VERSION}\"/" "$CHART_YAML"
sed -i "s/^version: .*/version: ${CHART_VERSION}/" "$CHART_YAML"

log "stamped chart version ${CHART_VERSION}, appVersion ${VERSION}"
