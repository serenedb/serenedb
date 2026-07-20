#!/bin/bash

# Stamps release versions into the chart's Chart.yaml (workspace only -- the
# committed file keeps placeholder versions; real values exist only in
# published packages).
#
# Usage:
#   stamp_chart_version.bash APP_VERSION CHART_VERSION
#
#   APP_VERSION    SereneDB version being shipped, e.g. 26.07.2
#   CHART_VERSION  chart version to publish, e.g. 0.0.7

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
CHART_YAML="${SCRIPT_DIR}/serenedb/Chart.yaml"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
error() {
	echo "[ERROR] $*" >&2
	exit 1
}

APP_VERSION="${1:?usage: stamp_chart_version.bash APP_VERSION CHART_VERSION}"
CHART_VERSION="${2:?usage: stamp_chart_version.bash APP_VERSION CHART_VERSION}"

[[ -f "$CHART_YAML" ]] || error "$CHART_YAML not found"
[[ "$APP_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
	error "APP_VERSION '$APP_VERSION' does not look like X.Y.Z"
[[ "$CHART_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
	error "CHART_VERSION '$CHART_VERSION' does not look like X.Y.Z"

sed -i "s/^appVersion: .*/appVersion: \"${APP_VERSION}\"/" "$CHART_YAML"
sed -i "s/^version: .*/version: ${CHART_VERSION}/" "$CHART_YAML"

log "stamped chart version ${CHART_VERSION}, appVersion ${APP_VERSION}"
