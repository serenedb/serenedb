#!/bin/bash

# Publishes the packaged serenedb helm chart as a GitHub Release.
#
# Creates release "helm-chart-v<chart version>" with the chart .tgz attached.
# Users install directly from the release asset URL:
#
#   helm install mydb \
#     https://github.com/<org>/<repo>/releases/download/helm-chart-v<ver>/serenedb-<ver>.tgz
#
# Usage: release_chart.bash [--draft]
# Requires: gh (authenticated), a package produced by package_chart.bash.
# Published chart versions are immutable: refuses to overwrite an existing
# non-draft release.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
CHART_YAML="${SCRIPT_DIR}/serenedb/Chart.yaml"
OUT_DIR="${SCRIPT_DIR}/out"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
error() {
	echo "[ERROR] $*" >&2
	exit 1
}

DRAFT_FLAG=""
[[ "${1:-}" == "--draft" ]] && DRAFT_FLAG="--draft"

command -v gh >/dev/null || error "gh CLI not found in PATH"

CHART_VERSION=$(sed -n 's/^version: *\(.*\)$/\1/p' "$CHART_YAML")
APP_VERSION=$(sed -n 's/^appVersion: *"\{0,1\}\([^"]*\)"\{0,1\}$/\1/p' "$CHART_YAML")
TGZ="${OUT_DIR}/serenedb-${CHART_VERSION}.tgz"
TAG="helm-chart-v${CHART_VERSION}"

[[ -f "$TGZ" ]] || error "${TGZ} not found -- run package_chart.bash first"

if gh release view "$TAG" --json isDraft --jq '.isDraft' 2>/dev/null | grep -q false; then
	error "release ${TAG} already published -- bump the chart version instead of overwriting"
fi

REPO=$(gh repo view --json nameWithOwner --jq .nameWithOwner)

log "creating release ${TAG} (chart ${CHART_VERSION}, SereneDB ${APP_VERSION})"
gh release create "$TAG" "$TGZ" \
	$DRAFT_FLAG \
	--title "SereneDB Helm chart ${CHART_VERSION}" \
	--notes "Helm chart ${CHART_VERSION} for SereneDB ${APP_VERSION}.

Install:
\`\`\`
helm install mydb \\
  https://github.com/${REPO}/releases/download/${TAG}/serenedb-${CHART_VERSION}.tgz
\`\`\`" ||
	error "gh release create failed"

log "published ${TAG}"
