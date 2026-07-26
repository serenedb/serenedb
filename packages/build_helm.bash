#!/bin/bash

# Lints and packages the serenedb helm chart into packages/helm/out/.
#
# Usage: build_helm.bash
# Requires: helm 3.8+

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
CHART_DIR="${SCRIPT_DIR}/helm/serenedb"
OUT_DIR="${SCRIPT_DIR}/helm/out"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
error() {
	echo "[ERROR] $*" >&2
	exit 1
}

command -v helm >/dev/null || error "helm not found in PATH"

log "linting chart"
helm lint "$CHART_DIR"

# Render smoke tests: default values plus the feature toggles.
log "render smoke tests"
helm template smoke "$CHART_DIR" >/dev/null
helm template smoke "$CHART_DIR" \
	--set listeners.http.enabled=true \
	--set networkPolicy.enabled=true \
	--set tls.enabled=true --set tls.existingSecret=certs \
	--set persistence.existingClaim=pvc \
	--set auth.existingSecret=creds >/dev/null

mkdir -p "$OUT_DIR"
log "packaging into ${OUT_DIR}"
helm package "$CHART_DIR" -d "$OUT_DIR"

CHART_VERSION=$(sed -n 's/^version: *\(.*\)$/\1/p' "${CHART_DIR}/Chart.yaml")
TGZ="${OUT_DIR}/serenedb-${CHART_VERSION}.tgz"
[[ -f "$TGZ" ]] || error "expected package ${TGZ} was not produced"
log "packaged: ${TGZ}"
