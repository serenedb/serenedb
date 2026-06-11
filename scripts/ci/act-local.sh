#!/usr/bin/env bash
# Run / validate the GitHub Actions workflows locally via nektos/act, so CI
# changes can be checked without pushing. Resolves act from PATH, the gh-act
# extension, or a local download (out/bin/act); shares the host docker socket so
# the workflows' in-container `docker run` build steps work like on the real
# self-hosted runners.
#
# Usage:
#   scripts/ci/act-local.sh list                       # list workflows/jobs
#   scripts/ci/act-local.sh validate [workflow.yml]    # parse + dry-run (-n), no exec
#   scripts/ci/act-local.sh run <workflow.yml> [-j JOB] [act args...]
#   scripts/ci/act-local.sh classify [BASE_REF]        # shortcut: run the change classifier locally (no act)
#
# Secrets: put fakes in .secrets (gitignored) as KEY=VALUE lines; passed via --secret-file.
# Limits: heavy docker-in-docker builds need the build image + /mnt/data caches;
# `validate` always works, full `run` depends on local resources. Honest about it.

set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." &>/dev/null && pwd)"
cd "$ROOT"
OUT_BIN="$ROOT/out/bin"

log() { echo "[act-local] $*" >&2; }

resolve_act() {
	if command -v act >/dev/null 2>&1; then
		ACT=(act)
		return
	fi
	if gh act --version >/dev/null 2>&1; then
		ACT=(gh act)
		return
	fi
	if [[ -x "$OUT_BIN/act" ]]; then
		ACT=("$OUT_BIN/act")
		return
	fi
	log "act not found. Installing..."
	if command -v gh >/dev/null 2>&1 && gh extension install nektos/gh-act 2>/dev/null; then
		ACT=(gh act)
		log "installed gh-act extension"
		return
	fi
	log "Falling back to downloading the act binary into $OUT_BIN"
	mkdir -p "$OUT_BIN"
	curl -fsSL https://raw.githubusercontent.com/nektos/act/master/install.sh |
		bash -s -- -b "$OUT_BIN" >&2
	ACT=("$OUT_BIN/act")
}

secret_args() {
	[[ -f "$ROOT/.secrets" ]] && printf -- '--secret-file\n%s\n' "$ROOT/.secrets"
}

case "${1:-help}" in
classify)
	shift
	exec "$ROOT/scripts/ci/classify-changes.sh" "$@"
	;;
list)
	resolve_act
	exec "${ACT[@]}" --list
	;;
validate)
	resolve_act
	wf="${2:-}"
	if [[ -n "$wf" ]]; then exec "${ACT[@]}" -n -W ".github/workflows/$wf"; fi
	exec "${ACT[@]}" -n
	;;
run)
	shift
	wf="${1:?usage: act-local.sh run <workflow.yml> [act args...]}"
	shift
	resolve_act
	# Share the host docker daemon so in-container `docker run` works.
	exec "${ACT[@]}" -W ".github/workflows/$wf" \
		--container-daemon-socket /var/run/docker.sock \
		$(secret_args) "$@"
	;;
help | *)
	# Print the leading comment block (line 2 up to the first blank line).
	sed -n '2,/^$/p' "${BASH_SOURCE[0]}"
	;;
esac
