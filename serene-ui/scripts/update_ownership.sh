#!/bin/bash
set -euo pipefail

# Fix ownership of screenshot files written by Docker container as root
workspace="${WORKSPACE:-.}"
owner="$(id -u):$(id -g)"

if [[ "$(id -u)" -eq 0 ]]; then
  chown -R "$owner" "$workspace" || true
elif command -v sudo >/dev/null 2>&1; then
  if sudo -n true >/dev/null 2>&1; then
    sudo chown -R "$owner" "$workspace" || true
  elif [[ -t 0 ]]; then
    sudo chown -R "$owner" "$workspace" || true
  else
    echo "Skipping ownership update: sudo requires a password in non-interactive mode."
  fi
else
  echo "Skipping ownership update: sudo is not available."
fi
