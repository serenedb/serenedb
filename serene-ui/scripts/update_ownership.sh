#!/bin/bash
set -euo pipefail

# Fix ownership of screenshot files written by Docker container as root
sudo chown -R "$(id -u):$(id -g)" "${WORKSPACE:-.}" || true
