#!/usr/bin/env bash
set -euo pipefail

REPO=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
exec "${PDF_FIXTURE_PYTHON:-python3}" "$REPO/scripts/gen_pdf_fixture.py"
