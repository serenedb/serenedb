#!/usr/bin/env python3
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHECKS = (
    ("scripts/generate_pg_catalog.py", "--check"),
    ("scripts/generate_log_ids.py", "--check"),
    ("scripts/otel/schema.py", "check"),
)


def main():
    stale = [check for check in CHECKS
             if subprocess.run([sys.executable, *check], cwd=ROOT).returncode != 0]
    for script, _ in stale:
        print(f"{script} reports its outputs stale: rerun it", file=sys.stderr)
    return 1 if stale else 0


if __name__ == "__main__":
    sys.exit(main())
