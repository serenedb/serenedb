#!/usr/bin/env python3
"""Check that header files do not contain anonymous namespaces.

Anonymous namespaces in headers cause ODR violations when the header
is included in multiple translation units.
"""

import re
import sys

ANON_NS = re.compile(r"^\s*namespace\s*\{")


def check_file(path: str) -> list[str]:
    try:
        lines = open(path, encoding="utf-8", errors="replace").read().splitlines()
    except OSError as e:
        return [f"cannot read: {e}"]

    errors = []
    for i, line in enumerate(lines):
        if ANON_NS.match(line):
            errors.append(f"line {i + 1}: anonymous namespace in header: '{line.strip()}'")
    return errors


failed = 0
for path in sys.argv[1:]:
    errors = check_file(path)
    for e in errors:
        print(f"{path}: {e}", file=sys.stderr)
    if errors:
        failed += 1

sys.exit(1 if failed else 0)
