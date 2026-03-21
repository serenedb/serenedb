#!/usr/bin/env python3
"""
Check namespace formatting rules:
  - blank line required after 'namespace ... {' only if next line is not another namespace
  - blank line required before '}  // namespace' only if previous line is not another '}'
  - no blank line between '}  // namespace' and next 'namespace ... {'
  - no blank line between 'namespace ... {' and next 'namespace ... {'
"""

import re
import sys

NS_OPEN = re.compile(r"^\s*(?:inline\s+)?namespace(?:\s+[\w:]+)?\s*\{\s*$")
NS_CLOSE = re.compile(r"^\s*\}\s*//\s*namespace")

EXCEPTIONS = set()


def check_file(path: str) -> list[str]:
    try:
        lines = open(path, encoding="utf-8", errors="replace").read().splitlines()
    except OSError as e:
        return [f"cannot read: {e}"]

    errors = []
    for i, line in enumerate(lines):
        lineno = i + 1
        next_line = lines[i + 1] if i + 1 < len(lines) else None
        prev_line = lines[i - 1] if i > 0 else None

        if NS_OPEN.match(line):
            # No blank line before next namespace open or close
            if next_line is not None and not NS_OPEN.match(next_line) and not NS_CLOSE.match(next_line):
                if next_line != "":
                    errors.append(f"line {lineno}: blank line required after '{line.strip()}'")
                elif i + 2 < len(lines) and NS_OPEN.match(lines[i + 2]):
                    errors.append(f"line {lineno}: no blank line allowed between consecutive namespaces")

        if NS_CLOSE.match(line):
            # No blank line required before another close
            if prev_line is not None and not NS_CLOSE.match(prev_line):
                if prev_line != "":
                    errors.append(f"line {lineno}: blank line required before '{line.strip()}'")
                elif i >= 2 and NS_CLOSE.match(lines[i - 2]):
                    errors.append(f"line {lineno}: no blank line allowed between consecutive closing namespaces")
            # No blank line before next namespace open
            if next_line == "" and i + 2 < len(lines) and NS_OPEN.match(lines[i + 2]):
                errors.append(f"line {lineno}: no blank line allowed between '}}  // namespace' and next 'namespace'")

    return errors


failed = 0
for path in sys.argv[1:]:
    if path in EXCEPTIONS:
        continue
    errors = check_file(path)
    for e in errors:
        print(f"{path}: {e}", file=sys.stderr)
    if errors:
        failed += 1

sys.exit(1 if failed else 0)
