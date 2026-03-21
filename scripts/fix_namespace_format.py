#!/usr/bin/env python3
"""Auto-fix namespace formatting. Run manually, not a pre-commit hook."""

import re
import sys
from pathlib import Path

NS_OPEN = re.compile(r"^\s*(?:inline\s+)?namespace(?:\s+[\w:]+)?\s*\{\s*$")
NS_CLOSE = re.compile(r"^\s*\}\s*//\s*namespace")


def fix(lines: list[str]) -> list[str]:
    # Pass 1: ensure blank line after NS_OPEN, but NOT before another NS_OPEN or NS_CLOSE
    result = []
    for i, line in enumerate(lines):
        result.append(line)
        if NS_OPEN.match(line):
            next_line = lines[i + 1] if i + 1 < len(lines) else None
            if next_line is not None and not NS_OPEN.match(next_line) and not NS_CLOSE.match(next_line) and next_line != "":
                result.append("")

    lines = result

    # Pass 2: ensure blank line before NS_CLOSE, but NOT after another NS_CLOSE
    result = []
    for i, line in enumerate(lines):
        if NS_CLOSE.match(line):
            prev = result[-1] if result else None
            if prev is not None and not NS_CLOSE.match(prev) and prev != "":
                result.append("")
        result.append(line)
    lines = result

    # Pass 3: remove blank lines between NS_CLOSE and NS_OPEN
    result = []
    i = 0
    while i < len(lines):
        line = lines[i]
        result.append(line)
        if NS_CLOSE.match(line):
            j = i + 1
            while j < len(lines) and lines[j] == "":
                j += 1
            if j > i + 1 and j < len(lines) and NS_OPEN.match(lines[j]):
                i = j - 1  # skip blank lines
        i += 1
    lines = result

    # Pass 4: remove blank lines between NS_OPEN and NS_OPEN
    result = []
    i = 0
    while i < len(lines):
        line = lines[i]
        result.append(line)
        if NS_OPEN.match(line):
            j = i + 1
            while j < len(lines) and lines[j] == "":
                j += 1
            if j > i + 1 and j < len(lines) and NS_OPEN.match(lines[j]):
                i = j - 1  # skip blank lines
        i += 1
    lines = result

    # Pass 5: remove blank lines between NS_CLOSE and NS_CLOSE
    result = []
    i = 0
    while i < len(lines):
        line = lines[i]
        result.append(line)
        if NS_CLOSE.match(line):
            j = i + 1
            while j < len(lines) and lines[j] == "":
                j += 1
            if j > i + 1 and j < len(lines) and NS_CLOSE.match(lines[j]):
                i = j - 1  # skip blank lines
        i += 1

    return result


def fix_file(path: str) -> bool:
    text = Path(path).read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    fixed = fix(lines)
    if fixed == lines:
        return False
    Path(path).write_text("\n".join(fixed) + ("\n" if text.endswith("\n") else ""), encoding="utf-8")
    return True


def main():
    files = sys.argv[1:]
    changed = 0
    for path in files:
        if fix_file(path):
            print(f"fixed: {path}")
            changed += 1
    if changed:
        print(f"\n{changed} file(s) fixed.")


if __name__ == "__main__":
    main()
