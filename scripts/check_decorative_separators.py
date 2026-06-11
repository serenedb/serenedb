#!/usr/bin/env python3
"""Detect and optionally remove decorative comment separator lines.

A line is treated as a separator when it contains no letters or digits
and has a run of 5+ of the same character from `=`, `-`, `/`, or `*`.
The 5-char threshold leaves sqllogictest's 4-dash row delimiter alone.

The license header at the top of each file is skipped automatically,
both the project's 80-slash bracket and the Postgres-style block form.

Dry-run by default; pass --apply to rewrite files in place.
"""

import argparse
import re
import sys

RUN_RE = re.compile(r"([=\-/*])\1{4,}")
ALNUM_RE = re.compile(r"[A-Za-z0-9]")

# License-header brackets to skip at the top of a file.
SLASH_BORDER = "/" * 80
PG_OPEN = re.compile(r"^/\*-{5,}\s*$")
PG_CLOSE = re.compile(r"^\s*\*-{5,}\s*$")

EXCEPTIONS: set[str] = set()

EXCLUDE_PREFIXES: tuple[str, ...] = (
    "third_party/",
    "libs/iresearch/",
    "libs/basics/",
    "libs/fuerte/",
    "libs/endpoint/",
    "libs/http_client/",
    "tests/libs/iresearch/",
    "tests/libs/fuerte/",
)


def is_separator(line: str) -> bool:
    return not ALNUM_RE.search(line) and bool(RUN_RE.search(line))


def header_end(lines: list[str]) -> int:
    """Inclusive index of the closing line of a file-top license block, else -1."""
    if not lines:
        return -1
    if lines[0] == SLASH_BORDER:
        is_close = SLASH_BORDER.__eq__
    elif PG_OPEN.match(lines[0]):
        is_close = lambda l: bool(PG_CLOSE.match(l))
    else:
        return -1
    for i in range(1, min(len(lines), 60)):
        if is_close(lines[i]):
            return i
    return -1


def find_separators(path: str) -> list[tuple[int, str]]:
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        return []
    lines = text.splitlines()
    skip_through = header_end(lines)
    return [
        (i + 1, line)
        for i, line in enumerate(lines)
        if i > skip_through and is_separator(line)
    ]


def rewrite(path: str, drop: set[int]) -> None:
    with open(path, encoding="utf-8", errors="replace") as f:
        text = f.read()
    trailing_nl = text.endswith("\n")
    kept = [ln for i, ln in enumerate(text.splitlines(), 1) if i not in drop]
    out = "\n".join(kept) + ("\n" if kept and trailing_nl else "")
    with open(path, "w", encoding="utf-8") as f:
        f.write(out)


def is_excluded(path: str) -> bool:
    return path in EXCEPTIONS or any(path.startswith(p) for p in EXCLUDE_PREFIXES)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="rewrite files (default: dry run)")
    parser.add_argument("paths", nargs="*")
    args = parser.parse_args()

    total_lines = total_files = 0
    for path in args.paths:
        if is_excluded(path):
            continue
        hits = find_separators(path)
        if not hits:
            continue
        total_files += 1
        total_lines += len(hits)
        for lineno, line in hits:
            print(f"{path}:{lineno}: {line}")
        if args.apply:
            rewrite(path, {n for n, _ in hits})

    if total_lines:
        verb = "removed" if args.apply else "would be removed"
        print(f"\n{total_lines} line(s) across {total_files} file(s) {verb}.", file=sys.stderr)
        return 0 if args.apply else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
