#!/usr/bin/env python3
"""Pre-commit hook: enforce `control substitution on` in sqllogic tests.

For each .test file that references ${...} substitution outside comments,
ensure `control substitution on` is present and appears before the first
use. If missing or misplaced, the directive is moved to the top of the
file and the hook exits non-zero so the developer re-stages and commits.
"""
import re
import sys

SUBST_RE = re.compile(r"\$\{[^}]+\}")
DIRECTIVE = "control substitution on"


def first_subst_line(lines: list[str]) -> int | None:
    """Return 1-based line number of the first ${...} use outside comments."""
    for i, line in enumerate(lines, start=1):
        if line.lstrip().startswith("#"):
            continue
        if SUBST_RE.search(line):
            return i
    return None


def directive_line(lines: list[str]) -> int | None:
    """Return 1-based line number of `control substitution on`, if present."""
    for i, line in enumerate(lines, start=1):
        if line.strip() == DIRECTIVE:
            return i
    return None


def directive_at_top(content: str, drop_line: int | None) -> str:
    """Return content with the directive at the top, dropping a misplaced
    occurrence at `drop_line` (1-based) first if given."""
    body = content
    if drop_line is not None:
        lines = content.splitlines()
        kept = [l for i, l in enumerate(lines, start=1) if i != drop_line]
        body = "\n".join(kept) + ("\n" if content.endswith("\n") else "")
    sep = "" if body.startswith("\n") else "\n"
    return f"{DIRECTIVE}\n{sep}{body}"


failed = False
for path in sys.argv[1:]:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            content = f.read()
    except OSError as e:
        print(f"{path}: error reading: {e}", file=sys.stderr)
        failed = True
        continue

    lines = content.splitlines()
    first_use = first_subst_line(lines)
    if first_use is None:
        continue

    directive = directive_line(lines)
    if directive is not None and directive < first_use:
        continue

    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(directive_at_top(content, directive))
    except OSError as e:
        print(f"{path}: error writing: {e}", file=sys.stderr)
        failed = True
        continue

    if directive is None:
        reason = f"lacks '{DIRECTIVE}'"
    else:
        reason = f"used before '{DIRECTIVE}' (directive was at line {directive})"
    print(
        f"{path}:{first_use}: ${{...}} substitution {reason}. "
        f"Moved directive to top; please re-stage and commit.",
        file=sys.stderr,
    )
    failed = True

sys.exit(1 if failed else 0)
