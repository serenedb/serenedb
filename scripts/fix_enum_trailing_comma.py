#!/usr/bin/env python3
"""Pre-commit hook: enforce a trailing comma after the last enumerator.

The trailing comma keeps appends single-line in diffs and makes
clang-format lay out one enumerator per line. Files are fixed in place;
the hook is ordered before clang-format so the reflow happens in the
same pre-commit run.
"""
import re
import sys

ENUM_RE = re.compile(r"\benum\b")
HEAD_OK_RE = re.compile(r"[A-Za-z0-9_:\s\[\]]*", re.DOTALL)


def blanked(content: str) -> str:
    """content with comments replaced by spaces and string/char literals by
    'x' filler (still visible as content), newlines and offsets preserved."""
    out = list(content)
    i, n = 0, len(content)

    def blank(start: int, end: int, fill: str) -> None:
        for k in range(start, end):
            if out[k] != "\n":
                out[k] = fill

    while i < n:
        c = content[i]
        if c == "/" and i + 1 < n and content[i + 1] == "/":
            j = content.find("\n", i)
            j = n if j == -1 else j
            blank(i, j, " ")
            i = j
        elif c == "/" and i + 1 < n and content[i + 1] == "*":
            j = content.find("*/", i + 2)
            j = n if j == -1 else j + 2
            blank(i, j, " ")
            i = j
        elif c == "R" and content[i + 1 : i + 2] == '"':
            m = re.match(r'R"([^ ()\\\n]*)\(', content[i:])
            if not m:
                i += 1
                continue
            close = f"){m.group(1)}\""
            j = content.find(close, i + m.end())
            j = n if j == -1 else j + len(close)
            blank(i, j, "x")
            i = j
        elif c in "\"'":
            j = i + 1
            while j < n and content[j] != c:
                j += 2 if content[j] == "\\" else 1
            j = min(j + 1, n)
            blank(i, j, "x")
            i = j
        else:
            i += 1
    return "".join(out)


def fix_files(paths: list[str], fixer, what: str) -> int:
    """Shared driver: rewrite each file with `fixer`, report, exit code."""
    failed = False
    for path in paths:
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                content = f.read()
        except OSError as e:
            print(f"{path}: error reading: {e}", file=sys.stderr)
            failed = True
            continue

        fixed, count = fixer(content)
        if not count:
            continue

        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(fixed)
        except OSError as e:
            print(f"{path}: error writing: {e}", file=sys.stderr)
            failed = True
            continue

        print(
            f"{path}: {what} in {count} place(s); re-stage and commit.",
            file=sys.stderr,
        )
        failed = True
    return 1 if failed else 0


def missing_comma_offsets(sh: str) -> list[int]:
    """Offsets at which a trailing comma must be inserted."""
    offsets = []
    for m in ENUM_RE.finditer(sh):
        brace = sh.find("{", m.end())
        semi = sh.find(";", m.end())
        if brace == -1 or (semi != -1 and semi < brace):
            continue
        if HEAD_OK_RE.fullmatch(sh, m.end(), brace) is None:
            continue
        depth, k = 0, brace
        while k < len(sh):
            if sh[k] == "{":
                depth += 1
            elif sh[k] == "}":
                depth -= 1
                if depth == 0:
                    break
            k += 1
        if k >= len(sh):
            continue
        e = k - 1
        while e > brace and sh[e].isspace():
            e -= 1
        if e > brace and sh[e] != ",":
            offsets.append(e + 1)
    return offsets


def add_trailing_commas(content: str) -> tuple[str, int]:
    offsets = missing_comma_offsets(blanked(content))
    for off in reversed(offsets):
        content = content[:off] + "," + content[off:]
    return content, len(offsets)


if __name__ == "__main__":
    sys.exit(
        fix_files(sys.argv[1:], add_trailing_commas, "added enum trailing comma")
    )
