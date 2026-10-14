#!/usr/bin/env python3
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fix_enum_trailing_comma import blanked, fix_files  # noqa: E402

LIST_START_RE = re.compile(r"\btemplate\s*<|\]\s*<(?=\s*(?:class|typename)\b)")
CLASS_RE = re.compile(r"(?<!\benum\s)\bclass\b")


def parameter_lists(sh: str) -> list[tuple[int, int]]:
    spans = []
    for m in LIST_START_RE.finditer(sh):
        depth, nesting, k = 1, 0, m.end()
        while k < len(sh) and depth:
            c = sh[k]
            if c in "([{":
                nesting += 1
            elif c in ")]}":
                nesting -= 1
            elif nesting == 0 and c == "<":
                depth += 1
            elif nesting == 0 and c == ">":
                depth -= 1
            k += 1
        if depth == 0:
            spans.append((m.end(), k - 1))
    return spans


def class_to_typename(content: str) -> tuple[str, int]:
    sh = blanked(content)
    offsets = set()
    for start, end in parameter_lists(sh):
        offsets.update(m.start() for m in CLASS_RE.finditer(sh, start, end))
    for off in sorted(offsets, reverse=True):
        content = content[:off] + "typename" + content[off + len("class") :]
    return content, len(offsets)


if __name__ == "__main__":
    sys.exit(
        fix_files(
            sys.argv[1:], class_to_typename, "replaced template <class> by <typename>"
        )
    )
