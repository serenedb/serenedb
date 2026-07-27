#!/usr/bin/env python3
"""Pre-commit hook: drop the empty parameter list from lambdas.

`[&]() {` becomes `[&] {`. Only the bare form is rewritten -- a lambda
whose `()` is followed by `mutable`, `noexcept`, `->` or an attribute
keeps its parentheses (required before C++23). Files are fixed in
place; the hook is ordered before clang-format.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fix_enum_trailing_comma import blanked, fix_files  # noqa: E402

LAMBDA_RE = re.compile(r"(?<!\])(?<!operator\[)(?<!operator \[)\](\s*\(\s*\)\s*)\{")


def drop_empty_parens(content: str) -> tuple[str, int]:
    spans = [m.span(1) for m in LAMBDA_RE.finditer(blanked(content))]
    for start, end in reversed(spans):
        content = content[:start] + " " + content[end:]
    return content, len(spans)


if __name__ == "__main__":
    sys.exit(
        fix_files(sys.argv[1:], drop_empty_parens, "removed empty lambda ()")
    )
