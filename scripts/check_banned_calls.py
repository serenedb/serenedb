#!/usr/bin/env python3
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fix_enum_trailing_comma import blanked  # noqa: E402

BANNED = [
    (
        re.compile(r"\bstd::(unordered_(?:map|set|multimap|multiset))\b"),
        "irs::containers::FlatHashMap, FlatHashSet, NodeHashMap or NodeHashSet",
    ),
    (
        re.compile(r"(?<![\w:])(?:std::)?(v?(?:f|s|sn)?printf)\s*\("),
        "absl::StrFormat, absl::StrAppendFormat, absl::FPrintF or absl::SNPrintF",
    ),
    (
        re.compile(r"\bstd::(v?format(?:_to(?:_n)?)?|print(?:ln)?)\s*\("),
        "absl::StrFormat or absl::StrAppendFormat (fmt only where neither fits)",
    ),
    (
        re.compile(
            r"(?<![\w:])(?:std::)?(sto(?:i|l|ll|ul|ull|f|d|ld)|from_chars"
            r"|ato(?:i|l|ll|f)|strto(?:l|ll|ul|ull|f|d|ld))\s*\("
        ),
        "fast_float::from_chars",
    ),
]

EXCEPTIONS = {
    "iresearch/utils/crash_handler.cpp": {"snprintf"},
}


def check_file(path: str) -> list[str]:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            content = f.read()
    except OSError as e:
        return [f"cannot read: {e}"]

    sh = blanked(content)
    allowed = EXCEPTIONS.get(path, set())
    errors = []
    for regex, instead in BANNED:
        for m in regex.finditer(sh):
            if m.group(1) in allowed:
                continue
            line = sh.count("\n", 0, m.start()) + 1
            errors.append(f"line {line}: use {instead} instead of {m.group(1)}")
    return errors


failed = 0
for path in sys.argv[1:]:
    errors = check_file(path)
    for e in errors:
        print(f"{path}: {e}", file=sys.stderr)
    if errors:
        failed += 1

sys.exit(1 if failed else 0)
