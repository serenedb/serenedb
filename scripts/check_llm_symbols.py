#!/usr/bin/env python3
"""Check and fix Unicode symbols commonly inserted by LLMs."""

import sys

REPLACEMENTS = {
    "\u00d7": "x",      # × -> x
    "\u2192": "->",     # → -> ->
    "\u2190": "<-",     # ← -> <-
    "\u2194": "<->",    # ↔ -> <->
    "\u21d2": "=>",     # ⇒ -> =>
    "\u21d0": "<=",     # ⇐ -> <=
    "\u21d4": "<=>",    # ⇔ -> <=>
    "\u2196": "<-",     # ↖ -> <-
    "\u2197": "->",     # ↗ -> ->
    "\u2198": "->",     # ↘ -> ->
    "\u2199": "<-",     # ↙ -> <-
    "\u21a6": "->",     # ↦ -> ->
    "\u27f6": "->",     # ⟶ -> ->
    "\u27f5": "<-",     # ⟵ -> <-
    "\u27f7": "<->",    # ⟷ -> <->
    "\u27f9": "=>",     # ⟹ -> =>
    "\u27f8": "<=",     # ⟸ -> <=
    "\u27fa": "<=>",    # ⟺ -> <=>
    "\u2018": "'",      # ‘ -> '
    "\u2019": "'",      # ’ -> '
    "\u201c": '"',      # “ -> "
    "\u201d": '"',      # ” -> "
    "\u2014": "--",     # — -> --
    "\u2013": "-",      # – -> -
    "\u2026": "...",    # … -> ...
    "\u2212": "-",      # − -> -
    "\u2217": "*",      # ∗ -> *
    "\u00a0": " ",      #   -> space
    "\u00f7": "/",      # ÷ -> /
    "\u2260": "!=",     # ≠ -> !=
    "\u2264": "<=",     # ≤ -> <=
    "\u2265": ">=",     # ≥ -> >=
}

EXCEPTIONS = {
    "scripts/check_llm_symbols.py",
}


def check_file(path: str) -> list[str]:
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except OSError as e:
        return [f"cannot read: {e}"]

    errors = []
    for i, line in enumerate(text.splitlines(), 1):
        for char, replacement in REPLACEMENTS.items():
            if char in line:
                errors.append(f"line {i}: found {repr(char)} (U+{ord(char):04X}), should be {repr(replacement)}")
    return errors


def fix_file(path: str) -> bool:
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        return False

    fixed = text
    for char, replacement in REPLACEMENTS.items():
        fixed = fixed.replace(char, replacement)

    if fixed == text:
        return False
    with open(path, "w", encoding="utf-8") as f:
        f.write(fixed)
    return True


failed = 0
for path in sys.argv[1:]:
    if path in EXCEPTIONS:
        continue
    errors = check_file(path)
    for e in errors:
        print(f"{path}: {e}", file=sys.stderr)
    if errors:
        fix_file(path)
        failed += 1

sys.exit(1 if failed else 0)
