#!/usr/bin/env python3
"""Check that C++ files use approved extensions.

Three rule sets exist:
- Default (most of the codebase): .h is allowed, .hpp is disallowed
  (transition period; TODO: add .h once rename is complete).
- libs/iresearch: .hpp is allowed, .h is disallowed.
- Mixed directories (libs/basics, server/connector): both .h and .hpp are allowed,
  while other unwanted extensions remain forbidden.
"""

import os
import sys

# Default disallowed extensions (for most of the codebase)
DEFAULT_DISALLOWED = {
    ".cc": "use .cpp",
    ".ipp": "use .tpp",
    ".c": "use .cpp (or move to a C-specific directory)",
    ".hh": "use .hpp",
    ".h++": "use .hpp",
    ".c++": "use .cpp",
    ".hpp": "use .h",   # .hpp not allowed in the main code (transition period)
}

# Disallowed extensions for libs/iresearch
IRESEARCH_DISALLOWED = {
    ".cc": "use .cpp",
    ".ipp": "use .tpp",
    ".c": "use .cpp (or move to a C-specific directory)",
    ".hh": "use .hpp",
    ".h++": "use .hpp",
    ".c++": "use .cpp",
    ".h": "use .hpp",   # iresearch must use .hpp headers
}

# Mixed directories where both .h and .hpp are allowed
MIXED_DISALLOWED = {
    ".cc": "use .cpp",
    ".ipp": "use .tpp",
    ".c": "use .cpp (or move to a C-specific directory)",
    ".hh": "use .hpp",
    ".h++": "use .hpp",
    ".c++": "use .cpp",
    # no .h or .hpp entries -> both are permitted
}

errors = 0
for path in sys.argv[1:]:
    # Choose rule set based on path
    if "libs/iresearch" in path:
        disallowed = IRESEARCH_DISALLOWED
    elif "libs/basics" in path or "server/connector" in path:
        disallowed = MIXED_DISALLOWED
    else:
        disallowed = DEFAULT_DISALLOWED

    ext = os.path.splitext(path)[1]
    if ext in disallowed:
        print(f"{path}: {ext} not allowed -- {disallowed[ext]}")
        errors += 1

sys.exit(1 if errors else 0)
