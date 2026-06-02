#!/usr/bin/env python3
"""Check that C++ source filenames (outside third_party) are snake_case.

TODO: extend to all non-third_party files, not just C++.
TODO: rename legacy hyphenated files and remove LEGACY_EXCEPTIONS.
"""

import os
import re
import sys

SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)*\.(cpp|cc|c|hpp|hh|h|ipp|tpp)$")

# Legacy files with hyphens -- rename these and remove from this list
LEGACY_EXCEPTIONS = {
    "application-exit.cpp",
    "application-exit.h",
    "asm-functions.cpp",
    "asm-functions.h",
    "asm-utf8check.cpp",
    "asm-utf8check.h",
    "dtrace-wrapper.h",
    "icu-helper.cpp",
    "icu-helper.h",
    "operating-system.h",
    "string-functions.cpp",
    "system-compiler.h",
    "system-functions.cpp",
    "system-functions.h",
    "tests-common.h",
    "vpack-to-json.cpp",
    "vpack-validate.cpp",
    "10000_writes_test.cpp",
}

errors = 0
for path in sys.argv[1:]:
    basename = os.path.basename(path)
    if basename in LEGACY_EXCEPTIONS:
        continue
    if not SNAKE_CASE_RE.match(basename):
        print(f"{path}: filename is not snake_case")
        errors += 1

sys.exit(1 if errors else 0)
