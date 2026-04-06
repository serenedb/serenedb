#!/usr/bin/env python3
"""Check that C++ files use .hpp/.tpp/.cpp extensions.

TODO: disallow .h once all headers are renamed, then remove .h from allowed set.
Currently also allows .h for legacy code.

Flags .cc, .ipp, .c, .c++, .h++, .hh as disallowed.
"""

import os
import sys

# TODO: remove .h once rename is complete
ALLOWED = {".hpp", ".tpp", ".cpp", ".h"}
DISALLOWED_MSG = {
    ".cc": "use .cpp",
    ".ipp": "use .tpp",
    ".c": "use .cpp (or move to a C-specific directory)",
    ".hh": "use .hpp",
    ".h++": "use .hpp",
    ".c++": "use .cpp",
}

errors = 0
for path in sys.argv[1:]:
    ext = os.path.splitext(path)[1]
    if ext in DISALLOWED_MSG:
        print(f"{path}: {ext} not allowed -- {DISALLOWED_MSG[ext]}")
        errors += 1

sys.exit(1 if errors else 0)
