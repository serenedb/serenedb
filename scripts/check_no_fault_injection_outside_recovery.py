#!/usr/bin/env python3
import re
import sys

# Matches SET (possibly with whitespace/newlines) followed by sdb_fault_* identifier
FAULT_PATTERN = re.compile(r"SET\s+(sdb_fault_\w+)", re.IGNORECASE)

RECOVERY_DIR = "tests/sqllogic/recovery/"

failed = False
for path in sys.argv[1:]:
    # Normalize path separators and check if it's in the recovery folder
    normalized = path.replace("\\", "/")
    if RECOVERY_DIR in normalized:
        continue

    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            content = f.read()
    except OSError as e:
        print(f"{path}: error reading: {e}", file=sys.stderr)
        failed = True
        continue

    for match in FAULT_PATTERN.finditer(content):
        line_number = content.count("\n", 0, match.start()) + 1
        # Reconstruct the SET command text as it appears (collapse internal whitespace)
        set_command = f"SET {match.group(1)}"
        print(
            f"Test file {path}:{line_number} contains '{set_command}' and should be moved to recovery folder",
            file=sys.stderr,
        )
        failed = True

sys.exit(1 if failed else 0)
