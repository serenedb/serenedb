#!/usr/bin/env python3
"""Generate unique 4-character base64 log IDs for SDB_LOG macros.

Usage:
  # Replace all "xxxxx" placeholder IDs with unique ones:
  python3 scripts/generate_log_ids.py --fix

  # Check for duplicate or placeholder IDs:
  python3 scripts/generate_log_ids.py --check

  # Generate N new unique IDs (for manual use):
  python3 scripts/generate_log_ids.py --generate 5

The ID format is 4 characters from [A-Za-z0-9+/], giving 16M+ unique values.
"""

import argparse
import os
import random
import re
import string
import sys

ALPHABET = string.ascii_letters + string.digits + "+/"
ID_LEN = 4
PLACEHOLDER = "xxxxx"

# Matches the first argument (log ID) in SDB_LOG/SDB_ERROR/SDB_INFO/etc macros
LOG_MACRO_RE = re.compile(
    r'(SDB_(?:LOG|LOG_IF|TRACE|DEBUG|INFO|WARN|ERROR|FATAL|TRACE_IF|DEBUG_IF|INFO_IF|WARN_IF|ERROR_IF)'
    r'(?:_IF)?)\s*\(\s*"([^"]*)"'
)

EXCLUDE_DIRS = {"third_party", "build", "build_clangd", "build_bench"}


def find_cpp_files(root):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        for f in filenames:
            if f.endswith((".cpp", ".hpp", ".tpp", ".h")):
                yield os.path.join(dirpath, f)


def collect_existing_ids(root):
    ids = {}  # id -> [(file, line)]
    for path in find_cpp_files(root):
        with open(path) as f:
            for i, line in enumerate(f, 1):
                for m in LOG_MACRO_RE.finditer(line):
                    log_id = m.group(2)
                    if log_id != PLACEHOLDER:
                        ids.setdefault(log_id, []).append((path, i))
    return ids


def generate_id(existing):
    while True:
        new_id = "".join(random.choices(ALPHABET, k=ID_LEN))
        if new_id not in existing:
            return new_id


def cmd_check(root):
    existing = collect_existing_ids(root)
    errors = 0

    # Check duplicates
    for log_id, locations in existing.items():
        if len(locations) > 1:
            print(f"Duplicate log ID \"{log_id}\":")
            for path, line in locations:
                print(f"  {path}:{line}")
            errors += 1

    # Check placeholders
    for path in find_cpp_files(root):
        with open(path) as f:
            for i, line in enumerate(f, 1):
                for m in LOG_MACRO_RE.finditer(line):
                    if m.group(2) == PLACEHOLDER:
                        print(f"Placeholder ID \"{PLACEHOLDER}\": {path}:{i}")
                        errors += 1

    if errors:
        print(f"\n{errors} issue(s) found")
        return 1
    print("All log IDs are unique and non-placeholder")
    return 0


def cmd_fix(root):
    existing = collect_existing_ids(root)
    used = set(existing.keys())
    replaced = 0

    for path in find_cpp_files(root):
        with open(path) as f:
            content = f.read()

        def replacer(m):
            nonlocal replaced
            if m.group(2) == PLACEHOLDER:
                new_id = generate_id(used)
                used.add(new_id)
                replaced += 1
                return f'{m.group(1)}("{new_id}"'
            return m.group(0)

        new_content = LOG_MACRO_RE.sub(replacer, content)
        if new_content != content:
            with open(path, "w") as f:
                f.write(new_content)

    print(f"Replaced {replaced} placeholder IDs")
    return 0


def cmd_generate(count):
    used = set()
    for _ in range(count):
        new_id = generate_id(used)
        used.add(new_id)
        print(new_id)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="Check for duplicates/placeholders")
    parser.add_argument("--fix", action="store_true", help="Replace placeholder IDs with unique ones")
    parser.add_argument("--generate", type=int, metavar="N", help="Generate N new unique IDs")
    parser.add_argument("--root", default=".", help="Project root directory")
    args = parser.parse_args()

    if args.generate:
        return cmd_generate(args.generate)
    elif args.fix:
        return cmd_fix(args.root)
    elif args.check:
        return cmd_check(args.root)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
