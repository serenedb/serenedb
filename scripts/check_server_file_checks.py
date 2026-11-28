#!/usr/bin/env python3
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fix_enum_trailing_comma import blanked  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TREES = (
    "server",
    "third_party/duckdb/src",
    "third_party/duckdb/extension",
    "third_party/duckdb_iceberg/src",
    "third_party/duckdb_spatial/src",
)
CAPABILITY_TEST = re.compile(
    r"\bPrivilegeTarget::Role\(\s*(?:duckdb::)?RoleCapability::(?:Read|Write)ServerFiles\b"
)
ALLOWED = {
    "third_party/duckdb/src/planner/binder/tableref/bind_basetableref.cpp",
    "server/catalog/catalog_subscription.cpp",
    "server/pg/tsdictionary.cpp",
}


def main():
    failed = False
    script = os.path.relpath(__file__, ROOT)
    for tree in TREES:
        for directory, _, files in os.walk(os.path.join(ROOT, tree)):
            for name in sorted(files):
                if not name.endswith((".cpp", ".hpp", ".h")):
                    continue
                path = os.path.join(directory, name)
                relative = os.path.relpath(path, ROOT)
                if relative in ALLOWED:
                    continue
                with open(path, encoding="utf-8") as source:
                    text = blanked(source.read())
                for match in CAPABILITY_TEST.finditer(text):
                    line = text.count("\n", 0, match.start()) + 1
                    print(
                        f"{relative}:{line}: open the file instead: every open asks ClientContext::CheckFileAccess "
                        f"(a place that names a file without opening it is listed in {script})"
                    )
                    failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
