#!/usr/bin/env python3
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLANNER = "third_party/duckdb/src/planner"
DIRECT_LOOKUP = re.compile(r"Catalog::(?:GetEntry(?:<\w+>)?|GetSchema)\(\s*(\w+)")
ROUTED_ARGUMENTS = frozenset({"retriever", "entry_retriever"})
ALLOWED = {
    f"{PLANNER}/binder.cpp": 1,
    f"{PLANNER}/binder/expression/bind_window_expression.cpp": 1,
    f"{PLANNER}/binder/statement/bind_alter.cpp": 3,
    f"{PLANNER}/binder/statement/bind_copy.cpp": 1,
    f"{PLANNER}/binder/statement/bind_create.cpp": 5,
    f"{PLANNER}/binder/statement/bind_create_table.cpp": 2,
    f"{PLANNER}/binder/statement/bind_delete.cpp": 2,
    f"{PLANNER}/binder/statement/bind_drop.cpp": 1,
    f"{PLANNER}/binder/statement/bind_export.cpp": 1,
    f"{PLANNER}/binder/statement/bind_merge_into.cpp": 1,
    f"{PLANNER}/binder/statement/bind_pragma.cpp": 3,
    f"{PLANNER}/binder/statement/bind_update.cpp": 1,
    f"{PLANNER}/binder/tableref/bind_pivot.cpp": 2,
    f"{PLANNER}/binder/tableref/bind_showref.cpp": 3,
    f"{PLANNER}/operator/logical_copy_to_file.cpp": 1,
    f"{PLANNER}/operator/logical_create.cpp": 1,
    f"{PLANNER}/operator/logical_create_index.cpp": 2,
    f"{PLANNER}/operator/logical_create_table.cpp": 1,
    f"{PLANNER}/operator/logical_delete.cpp": 1,
    f"{PLANNER}/operator/logical_export.cpp": 1,
    f"{PLANNER}/operator/logical_insert.cpp": 1,
    f"{PLANNER}/operator/logical_merge_into.cpp": 1,
    f"{PLANNER}/operator/logical_update.cpp": 1,
    f"{PLANNER}/statement_preprocessor.cpp": 2,
}


def direct_lookups(path):
    with open(path, encoding="utf-8") as source:
        text = source.read()
    return [
        text.count("\n", 0, match.start()) + 1
        for match in DIRECT_LOOKUP.finditer(text)
        if match.group(1) not in ROUTED_ARGUMENTS
    ]


def main():
    planner = os.path.join(ROOT, PLANNER)
    if not os.path.isdir(planner):
        return 0
    failed = False
    for directory, _, files in os.walk(planner):
        for name in sorted(files):
            if not name.endswith(".cpp"):
                continue
            path = os.path.join(directory, name)
            relative = os.path.relpath(path, ROOT)
            lines = direct_lookups(path)
            allowed = ALLOWED.get(relative, 0)
            if len(lines) > allowed:
                failed = True
                where = ", ".join(str(line) for line in lines)
                print(
                    f"{relative}: {len(lines)} direct catalog lookups (lines {where}), "
                    f"{allowed} allowed: resolve names through the binder's entry_retriever, "
                    f"which checks schema USAGE and cross-database CONNECT, or raise the "
                    f"count in {os.path.relpath(__file__, ROOT)} for an internal lookup",
                    file=sys.stderr,
                )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
