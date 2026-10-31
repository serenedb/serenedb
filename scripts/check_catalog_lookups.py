#!/usr/bin/env python3
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLANNER = "third_party/duckdb/src/planner"
SERVER = "server"
DIRECT_LOOKUP = re.compile(r"Catalog::(?:GetEntry(?:<[\w:]+>)?|GetSchema)\(\s*([^,()]*)")
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
    f"{SERVER}/connector/copy_from_batches.cpp": 1,
    f"{SERVER}/connector/duckdb_physical_create_index.cpp": 1,
    f"{SERVER}/connector/functions/jobs.cpp": 1,
    f"{SERVER}/connector/index_source_external_lookup.cpp": 1,
    f"{SERVER}/connector/index_source_native.cpp": 1,
    f"{SERVER}/connector/iresearch_replacement_scan.cpp": 1,
    f"{SERVER}/connector/scan/scan_bind.cpp": 1,
    f"{SERVER}/connector/view_fast_path.cpp": 2,
    f"{SERVER}/docs/builder/docs_loader.cpp": 1,
    f"{SERVER}/network/pg/pg_wire_session.cpp": 1,
    f"{SERVER}/pg/catalog/engine/scan_function.cpp": 1,
    f"{SERVER}/pg/commands/create_subscription.cpp": 1,
    f"{SERVER}/replication/sync_session.cpp": 1,
}
ROUTE = {
    PLANNER: "resolve names through the binder's entry_retriever",
    SERVER: "resolve a name the client wrote through a binder's entry_retriever, or auth::Lookup outside a binder",
}


def direct_lookups(path):
    with open(path, encoding="utf-8") as source:
        text = source.read()
    return [
        text.count("\n", 0, match.start()) + 1
        for match in DIRECT_LOOKUP.finditer(text)
        if match.group(1).strip() not in ROUTED_ARGUMENTS
    ]


def main():
    failed = False
    for tree, route in ROUTE.items():
        top = os.path.join(ROOT, tree)
        if not os.path.isdir(top):
            continue
        for directory, _, files in os.walk(top):
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
                        f"{allowed} allowed: {route}, which checks schema USAGE and cross-database "
                        f"CONNECT, or raise the count in {os.path.relpath(__file__, ROOT)} for an "
                        f"internal lookup",
                        file=sys.stderr,
                    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
