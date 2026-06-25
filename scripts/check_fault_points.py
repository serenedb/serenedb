#!/usr/bin/env python3
"""Verify fault-injection points are consistent between sqllogic tests and C++.

This merges and replaces check_no_fault_injection_outside_recovery.py. It runs
three checks over the whole tree (it cross-references the entire repo, so it must
not rely on the per-file argv list pre-commit passes; see pass_filenames: false
in .pre-commit-config.yaml):

  1. Faults only in recovery: any `SET sdb_faults = '...'` in a .test/.test_slow
     outside tests/sqllogic/recovery/ is an error.

  2. Test -> Source: every fault NAME activated in a recovery .test must be
     defined in C++ source (SDB_IF_FAILURE / WaitWhileFailurePointDebugging
     string literal in server/ or libs/).

  3. Source -> Test: every fault NAME defined in C++ source must be exercised by
     at least one test (a recovery .test, or a C++/Python test under tests/).
"""

import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RECOVERY_DIR = "tests/sqllogic/recovery"
SOURCE_DIRS = ("server", "libs")
TEST_DIR = "tests"

TEST_EXTS = (".test", ".test_slow")
SOURCE_EXTS = (".cpp", ".cc", ".hpp", ".hh", ".h", ".ipp", ".tpp")
# Test net for check #3: gtests, header-only test helpers, and the python
# driver tests that drive faults over the wire via `set sdb_faults='...'`.
TESTNET_EXTS = (".cpp", ".cc", ".hpp", ".hh", ".test", ".test_slow", ".py")

# Activate/deactivate: SET [LOCAL|SESSION] sdb_faults = '[-]NAME'
# The leading '-' deactivates and is not part of the name. RESET clears all.
SET_FAULT = re.compile(
    r"SET\s+(?:(?:LOCAL|SESSION)\s+)?sdb_faults\s*=\s*'(-?)([^']*)'",
    re.IGNORECASE,
)
# Just the `SET ... sdb_faults` head, for the recovery-only location check.
SET_FAULT_HEAD = re.compile(r"SET\s+(?:(?:LOCAL|SESSION)\s+)?sdb_faults\b", re.IGNORECASE)

# Source-side definitions: SDB_IF_FAILURE("NAME") / WaitWhile...("NAME").
SOURCE_DEF = re.compile(
    r'(?:SDB_IF_FAILURE|WaitWhileFailurePointDebugging)\s*\(\s*"([^"]+)"'
)
# Names referenced by C++/python tests (string literal in any of these calls,
# or a SET sdb_faults activation).
TESTNET_LITERAL = re.compile(
    r'(?:SDB_IF_FAILURE|AddFailurePointDebugging|ShouldFailDebugging|'
    r'WaitWhileFailurePointDebugging)\s*\(\s*"([^"]+)"'
)

# faults.test is the test of the fault FRAMEWORK itself; its names (some1, some2,
# some3) are synthetic and intentionally have no source definition.
FRAMEWORK_TEST = os.path.join(RECOVERY_DIR, "faults.test")

# Recovery-test fault names whose source definition is a KNOWN pending gap: the
# old code built these dynamically (e.g. StrCat(table->GetName(),
# "_connector_must_one_index")) and the new code has not re-implemented them yet.
# Tracked in https://github.com/serenedb/serenedb/issues/847. Remove entries
# here as the corresponding SDB_IF_FAILURE points land.
KNOWN_MISSING_SOURCE_FAULTS = {
    "idx_update_pk_connector_must_one_index",
    "idx_update_pk_connector_must_two_index",
    "idx_update_nopk_connector_must_one_index",
    "idx_update_nopk_connector_must_two_index",
}

# Source faults that no test exercises yet. Each is a deliberate, documented
# gap rather than a silent skip; remove an entry once a test covers it.
KNOWN_UNTESTED_SOURCE_FAULTS = {
    # Search commit crash/error injection points; recovery coverage pending.
    "Search::CrashAfterCommit",
    "Search::FailOnCommit",
    # Search compaction task fault points; no recovery test drives compaction yet.
    "SearchCompactionTask::compactUnsafe",
    "SearchCompactionTask::lockInvertedIndexStorage",
    # Search refresh task fault points; cleanup/lock paths untested.
    "SearchRefreshTask::cleanupUnsafe",
    "SearchRefreshTask::lockInvertedIndexStorage",
    # Truncate failure injection; no test drives a failed search truncate yet.
    "SereneSearchTruncateFailure",
    # Crash before the drop-task wipes the sequence counter; recovery test pending.
    "crash_before_seq_counter_wipe",
}


def walk_files(rel_dir, exts):
    base = os.path.join(ROOT, rel_dir)
    for dirpath, _, filenames in os.walk(base):
        for fn in filenames:
            if fn.endswith(exts):
                yield os.path.join(dirpath, fn)


def read(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()


def rel(path):
    return os.path.relpath(path, ROOT).replace("\\", "/")


def lineno(text, pos):
    return text.count("\n", 0, pos) + 1


def main():
    errors = []

    # ----- collect source-side definitions -----
    # Substring pre-filters gate the regex: the bulk of the tree (notably the
    # ~630MB of sqlite-derived .test_slow files) contains no fault points, and a
    # plain `in` is far cheaper than running finditer over every byte.
    source_faults = {}  # name -> "path:line"
    for path in (p for d in SOURCE_DIRS for p in walk_files(d, SOURCE_EXTS)):
        text = read(path)
        if "FAIL" not in text and "Fail" not in text:
            continue
        for m in SOURCE_DEF.finditer(text):
            source_faults.setdefault(m.group(1), f"{rel(path)}:{lineno(text, m.start())}")

    # ----- single pass over tests/ (checks #1 and the #3 test net) -----
    test_faults = {}  # name -> "path:line" (recovery activations only)
    exercised = set()  # any test reference, for check #3
    for path in walk_files(TEST_DIR, TESTNET_EXTS):
        text = read(path)
        has_set = "sdb_faults" in text
        has_lit = "FAIL" in text or "Fail" in text
        if not has_set and not has_lit:
            continue
        normalized = rel(path)
        is_recovery = normalized.startswith(RECOVERY_DIR + "/")
        is_sqllogic = path.endswith(TEST_EXTS)
        if has_set:
            # check #1: SET sdb_faults only belongs under recovery/ (.test files).
            if is_sqllogic and not is_recovery:
                for m in SET_FAULT_HEAD.finditer(text):
                    errors.append(
                        f"{normalized}:{lineno(text, m.start())}: "
                        "'SET sdb_faults' must live under tests/sqllogic/recovery/ "
                        "(move this test to the recovery folder)"
                    )
            for m in SET_FAULT.finditer(text):
                name = m.group(2)
                if not name:  # RESET-like empty activation
                    continue
                exercised.add(name)
                if is_recovery and is_sqllogic:
                    test_faults.setdefault(name, f"{normalized}:{lineno(text, m.start())}")
        if has_lit:
            for m in TESTNET_LITERAL.finditer(text):
                exercised.add(m.group(1))

    # ----- check #2: test -> source -----
    for name, loc in sorted(test_faults.items()):
        if FRAMEWORK_TEST.replace("\\", "/") in loc:
            continue  # framework test: synthetic names by design
        if name in source_faults:
            continue
        if name in KNOWN_MISSING_SOURCE_FAULTS:
            continue  # documented pending gap, issue #847
        errors.append(
            f"{loc}: fault '{name}' is activated by a test but is not defined in "
            "C++ source (no SDB_IF_FAILURE/WaitWhileFailurePointDebugging literal "
            "in server/ or libs/)"
        )

    # ----- check #3: source -> test -----
    for name, loc in sorted(source_faults.items()):
        if name in exercised:
            continue
        if name in KNOWN_UNTESTED_SOURCE_FAULTS:
            continue
        errors.append(
            f"{loc}: fault '{name}' is defined in C++ source but no test exercises "
            "it (add a recovery .test or a gtest reference)"
        )

    for e in errors:
        print(e, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
