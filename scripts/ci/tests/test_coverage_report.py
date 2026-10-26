import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts" / "ci"), str(ROOT / "scripts" / "coverage")]

import diff  # noqa: E402
import report  # noqa: E402
from testlib import selection  # noqa: E402


@pytest.mark.parametrize("path,test", [
    ("tests/server/connector/pg_progress_registry_test.cpp", True),
    ("resources/tests/iresearch/europarl.subset.txt", True),
    ("third_party/duckdb/test/common/test_async_file_writer.cpp", True),
    ("third_party/re2/re2/testing/backtrack.cc", True),
    ("server/connector/duckdb_client_state.cpp", False),
    ("third_party/duckdb/src/function/window/window_distinct_aggregator.cpp", False),
])
def test_is_test(path, test):
    assert diff.is_test(path) is test


def run_report(monkeypatch, files, spans, covering, units, batches=None, changed=None, claimed=()):
    monkeypatch.setattr(selection.diff, "changed_lines", lambda base, deletions: files)
    monkeypatch.setattr(selection, "covering_units", lambda map_path, functions, work: covering)
    monkeypatch.setattr(selection, "members_of", lambda map_path, found, work: {
        u: m for u, m in (batches or {}).items() if u in found})
    monkeypatch.setattr(selection.subprocess, "run",
                        lambda *args, **kwargs: SimpleNamespace(stdout=""))
    units = [SimpleNamespace(id=u, cost=1.0, members=members) for u, members in units.items()]
    return selection.report(units, "origin/main", sorted(changed or files), [], set(claimed),
                            "map.parquet", spans, ".")


def test_a_line_selects_the_current_batch_of_a_test_the_map_saw_in_another_batch(monkeypatch):
    path = "tests/server/connector/pg_progress_registry_test.cpp"
    result = run_report(
        monkeypatch, {path: {10}}, {path: [[5, 20, "Progress"]]},
        {"Progress": [{"unit": "gtest:serenedb-tests:batch:A.a+1", "covering": 1, "rank": 1}]},
        {"gtest:serenedb-tests:batch:B.b+1": ("gtest:serenedb-tests:B.b", "gtest:serenedb-tests:Progress.Runs"),
         "sqllogic:pg-wire-simple:sdb/a.test": ()},
        batches={"gtest:serenedb-tests:batch:A.a+1": ["gtest:serenedb-tests:A.a",
                                                      "gtest:serenedb-tests:Progress.Runs"]})
    assert result["mode"] == "selected"
    assert result["reasons"] == {"gtest:serenedb-tests:batch:B.b+1": [f"{path}:10"]}


def test_a_line_covered_only_by_units_missing_from_the_plan_runs_everything(monkeypatch):
    path = "server/a.cpp"
    result = run_report(
        monkeypatch, {path: {3}}, {path: [[1, 9, "F"]]},
        {"F": [{"unit": "duckdb:core:batch:old.test+7", "covering": 1, "rank": 1}]},
        {"duckdb:core:batch:new.test+7": ("duckdb:core:new.test",)},
        batches={"duckdb:core:batch:old.test+7": ["duckdb:core:old.test"]})
    assert result["mode"] == "everything"
    assert result["everything_because"] == [f"{path}:3"]


def test_a_changed_file_no_unit_claims_runs_everything_unless_nothing_reads_it(monkeypatch):
    harness = "tests/drivers/harness/services.py"
    changed = [harness, "tests/sqllogic/sdb/a.test", "README.md", ".claude/rules/ci.md"]
    result = run_report(monkeypatch, {}, {}, {}, {"sqllogic:pg-wire-simple:sdb/a.test": ()},
                        changed=changed, claimed={"tests/sqllogic/sdb/a.test"})
    assert result["everything_because"] == [harness]
    result = run_report(monkeypatch, {}, {}, {}, {"sqllogic:pg-wire-simple:sdb/a.test": ()},
                        changed=changed[1:], claimed={"tests/sqllogic/sdb/a.test"})
    assert result["mode"] == "selected"


def test_uncovered_items_are_listed_with_their_nearest_tests():
    coverage = [{"summary": "coverage dev/x86_64: lines 1/2", "uncovered": [
        {"kind": "branch", "file": "server/a.cpp", "line": 3, "direction": "false",
         "source": "  if (x || y) {", "tests": ["sqllogic:pg-wire-simple:sdb/a.test"]},
        {"kind": "mcdc", "file": "server/a.cpp", "line": 3, "condition": 2,
         "condition_source": "y", "source": "unused"},
    ]}]
    body, _ = report.build_report([], [], {}, "https://example.com/runs/1", coverage)
    assert "coverage dev/x86_64: lines 1/2" in body
    assert ("| `server/a.cpp:3` | branch never false | `if (x \\|\\| y) {` | "
            "`sqllogic:pg-wire-simple:sdb/a.test` |") in body
    assert "| `server/a.cpp:3` | MC/DC condition 2 | `y` | none in the map |" in body
