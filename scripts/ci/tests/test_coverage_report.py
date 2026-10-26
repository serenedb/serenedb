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


def run_report(monkeypatch, files, spans, covering, units):
    monkeypatch.setattr(selection.diff, "changed_lines", lambda base: files)
    monkeypatch.setattr(selection, "covering_units", lambda map_path, functions, work: covering)
    monkeypatch.setattr(selection.subprocess, "run",
                        lambda *args, **kwargs: SimpleNamespace(stdout=""))
    return selection.report([SimpleNamespace(id=u, cost=1.0) for u in units], "origin/main",
                            sorted(files), [], "map.parquet", spans, ".")


def test_changed_cpp_test_code_selects_the_units_that_run_it(monkeypatch):
    path = "tests/server/connector/pg_progress_registry_test.cpp"
    result = run_report(
        monkeypatch, {path: {10}}, {path: [[5, 20, "Progress"]]},
        {"Progress": [{"unit": "gtest:serenedb-tests:Progress", "covering": 1, "rank": 1}]},
        ["gtest:serenedb-tests:Progress", "sqllogic:pg-wire-simple:sdb/a.test"])
    assert result["mode"] == "selected"
    assert result["reasons"] == {"gtest:serenedb-tests:Progress": [f"{path}:10"]}


def test_a_line_covered_only_by_units_missing_from_the_plan_runs_everything(monkeypatch):
    path = "server/a.cpp"
    result = run_report(
        monkeypatch, {path: {3}}, {path: [[1, 9, "F"]]},
        {"F": [{"unit": "duckdb:core:batch:old.test+7", "covering": 1, "rank": 1}]},
        ["duckdb:core:batch:new.test+7"])
    assert result["mode"] == "everything"
    assert result["everything_because"] == [f"{path}:3"]


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
