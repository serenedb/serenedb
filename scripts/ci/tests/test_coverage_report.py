import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
COVERAGE = Path(__file__).resolve().parent / "coverage"
sys.path[:0] = [str(ROOT / "scripts" / "ci"), str(ROOT / "scripts" / "coverage")]

import diff  # noqa: E402
import report  # noqa: E402


def test_assertions_verification_and_unreachable_code_are_not_coverage_targets(monkeypatch):
    monkeypatch.setattr(diff, "ROOT", COVERAGE)
    monkeypatch.setattr(diff, "demangle", lambda names: {})
    diff.source_lines.cache_clear()
    spans = diff.parse_json((COVERAGE / "asserts.json").read_text())["asserts.cpp"]
    data = diff.parse_lcov((COVERAGE / "asserts.lcov").read_text())["asserts.cpp"]
    summary = {"lines": [0, 0], "functions": [0, 0], "branches": [0, 0], "mcdc": [0, 0]}
    details = []
    diff.cover("asserts.cpp", set(range(1, 200)), data, spans, summary, details)
    diff.source_lines.cache_clear()
    assert summary == {"lines": [14, 14], "functions": [3, 3], "branches": [4, 8], "mcdc": [0, 2]}
    assert sorted((d["kind"], d.get("direction") or d["condition"],
                   (d.get("condition_source") or d["source"]).strip()) for d in details) == [
        ("branch", "false", "case 1:"),
        ("branch", "false", "if (x > 0 && y > 0) {"),
        ("branch", "false", "if (x > 0 && y > 0) {"),
        ("branch", "false", "return sdb::coverage::Work(argc, argc) == 1 ? 0 : 1;"),
        ("mcdc", 1, "x > 0"),
        ("mcdc", 2, "y > 0"),
    ]


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


def test_uncovered_items_are_listed_with_their_code():
    coverage = [{"summary": "coverage dev/x86_64: lines 1/2", "uncovered": [
        {"kind": "branch", "file": "server/a.cpp", "line": 3, "direction": "false",
         "source": "  if (x || y) {"},
        {"kind": "mcdc", "file": "server/a.cpp", "line": 3, "condition": 2,
         "condition_source": "y", "source": "unused"},
    ]}]
    body, _ = report.build_report([], [], {}, "https://example.com/runs/1", coverage)
    assert "coverage dev/x86_64: lines 1/2" in body
    assert "| `server/a.cpp:3` | branch never false | `if (x \\|\\| y) {` |" in body
    assert "| `server/a.cpp:3` | MC/DC condition 2 | `y` |" in body
