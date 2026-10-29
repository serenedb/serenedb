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
    assert summary == {"lines": [10, 10], "functions": [3, 3], "branches": [4, 7], "mcdc": [0, 2]}
    assert sorted((d["kind"], d.get("direction") or d["condition"],
                   (d.get("condition_source") or d["source"]).strip()) for d in details) == [
        ("branch", "false", "if (x > 0 && y > 0) {"),
        ("branch", "false", "if (x > 0 && y > 0) {"),
        ("branch", "false", "return sdb::coverage::Work(argc, argc) == 1 ? 0 : 1;"),
        ("mcdc", 1, "x > 0"),
        ("mcdc", 2, "y > 0"),
    ]


def test_each_uncovered_instantiation_is_listed_under_its_own_name(monkeypatch):
    plain = {"_Z1fIiEvv": "void f<int>()", "_Z1fIlEvv": "void f<long>()"}
    monkeypatch.setattr(diff, "demangle", lambda names: {n: plain.get(n, n) for n in names})
    data = diff.parse_lcov("SF:x.cpp\nFN:3,x.cpp:_Z1fIiEvv\nFN:3,_Z1fIlEvv\n"
                           "FNDA:0,x.cpp:_Z1fIiEvv\nFNDA:0,_Z1fIlEvv\nend_of_record\n")["x.cpp"]
    spans = {"mcdc": {}, "functions": [(3, 5, "void f<int>()", "_Z1fIiEvv")], "excluded": set(),
             "code": []}
    summary = {"lines": [0, 0], "functions": [0, 0], "branches": [0, 0], "mcdc": [0, 0]}
    details = []
    diff.cover("x.cpp", {3}, data, spans, summary, details)
    names = diff.readable({d["function"] for d in details})
    assert sorted(names[d["function"]] for d in details) == ["void f<long>()", "x.cpp:void f<int>()"]


def test_a_switch_counts_only_whether_each_case_ran(monkeypatch, tmp_path):
    (tmp_path / "s.cpp").write_text("int f(int x) {\n  switch (x) {\n    case 1:\n      return 1;\n"
                                    "    default:\n      return 2;\n  }\n}\n")
    monkeypatch.setattr(diff, "ROOT", tmp_path)
    diff.source_lines.cache_clear()
    data = diff.parse_lcov("SF:s.cpp\nBRDA:2,0,0,0\nBRDA:2,0,1,1\nBRDA:3,0,0,1\nBRDA:3,0,1,0\n"
                           "BRDA:5,0,0,0\nBRDA:5,0,1,0\nend_of_record\n")["s.cpp"]
    spans = {"mcdc": {}, "functions": [], "excluded": set(), "code": []}
    summary = {"lines": [0, 0], "functions": [0, 0], "branches": [0, 0], "mcdc": [0, 0]}
    details = []
    diff.cover("s.cpp", {2, 3, 5}, data, spans, summary, details)
    diff.source_lines.cache_clear()
    assert summary["branches"] == [1, 2]
    assert [(d["line"], d["direction"]) for d in details] == [(5, "true")]


def test_a_decision_that_recorded_no_test_vector_is_not_counted(monkeypatch, tmp_path):
    (tmp_path / "m.cpp").write_text("int f(int k) {\n  return k == 1 || k == 2 ? 0 : 1;\n}\n"
                                    "int g(int k) {\n  if (k == 1 || k == 2) {\n    return 0;\n"
                                    "  }\n  return 1;\n}\n")
    monkeypatch.setattr(diff, "ROOT", tmp_path)
    diff.source_lines.cache_clear()
    data = diff.parse_lcov("SF:m.cpp\nend_of_record\n")["m.cpp"]
    executed = {"executed": True, "conditions": [False, False], "result": False}
    spans = {"functions": [], "excluded": set(), "code": [], "mcdc": {
        "ternary": {"decision": [2, 10, 2, 26], "site": None,
                    "conditions": [[2, 10, 2, 16], [2, 20, 2, 26]], "shown": [False, False],
                    "vectors": []},
        "if": {"decision": [5, 7, 5, 23], "site": None,
               "conditions": [[5, 7, 5, 13], [5, 17, 5, 23]], "shown": [False, False],
               "vectors": [executed]},
    }}
    summary = {"lines": [0, 0], "functions": [0, 0], "branches": [0, 0], "mcdc": [0, 0]}
    details = []
    diff.cover("m.cpp", set(range(1, 10)), data, spans, summary, details)
    diff.source_lines.cache_clear()
    assert summary["mcdc"] == [0, 2]
    assert [(d["line"], d["condition"]) for d in details] == [(5, 1), (5, 2)]


def test_an_unreachable_marker_excludes_only_what_leads_to_it(monkeypatch, tmp_path):
    (tmp_path / "u.cpp").write_text("int Kind(int k) {\n  switch (k) {\n    case 0:\n      return 1;\n"
                                    "  }\n  SDB_UNREACHABLE();\n}\nvoid Stub() {\n"
                                    "  SDB_UNREACHABLE();\n}\n")
    monkeypatch.setattr(diff, "ROOT", tmp_path)
    diff.source_lines.cache_clear()
    code = [[1, 17, 7, 2, 0, 0, 0, 0], [3, 5, 4, 15, 0, 0, 0, 0], [6, 20, 7, 2, 0, 0, 0, 0],
            [8, 13, 10, 2, 0, 0, 0, 0]]
    lines = diff.unreachable("u.cpp", set(range(1, 11)), code)
    diff.source_lines.cache_clear()
    assert lines == {6, 7, 8, 9, 10}


def test_a_closing_brace_and_the_exit_of_an_endless_loop_are_not_coverage_targets(monkeypatch,
                                                                                  tmp_path):
    (tmp_path / "b.cpp").write_text("int f(int x) {\n  while (true) {\n    if (x > 3) {\n"
                                    "      return x;\n    }\n    ++x;\n  }\n}\n")
    monkeypatch.setattr(diff, "ROOT", tmp_path)
    diff.source_lines.cache_clear()
    data = diff.parse_lcov("SF:b.cpp\nDA:2,1\nDA:3,1\nDA:4,1\nDA:5,0\nDA:6,1\nDA:7,1\nDA:8,0\n"
                           "BRDA:2,0,0,1\nBRDA:2,0,1,0\nBRDA:3,0,0,1\nBRDA:3,0,1,1\n"
                           "end_of_record\n")["b.cpp"]
    spans = {"mcdc": {}, "functions": [], "excluded": set(), "code": []}
    summary = {"lines": [0, 0], "functions": [0, 0], "branches": [0, 0], "mcdc": [0, 0]}
    details = []
    diff.cover("b.cpp", set(range(1, 9)), data, spans, summary, details)
    diff.source_lines.cache_clear()
    assert summary["lines"] == [4, 4]
    assert summary["branches"] == [3, 3]
    assert details == []


def test_a_using_declaration_is_not_a_coverage_target(monkeypatch, tmp_path):
    (tmp_path / "e.cpp").write_text("int f(E e) {\n  switch (e) {\n    using enum E;\n    case kA:\n"
                                    "      return 1;\n    default:\n      return 2;\n  }\n}\n")
    monkeypatch.setattr(diff, "ROOT", tmp_path)
    diff.source_lines.cache_clear()
    data = diff.parse_lcov("SF:e.cpp\nDA:1,1\nDA:2,1\nDA:3,0\nDA:4,1\nDA:5,1\nDA:6,1\nDA:7,1\n"
                           "end_of_record\n")["e.cpp"]
    spans = {"mcdc": {}, "functions": [], "excluded": set(), "code": []}
    summary = {"lines": [0, 0], "functions": [0, 0], "branches": [0, 0], "mcdc": [0, 0]}
    details = []
    diff.cover("e.cpp", set(range(1, 10)), data, spans, summary, details)
    diff.source_lines.cache_clear()
    assert summary["lines"] == [6, 6]
    assert details == []


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
