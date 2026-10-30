import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts" / "ci")]

import report  # noqa: E402


def shard(config, arch="x86_64", check=""):
    return {"config": config, "arch": arch, "check": check, "duration": 60, "shard": "1/1",
            "plan": "p"}


def unit(config, status, arch="x86_64", check=""):
    return {"unit": f"{config}:u", "config": config, "arch": arch, "check": check, "status": status,
            "attempt": 0, "member_of": "", "wrapper": False, "detail": "", "signature": ""}


def entry(run, verdict):
    return {"run": run, "url": f"https://example.com/runs/{run}", "sha": f"sha{run}",
            "options": "default", "verdict": verdict, "time": f"t{run}"}


def test_the_comment_shows_the_latest_report_and_folds_the_earlier_runs():
    first = report.render_comment("### CI results · first\n", report.updated_history("", entry("1", "✅")))
    second = report.render_comment("### CI results · second\n",
                                   report.updated_history(first, entry("2", "❌ 1 flaky")))
    third = report.render_comment("### CI results · third\n",
                                  report.updated_history(second, entry("3", "✅")))
    assert third.startswith(report.MARKER)
    assert "### CI results · third" in third and "### CI results · second" not in third
    assert "<details><summary>Previous runs (2)</summary>" in third
    rows = [line for line in third.splitlines() if line.startswith("| [")]
    assert rows == ["| [2](https://example.com/runs/2) | `sha2` | default | ❌ 1 flaky | t2 |",
                    "| [1](https://example.com/runs/1) | `sha1` | default | ✅ | t1 |"]


def test_a_rerun_of_the_same_run_replaces_its_entry():
    first = report.render_comment("report\n", report.updated_history("", entry("1", "❌ 1 failed")))
    again = report.updated_history(first, entry("1", "✅"))
    assert [h["verdict"] for h in again] == ["✅"]


def test_the_history_keeps_the_newest_runs():
    comment = ""
    for run in range(report.KEEP_RUNS + 1):
        comment = report.render_comment("report\n", report.updated_history(comment, entry(str(run), "✅")))
    kept = report.updated_history(comment, entry("next", "✅"))
    assert [h["run"] for h in kept] == [str(r) for r in range(2, report.KEEP_RUNS + 1)] + ["next"]


def test_run_options_name_the_ticked_boxes():
    assert report.run_options({"LOCATION": "all", "EVERYTHING": True, "CORPUS": False,
                               "DUCKDB_SANITIZERS": "true", "PR_SHA": "abc"}) == (
        "duckdb sanitizers, everything, all architectures")
    assert report.run_options({"LOCATION": "linux-amd64", "BUGFIX": False}) == "default"
    assert report.run_options({"LOCATION": "dev-only", "CORPUS": True}) == "corpus, dev only"
    assert report.run_options({"LOCATION": "linux-amd64", "BUILD_CONFIG": '{"FLAKY_RUNS": 3}'}) == (
        "flaky check 3 runs")
    assert report.run_options({"LOCATION": "linux-amd64", "BUILD_CONFIG": '{"FLAKY_RUNS": 1}'}) == "default"


def test_a_failed_job_is_listed_only_when_its_test_lines_do_not_show_it():
    needs = {"asan": {"result": "failure"}, "dev": {"result": "failure"},
             "image": {"result": "failure"}, "perf": {"result": "success"}}
    body, _ = report.build_report([shard("asan"), shard("dev"), shard("perf")],
                                  [unit("asan", "flaky"), unit("dev", "passed"),
                                   unit("perf", "passed")],
                                  needs, "https://example.com/runs/1")
    assert "- ❌ **asan/x86_64**: 0 passed, 0 failed, 1 flaky" in body
    assert "**asan**:" not in body
    assert "- ❌ **dev**: failure outside the tests" in body
    assert "- ❌ **image**: failure\n" in body
    assert "**perf**:" not in body


def test_a_failed_job_finds_its_arm64_and_check_results():
    needs = {"perf-arm64": {"result": "failure"}, "bugfix": {"result": "failure"},
             "tsan": {"result": "failure"}}
    body, _ = report.build_report([shard("perf", "aarch64"), shard("dev", "x86_64", "bugfix")],
                                  [unit("perf", "failed", "aarch64"), unit("dev", "failed", "x86_64", "bugfix")],
                                  needs, "https://example.com/runs/1")
    assert "**perf-arm64**" not in body and "**bugfix**" not in body
    assert "- ❌ **tsan**: failure before any test result" in body


def test_the_coverage_comes_from_the_coverage_job_or_the_dev_results_and_never_a_test_summary(tmp_path):
    results = tmp_path / "results" / "results-dev-linux-amd64"
    (results / "test-dev-1of1").mkdir(parents=True)
    (results / "test-dev-1of1" / "summary.txt").write_text("dev/x86_64 shard 1/1: 9 passed")
    (results / "coverage-dev").mkdir()
    (results / "coverage-dev" / "summary.txt").write_text("coverage dev/x86_64: 90% lines")
    (results / "coverage-dev" / "coverage.json").write_text('{"uncovered": [{"kind": "line"}]}')
    found = report.load_coverage("", str(tmp_path / "results"))
    assert found == [{"summary": "coverage dev/x86_64: 90% lines", "uncovered": [{"kind": "line"}]}]
