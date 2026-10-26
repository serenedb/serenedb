import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts" / "ci")]

import report  # noqa: E402


def shard(config):
    return {"config": config, "arch": "x86_64", "check": "", "duration": 60, "shard": "1/1",
            "plan": "p"}


def unit(config, status):
    return {"unit": f"{config}:u", "config": config, "arch": "x86_64", "check": "", "status": status,
            "attempt": 0, "member_of": "", "wrapper": False, "detail": "", "signature": ""}


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
    assert "- ❌ **image**: failure before any test result" in body
    assert "**perf**:" not in body
