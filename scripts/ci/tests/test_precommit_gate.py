import importlib.util
import os
import subprocess
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github" / "workflows" / "simple.yml"


def load_ci():
    spec = importlib.util.spec_from_file_location("ci_under_test", ROOT / "scripts" / "ci" / "ci.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def step_lines():
    return WORKFLOW.read_text().splitlines()


def step_index(marker):
    return next(i for i, line in enumerate(step_lines()) if marker in line)


def gitlink_step_script():
    lines = step_lines()
    start = step_index("name: Check submodule gitlinks")
    run = next(i for i in range(start, len(lines)) if lines[i].strip() == "run: |")
    indent = len(lines[run]) - len(lines[run].lstrip()) + 2
    body = []
    for line in lines[run + 1:]:
        if line.strip() and len(line) - len(line.lstrip()) < indent:
            break
        body.append(line[indent:])
    return "\n".join(body) + "\n"


def test_the_job_fetches_every_gitlink_before_the_hooks_and_reports_pending_after_them():
    reachable = step_index("check_submodule_pointers.py --reachable")
    hooks = step_index("uses: pre-commit/action")
    gitlinks = step_index("name: Check submodule gitlinks")
    assert reachable < hooks < gitlinks
    assert "SKIP: check-submodule-pointers" in "\n".join(step_lines()[hooks:gitlinks])


@pytest.fixture()
def gate(tmp_path):
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "check_submodule_pointers.py").write_text(
        "import os, sys\nprint(os.environ['STUB_LINE'])\nsys.exit(int(os.environ['STUB_RC']))\n")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    gh = bin_dir / "gh"
    gh.write_text('#!/bin/bash\necho "$*" >> "$GH_LOG"\n'
                  'if [[ "$*" == *"/status --jq"* ]]; then printf "%s\\n" $STUB_CONTEXTS; fi\n')
    gh.chmod(0o755)
    script = tmp_path / "step.sh"
    script.write_text(gitlink_step_script())

    def run(rc, line="", from_fork="false", contexts="", head_sha="abc123"):
        log = tmp_path / "gh.log"
        log.unlink(missing_ok=True)
        env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", GH_LOG=str(log),
                   STUB_RC=str(rc), STUB_LINE=line, STUB_CONTEXTS=contexts, HEAD_SHA=head_sha,
                   FROM_FORK=from_fork, RUN_URL="https://example.invalid/run",
                   GITHUB_REPOSITORY="serenedb/serenedb")
        res = subprocess.run(["bash", "-eo", "pipefail", str(script)], cwd=tmp_path, env=env,
                             capture_output=True, text=True)
        return res.returncode, log.read_text() if log.exists() else ""

    return run


PENDING = ("third_party/duckdb: gitlink 1234 is pending: head of open serenedb/duckdb#211 "
           "into v2026.10.06")


def test_a_gitlink_at_an_open_fork_pr_head_posts_a_pending_status(gate):
    rc, log = gate(3, PENDING)
    assert rc == 0
    assert "-f state=pending" in log
    assert "Waiting for serenedb/duckdb#211" in log


def test_a_pending_gitlink_from_a_forked_pr_fails_the_job(gate):
    rc, log = gate(3, PENDING, from_fork="true")
    assert rc == 1
    assert "statuses/abc123" not in log


def test_a_gitlink_off_every_version_branch_fails_without_a_status(gate):
    rc, log = gate(1, "third_party/duckdb: gitlink 1234 not reachable from any version branch")
    assert rc == 1
    assert log == ""


def test_landed_gitlinks_turn_an_earlier_pending_status_green(gate):
    rc, log = gate(0, contexts="pre-commit")
    assert rc == 0
    assert "-f state=success" in log


def test_landed_gitlinks_post_nothing_when_no_status_is_pending(gate):
    rc, log = gate(0, contexts="")
    assert rc == 0
    assert "statuses/abc123" not in log


def check_runs(ci, monkeypatch, answers):
    calls = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout=answers[min(len(calls), len(answers)) - 1])

    monkeypatch.setattr(ci.subprocess, "run", fake_run)
    monkeypatch.setattr(ci.time, "sleep", lambda seconds: None)
    return calls


def wait_args(timeout=600.0):
    return Namespace(repo="serenedb/serenedb", sha="abc123", name="pre-commit", timeout=timeout)


def test_build_and_test_starts_once_pre_commit_succeeds(monkeypatch):
    ci = load_ci()
    calls = check_runs(ci, monkeypatch, ["", "in_progress\t", "completed\tsuccess"])
    ci.cmd_wait_check(wait_args())
    assert len(calls) == 3


def test_build_and_test_does_not_start_after_a_red_pre_commit(monkeypatch):
    ci = load_ci()
    check_runs(ci, monkeypatch, ["completed\tfailure"])
    with pytest.raises(SystemExit, match="concluded failure"):
        ci.cmd_wait_check(wait_args())


def test_build_and_test_gives_up_when_pre_commit_never_finishes(monkeypatch):
    ci = load_ci()
    check_runs(ci, monkeypatch, ["in_progress\t"])
    with pytest.raises(SystemExit, match="did not finish"):
        ci.cmd_wait_check(wait_args(timeout=0.0))
