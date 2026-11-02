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


def step_script(name):
    lines = step_lines()
    start = step_index(f"name: {name}")
    run = next(i for i in range(start, len(lines)) if lines[i].strip() == "run: |")
    indent = len(lines[run]) - len(lines[run].lstrip()) + 2
    body = []
    for line in lines[run + 1:]:
        if line.strip() and len(line) - len(line.lstrip()) < indent:
            break
        body.append(line[indent:])
    return "\n".join(body) + "\n"


def test_one_gitlink_check_runs_before_the_hooks_and_the_family_checks_around_them():
    gitlinks = step_index("check_submodule_pointers.py --reachable --rev HEAD --base HEAD^1")
    fetch = step_index("duckdb_family.sh fetch HEAD^1")
    hooks = step_index("name: Run the hooks")
    check = step_index("duckdb_family.sh check HEAD^1")
    assert gitlinks < fetch < hooks < check
    assert "".join(step_lines()).count("check_submodule_pointers.py") == 1


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
    script.write_text(step_script("Check submodule gitlinks"))

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


@pytest.mark.parametrize("event, changed, all_files", [
    ("pull_request", "server/a.cpp", False),
    ("pull_request", "scripts/ci/ci.py", False),
    ("pull_request", ".pre-commit-config.yaml", True),
    ("pull_request", "scripts/check_banned_calls.py", True),
    ("pull_request", ".clang-format", True),
    ("pull_request", "iresearch/.clang-format", True),
    ("pull_request", ".github/actionlint.yaml", True),
    ("push", "server/a.cpp", True),
])
def test_the_hooks_see_the_change_staged_and_every_file_only_when_their_rules_change_or_on_main(
        tmp_path, event, changed, all_files):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    for name in ("README", changed):
        (repo / name).parent.mkdir(parents=True, exist_ok=True)
        (repo / name).write_text(name)
        subprocess.run(["git", "add", name], cwd=repo, check=True)
        subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.com", "commit",
                        "-q", "-m", name], cwd=repo, check=True)
    head, base = subprocess.run(["git", "rev-parse", "HEAD", "HEAD^1"], cwd=repo, check=True,
                                capture_output=True, text=True).stdout.split()
    stub = tmp_path / "home" / ".cache" / "pre-commit-venv" / "bin" / "pre-commit"
    stub.parent.mkdir(parents=True)
    stub.write_text('#!/bin/bash\n[[ "$1" == run ]] || exit 0\n'
                    'echo "$* | $(git rev-parse HEAD) | $(git diff --cached --name-only)" > "$PRE_COMMIT_LOG"\n')
    stub.chmod(0o755)
    log = tmp_path / "pre-commit.log"
    script = tmp_path / "step.sh"
    script.write_text(step_script("Run the hooks on the change, staged over HEAD^1"))
    env = dict(os.environ, HOME=str(tmp_path / "home"), EVENT=event, PRE_COMMIT_LOG=str(log))
    subprocess.run(["bash", "-eo", "pipefail", str(script)], cwd=repo, env=env, check=True,
                   capture_output=True)
    args = "run --show-diff-on-failure --color=always" + (" --all-files" if all_files else "")
    assert log.read_text().strip() == f"{args} | {base} | {changed}"
    after = subprocess.run(["git", "status", "--porcelain"], cwd=repo, capture_output=True,
                           text=True).stdout
    assert after == ""
    assert subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True,
                          text=True).stdout.strip() == head


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
