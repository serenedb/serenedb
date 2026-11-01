import importlib.util
import os
import subprocess
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github" / "workflows" / "simple.yml"
STATUS_WORKFLOW = ROOT / ".github" / "workflows" / "gitlink-status.yml"


def load_ci():
    spec = importlib.util.spec_from_file_location("ci_under_test", ROOT / "scripts" / "ci" / "ci.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def step_lines(workflow=WORKFLOW):
    return workflow.read_text().splitlines()


def step_index(marker, workflow=WORKFLOW):
    return next(i for i, line in enumerate(step_lines(workflow)) if marker in line)


def step_script(name, workflow=WORKFLOW):
    lines = step_lines(workflow)
    start = step_index(f"name: {name}", workflow)
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


def stub_check(root):
    (root / "scripts").mkdir()
    (root / "scripts" / "check_submodule_pointers.py").write_text(
        "import os, sys\nprint(os.environ['STUB_LINE'])\n"
        "open('check.log', 'w').write(' '.join(sys.argv[1:]))\n"
        "sys.exit(int(os.environ['STUB_RC']))\n")


@pytest.fixture()
def gate(tmp_path):
    stub_check(tmp_path)
    script = tmp_path / "step.sh"
    script.write_text(step_script("Check submodule gitlinks"))

    def run(rc, event):
        env = dict(os.environ, STUB_RC=str(rc), STUB_LINE="", EVENT=event)
        return subprocess.run(["bash", "-eo", "pipefail", str(script)], cwd=tmp_path, env=env,
                              capture_output=True, text=True).returncode

    return run


@pytest.mark.parametrize("rc, event, passes", [
    (0, "pull_request", True),
    (3, "pull_request", True),
    (1, "pull_request", False),
    (0, "push", True),
    (3, "push", False),
])
def test_a_gitlink_at_an_open_fork_pr_head_passes_any_pr_but_not_main(gate, rc, event, passes):
    assert (gate(rc, event) == 0) == passes


HEAD = "a" * 40
BASE = "b" * 40
PENDING = (f"third_party/duckdb: gitlink {'c' * 40} is pending: head of open serenedb/duckdb#193 "
           "into v2026.10.06")


@pytest.fixture()
def status(tmp_path):
    stub_check(tmp_path)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "gh").write_text('#!/bin/bash\necho "gh $*" >> "$CALLS"\n'
                                'if [[ "$*" == *"/compare/main..."* ]]; then echo "$STUB_BASE"; fi\n'
                                'if [[ "$*" == *"/status --jq"* ]]; then printf "%s\\n" $STUB_CONTEXTS; fi\n')
    (bin_dir / "git").write_text('#!/bin/bash\necho "git $*" >> "$CALLS"\n')
    for tool in bin_dir.iterdir():
        tool.chmod(0o755)
    script = tmp_path / "step.sh"
    script.write_text(step_script("Post the pre-commit status of the gitlinks", STATUS_WORKFLOW))

    def run(rc, line="", event="workflow_run", contexts="", head_sha=HEAD):
        calls = tmp_path / "calls.log"
        calls.unlink(missing_ok=True)
        env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", CALLS=str(calls),
                   STUB_RC=str(rc), STUB_LINE=line, STUB_CONTEXTS=contexts, STUB_BASE=BASE,
                   EVENT=event, HEAD_SHA=head_sha, RUN_URL="https://example.invalid/run",
                   GITHUB_REPOSITORY="serenedb/serenedb")
        res = subprocess.run(["bash", "-eo", "pipefail", str(script)], cwd=tmp_path, env=env,
                             capture_output=True, text=True)
        return res.returncode, calls.read_text() if calls.exists() else ""

    return run


def test_a_gitlink_at_an_open_fork_pr_head_gets_a_pending_status_on_the_pr_head(status, tmp_path):
    rc, calls = status(3, PENDING)
    assert rc == 0
    assert f"git fetch --quiet --no-tags --depth=1 origin {HEAD} {BASE}" in calls
    assert (tmp_path / "check.log").read_text() == f"--rev {HEAD} --base {BASE}"
    assert f"statuses/{HEAD} -f state=pending -f context=pre-commit" in calls
    assert "Waiting for serenedb/duckdb#193" in calls


def test_landed_gitlinks_turn_an_earlier_pending_status_green(status):
    rc, calls = status(0, contexts="pre-commit")
    assert rc == 0
    assert f"statuses/{HEAD} -f state=success" in calls


def test_landed_gitlinks_post_nothing_when_no_status_is_pending(status):
    rc, calls = status(0)
    assert rc == 0
    assert f"statuses/{HEAD}" not in calls


def test_a_run_land_sh_dispatches_posts_success_without_a_pending_status(status):
    rc, calls = status(0, event="workflow_dispatch")
    assert rc == 0
    assert f"statuses/{HEAD} -f state=success" in calls


def test_a_gitlink_off_every_version_branch_posts_no_status(status):
    rc, calls = status(1, "third_party/duckdb: gitlink 1234 not reachable from any version branch")
    assert rc == 1
    assert f"statuses/{HEAD}" not in calls


def test_a_dispatch_with_anything_but_a_commit_hash_does_nothing(status):
    rc, calls = status(0, event="workflow_dispatch", head_sha="main")
    assert rc == 1
    assert calls == ""


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
