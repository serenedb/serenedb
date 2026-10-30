import importlib.util
import os
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2]
IDENTITY = ["-c", "user.name=test", "-c", "user.email=test@example.com"]
FORK = "serenedb/duckdb"
MAIN_GITLINK = "e" * 40
GITLINK = "1a5ede688591d57b203bd89dd40129ba7da2097b"
PENDING = (f"third_party/duckdb: gitlink {GITLINK} is pending: head of open {FORK}#193 "
           "into v2026.10.06")


def git(*args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True,
                          text=True).stdout.strip()


@pytest.fixture()
def pointers():
    spec = importlib.util.spec_from_file_location("pointers_under_test",
                                                  SCRIPTS / "check_submodule_pointers.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def pr(number, sha, head_repo=FORK, base="v2026.10.06"):
    return {"number": number, "state": "open", "head": {"sha": sha, "repo": {"full_name": head_repo}},
            "base": {"ref": base, "repo": {"full_name": FORK}}}


def github(pointers, monkeypatch, pages, compare=None):
    asked = []

    def api_get(path):
        asked.append(path)
        prefix = f"repos/{FORK}/pulls?state=open&per_page=100&page="
        if path.startswith(prefix):
            page = int(path.removeprefix(prefix))
            return 200, pages[page - 1] if page <= len(pages) else []
        if path.startswith(f"repos/{FORK}/compare/") and compare:
            return 200, {"status": compare[path.removeprefix(f"repos/{FORK}/compare/")]}
        return 404, None

    monkeypatch.setattr(pointers, "api_get", api_get)
    return asked


def test_the_head_of_a_fork_pr_from_a_fork_of_the_fork_is_found(pointers, monkeypatch):
    github(pointers, monkeypatch, [[pr(229, "d" * 40), pr(193, GITLINK, "pudgeai-juniour/duckdb")]])
    assert pointers.open_fork_pr(FORK, GITLINK) == (f"{FORK}#193 into v2026.10.06", "v2026.10.06")


def test_every_page_of_open_prs_is_searched(pointers, monkeypatch):
    others = [pr(n, f"{n:040x}") for n in range(100)]
    asked = github(pointers, monkeypatch, [others, [pr(223, GITLINK, "mosya415/duckdb")]])
    assert pointers.open_fork_pr(FORK, GITLINK) == (f"{FORK}#223 into v2026.10.06", "v2026.10.06")
    assert [p.rsplit("=", 1)[1] for p in asked] == ["1", "2"]


def test_a_pr_into_a_branch_other_than_a_version_branch_is_not_waited_for(pointers, monkeypatch):
    asked = github(pointers, monkeypatch, [[pr(137, GITLINK, base="mbkkt/update-duckdb-base")]])
    assert pointers.open_fork_pr(FORK, GITLINK) is None
    assert len(asked) == 1


def test_the_lookup_fails_closed_when_github_does_not_answer(pointers, monkeypatch):
    monkeypatch.setattr(pointers, "api_get", lambda path: (502, None))
    with pytest.raises(pointers.Unreachable):
        pointers.open_fork_pr(FORK, GITLINK)


def test_the_check_reports_the_head_of_a_fork_pr_from_a_fork_of_the_fork_as_pending(
        pointers, monkeypatch, tmp_path, capsys):
    root = tmp_path / "super"
    root.mkdir()
    git("init", "-q", cwd=root)
    (root / ".gitmodules").write_text('[submodule "third_party/duckdb"]\n\tpath = third_party/duckdb\n'
                                      f"\turl = https://github.com/{FORK}\n")
    git("add", ".gitmodules", cwd=root)
    for name, sha in (("main", MAIN_GITLINK), ("head", GITLINK)):
        git("update-index", "--add", "--cacheinfo", f"160000,{sha},third_party/duckdb", cwd=root)
        git(*IDENTITY, "commit", "-q", "-m", name, cwd=root)
        if name == "main":
            git("update-ref", "refs/remotes/origin/main", "HEAD", cwd=root)
    monkeypatch.chdir(root)
    monkeypatch.setattr(pointers, "version_branches", lambda url, pattern: {"v2026.10.06": "f" * 40})
    github(pointers, monkeypatch, [[pr(193, GITLINK, "pudgeai-juniour/duckdb")]], compare={
        f"{MAIN_GITLINK}...{GITLINK}": "ahead",
        f"v2026.10.06...{MAIN_GITLINK}": "behind",
        f"v2026.10.06...{GITLINK}": "diverged",
    })
    assert pointers.check("third_party/duckdb", None, "HEAD") == "pending"
    assert capsys.readouterr().out == PENDING + "\n"


GH = r'''#!/usr/bin/env python3
import os
import sys
from pathlib import Path

args = sys.argv[1:]
state = Path(os.environ["STATE"])
with open(state / "calls.log", "a") as log:
    log.write(" ".join(args) + "\n")
head, base, fork_head = os.environ["HEAD_SHA"], os.environ["BASE_SHA"], os.environ["FORK_HEAD"]
answers = [
    (["api", "repos/serenedb/serenedb/pulls/1389"], f"open false {head}"),
    (["api", "repos/serenedb/serenedb/rules/branches/main"],
     "strict\ttrue\ncheck\tserenedb | build & test\ncheck\tpre-commit\napprovals\t1"),
    (["api", f"repos/serenedb/serenedb/compare/main...{head}"], f"{base} 0"),
    (["api", "graphql"], "review\t\tAPPROVED\ncheck\tpre-commit\tSUCCESS\n"
                         "status\tserenedb | build & test\tSUCCESS\nstatus\tpre-commit\tPENDING"),
    (["api", f"repos/serenedb/duckdb/compare/v2026.10.06...{fork_head}"], "ahead"),
    (["pr", "view", "193", "-R", "serenedb/duckdb"], "false APPROVED"),
    (["api", "-X", "PATCH", "repos/serenedb/duckdb/git/refs/heads/v2026.10.06"], "{}"),
    (["workflow", "run", "gitlink-status.yml"], ""),
    (["api", f"repos/serenedb/serenedb/commits/{head}/status"],
     os.environ["POSTED"] if (state / "dispatched").exists() else "pending"),
    (["api", "-X", "PUT", "repos/serenedb/serenedb/pulls/1389/merge"], "Pull Request successfully merged"),
]
for prefix, answer in answers:
    if args[:len(prefix)] == prefix:
        if prefix[0] == "api" and "PATCH" in prefix:
            (state / "landed").touch()
        if prefix[0] == "workflow":
            (state / "dispatched").touch()
        print(answer)
        sys.exit(0)
sys.exit(f"unexpected gh call: {' '.join(args)}")
'''

CHECK = '''import os
import sys
from pathlib import Path

if (Path(os.environ["STATE"]) / "landed").exists() and os.environ["LANDS"] == "yes":
    sys.exit(0)
print(os.environ["PENDING_LINE"])
sys.exit(3)
'''


@pytest.fixture()
def land(tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    git("init", "-q", cwd=work)
    commits = []
    for name in ("base", "head"):
        (work / name).write_text(name)
        git("add", name, cwd=work)
        git(*IDENTITY, "commit", "-q", "-m", name, cwd=work)
        commits.append(git("rev-parse", "HEAD", cwd=work))
    origin = tmp_path / "origin.git"
    git("init", "-q", "--bare", str(origin), cwd=tmp_path)
    git("config", "uploadpack.allowAnySHA1InWant", "true", cwd=origin)
    git("push", "-q", str(origin), "HEAD:refs/pull/1389/head", cwd=work)
    repo = tmp_path / "repo"
    repo.mkdir()
    git("init", "-q", cwd=repo)
    git("remote", "add", "origin", f"file://{origin}", cwd=repo)
    (repo / "scripts").mkdir()
    (repo / "scripts" / "check_submodule_pointers.py").write_text(CHECK)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "gh").write_text(GH)
    (bin_dir / "sleep").write_text("#!/bin/sh\n")
    for tool in bin_dir.iterdir():
        tool.chmod(0o755)
    state = tmp_path / "state"
    state.mkdir()
    base, head = commits

    def run(*args, posted="success", lands="yes"):
        env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", STATE=str(state),
                   HEAD_SHA=head, BASE_SHA=base, FORK_HEAD=GITLINK, PENDING_LINE=PENDING,
                   POSTED=posted, LANDS=lands)
        res = subprocess.run(["bash", str(SCRIPTS / "land.sh"), "1389", *args], cwd=repo, env=env,
                             capture_output=True, text=True)
        calls = (state / "calls.log").read_text().splitlines()
        return res, calls

    return run, head


def changes(calls):
    return [c.split()[0:3] for c in calls if c.startswith(("api -X", "workflow"))]


def test_a_dry_run_prints_the_landing_and_changes_nothing(land):
    run, head = land
    res, calls = run()
    assert res.returncode == 0, res.stderr
    assert res.stdout.splitlines() == [
        PENDING,
        f"land {FORK}#193: v2026.10.06 -> {GITLINK} (APPROVED)",
        f"turn the pre-commit status of {head} green",
        f"squash-merge #1389 at {head}",
        "dry run: rerun with --yes to land",
    ]
    assert changes(calls) == []


def test_landing_fast_forwards_the_fork_turns_the_status_green_and_merges(land):
    run, head = land
    res, calls = run("--yes")
    assert res.returncode == 0, res.stderr
    assert changes(calls) == [
        ["api", "-X", "PATCH"],
        ["workflow", "run", "gitlink-status.yml"],
        ["api", "-X", "PUT"],
    ]
    assert f"api -X PATCH repos/{FORK}/git/refs/heads/v2026.10.06 -f sha={GITLINK} -F force=false" in calls
    assert f"workflow run gitlink-status.yml -R serenedb/serenedb -f SHA={head}" in calls
    assert f"api -X PUT repos/serenedb/serenedb/pulls/1389/merge -f merge_method=squash -f sha={head} --jq .message" in calls
    assert res.stdout.splitlines()[-2:] == [f"pre-commit status of {head} is green",
                                            "Pull Request successfully merged"]


def test_the_merge_waits_for_a_green_status(land):
    run, head = land
    res, calls = run("--yes", posted="pending")
    assert res.returncode == 1
    assert "its pre-commit status is pending 10 minutes after landing" in res.stderr
    assert changes(calls)[-1] == ["workflow", "run", "gitlink-status.yml"]


def test_nothing_is_merged_while_the_gitlinks_are_still_off_their_version_branches(land):
    run, head = land
    res, calls = run("--yes", lands="no")
    assert res.returncode == 1
    assert f"#1389: its gitlinks are not on version branches after landing:\n{PENDING}" in res.stderr
    assert changes(calls) == [["api", "-X", "PATCH"]]
