import importlib.util
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "check_submodule_pointers.py"
OLD = "1" * 40
NEW = "2" * 40
HEADS = {"v2026.09.12": OLD, "v2026.10.20": NEW}
IDENTITY = ["-c", "user.name=test", "-c", "user.email=test@example.com"]


@pytest.fixture()
def checker():
    spec = importlib.util.spec_from_file_location("check_submodule_pointers_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def github(monkeypatch, checker, compare, commits):
    def api_get(path):
        _, _, _, kind, rest = path.split("/", 4)
        if kind == "compare":
            status = compare.get(tuple(rest.split("...")))
            return (200, {"status": status}) if status else (404, None)
        if kind == "commits" and "/" not in rest:
            return (200, {"sha": rest}) if rest in commits else (422, None)
        raise AssertionError(path)

    monkeypatch.setattr(checker, "api_get", api_get)


def test_a_release_import_with_no_history_in_common_with_mains_gitlink_passes(monkeypatch, checker):
    github(monkeypatch, checker, {("v2026.09.12", OLD): "identical", ("v2026.10.20", NEW): "identical"},
           {OLD, NEW})
    assert checker.validate_api("serenedb/fork", NEW, OLD, HEADS, lambda: None, None) == ("ok", "on v2026.10.20")


def test_a_commit_the_fork_does_not_have_fails(monkeypatch, checker):
    github(monkeypatch, checker, {("v2026.09.12", OLD): "identical"}, {OLD})
    status, detail = checker.validate_api("serenedb/fork", NEW, OLD, HEADS, lambda: None, None)
    assert status == "fail" and "unknown" in detail


def git(*args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def git_ok(*args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True).returncode == 0


@pytest.fixture()
def fork(tmp_path):
    repo = tmp_path / "local" / "dep"
    repo.mkdir(parents=True)
    git("init", "-q", cwd=repo)
    tree = git("write-tree", cwd=repo)

    def commit(name, *parents):
        args = [arg for parent in parents for arg in ("-p", parent)]
        return git(*IDENTITY, "commit-tree", tree, "-m", name, *args, cwd=repo)

    shas = {"older": commit("older version branch"), "main": commit("main's gitlink")}
    shas["on main"] = commit("on main's version branch", shas["main"])
    shas["pr"] = commit("open fork PR", shas["on main"])
    shas["old wip"] = commit("update older than main's version branch")
    shas["same wip"] = commit("update dated as main's version branch")
    shas["rebased"] = commit("our commit rebased onto the new upstream head", commit("new upstream head"))
    shas["wip"] = commit("regen", shas["rebased"])
    branches = {"v2026.09.12": shas["older"], "v2026.10.20": shas["on main"], "mbkkt/fix": shas["pr"],
                "v2026.10.01-wip": shas["old wip"], "v2026.10.20-wip": shas["same wip"],
                "v2026.11.15-wip": shas["wip"]}
    for name, sha in branches.items():
        git("update-ref", f"refs/heads/{name}", sha, cwd=repo)
    return repo, shas, branches


def fork_api(repo, pr_head):
    def api_get(path):
        _, _, _, kind, rest = path.split("/", 4)
        if kind == "compare":
            base, head = rest.split("...")
            if not git_ok("merge-base", base, head, cwd=repo):
                return 404, None
            if git("rev-parse", base, cwd=repo) == git("rev-parse", head, cwd=repo):
                status = "identical"
            elif git_ok("merge-base", "--is-ancestor", head, base, cwd=repo):
                status = "behind"
            elif git_ok("merge-base", "--is-ancestor", base, head, cwd=repo):
                status = "ahead"
            else:
                status = "diverged"
            return 200, {"status": status}
        sha, _, pulls = rest.partition("/")
        if kind == "commits" and pulls == "pulls":
            prs = [{"number": 7, "state": "open", "head": {"sha": sha}, "base": {"ref": "v2026.10.20"}}]
            return 200, prs if sha == pr_head else []
        if kind == "commits" and not pulls:
            exists = git_ok("cat-file", "-e", f"{sha}^{{commit}}", cwd=repo)
            return (200, {"sha": sha}) if exists else (422, None)
        raise AssertionError(path)

    return api_get


@pytest.fixture(params=["local", "api"])
def check(request, tmp_path, monkeypatch, checker, fork):
    repo, shas, branches = fork
    (tmp_path / "api").mkdir()
    monkeypatch.chdir(tmp_path / request.param)
    monkeypatch.setattr(checker, "main_gitlink", lambda path: shas["main"])
    monkeypatch.setattr(checker, "submodule_url", lambda path, rev: "https://github.com/serenedb/fork")
    monkeypatch.setattr(checker, "remote_heads", lambda url: branches)
    monkeypatch.setattr(checker, "api_get", fork_api(repo, shas["pr"]))
    return lambda gitlink: checker.check("dep", gitlink, None)


def test_a_commit_on_mains_version_branch_passes(check, fork, capsys):
    _, shas, _ = fork
    assert check(shas["on main"]) == "ok"
    assert capsys.readouterr() == ("", "")


def test_the_head_of_an_open_fork_pr_into_mains_version_branch_is_pending(check, fork, capsys):
    _, shas, _ = fork
    assert check(shas["pr"]) == "pending"
    assert capsys.readouterr().out == \
        f"dep: gitlink {shas['pr']} is pending: head of open serenedb/fork#7 into v2026.10.20\n"


def test_the_head_of_a_wip_branch_newer_than_mains_version_branch_is_pending(check, fork, capsys):
    _, shas, _ = fork
    assert check(shas["wip"]) == "pending"
    assert capsys.readouterr().out == f"dep: gitlink {shas['wip']} is pending: head of serenedb/fork v2026.11.15-wip\n"


def test_the_head_of_a_wip_branch_older_than_mains_version_branch_fails(check, fork, capsys):
    _, shas, _ = fork
    assert check(shas["old wip"]) == "fail"
    assert "is the head of serenedb/fork v2026.10.01-wip, not newer than main's gitlink branch v2026.10.20" in \
        capsys.readouterr().err


def test_the_head_of_a_wip_branch_dated_as_mains_version_branch_fails(check, fork, capsys):
    _, shas, _ = fork
    assert check(shas["same wip"]) == "fail"
    assert "is the head of serenedb/fork v2026.10.20-wip, not newer than main's gitlink branch v2026.10.20" in \
        capsys.readouterr().err


def test_a_commit_behind_a_wip_head_fails(check, fork, capsys):
    _, shas, _ = fork
    assert check(shas["rebased"]) == "fail"
    assert "not reachable from any version branch" in capsys.readouterr().err


def test_a_gitlink_on_the_version_branch_its_wip_branch_still_heads_passes_and_names_the_wip_branch(
        check, checker, fork, monkeypatch, capsys):
    repo, shas, branches = fork
    git("update-ref", "refs/heads/v2026.11.15", shas["wip"], cwd=repo)
    monkeypatch.setattr(checker, "remote_heads", lambda url: branches | {"v2026.11.15": shas["wip"]})
    assert check(shas["wip"]) == "ok"
    assert capsys.readouterr().out == \
        f"dep: gitlink {shas['wip']} is on v2026.11.15, and serenedb/fork v2026.11.15-wip still heads it\n"


def test_a_new_submodule_whose_fork_has_only_its_wip_branch_is_pending(check, checker, fork, monkeypatch, capsys):
    _, shas, _ = fork
    monkeypatch.setattr(checker, "main_gitlink", lambda path: "")
    monkeypatch.setattr(checker, "remote_heads", lambda url: {"v2026.11.15-wip": shas["wip"]})
    assert check(shas["wip"]) == "pending"
    assert capsys.readouterr().out == f"dep: gitlink {shas['wip']} is pending: head of serenedb/fork v2026.11.15-wip\n"
