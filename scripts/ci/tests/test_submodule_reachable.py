import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "check_submodule_pointers.py"
IDENTITY = ["-c", "user.name=test", "-c", "user.email=test@example.com"]


def git(*args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True,
                          text=True).stdout.strip()


def commit(repo, name):
    (repo / name).write_text(name)
    git("add", name, cwd=repo)
    git(*IDENTITY, "commit", "-q", "-m", name, cwd=repo)
    return git("rev-parse", "HEAD", cwd=repo)


@pytest.fixture()
def remote(tmp_path):
    bare = tmp_path / "remote.git"
    git("init", "-q", "--bare", str(bare), cwd=tmp_path)
    git("config", "uploadpack.allowAnySHA1InWant", "true", cwd=bare)
    git("config", "uploadpack.allowFilter", "true", cwd=bare)
    work = tmp_path / "work"
    work.mkdir()
    git("init", "-q", cwd=work)
    served = commit(work, "served")
    git("push", "-q", str(bare), "HEAD:refs/heads/v2026.10.24", cwd=work)
    unserved = commit(work, "unserved")
    return f"file://{bare}", served, unserved


def superproject(tmp_path, url, sha, entry="dep"):
    root = tmp_path / "super"
    root.mkdir()
    git("init", "-q", cwd=root)
    (root / ".gitmodules").write_text(f'[submodule "{entry}"]\n\tpath = {entry}\n\turl = {url}\n')
    git("add", ".gitmodules", cwd=root)
    git("update-index", "--add", "--cacheinfo", f"160000,{sha},dep", cwd=root)
    git(*IDENTITY, "commit", "-q", "-m", "gitlink", cwd=root)
    return subprocess.run([sys.executable, str(SCRIPT), "--reachable", "--rev", "HEAD"], cwd=root,
                          capture_output=True, text=True)


def test_a_gitlink_its_remote_serves_passes(tmp_path, remote):
    url, served, _ = remote
    result = superproject(tmp_path, url, served)
    assert result.returncode == 0, result.stderr


def test_a_gitlink_its_remote_does_not_serve_fails(tmp_path, remote):
    url, _, unserved = remote
    result = superproject(tmp_path, url, unserved)
    assert result.returncode == 1
    assert f"dep points at {unserved}, which {url} does not serve" in result.stderr


def test_a_gitlink_without_a_gitmodules_entry_fails(tmp_path, remote):
    url, served, _ = remote
    result = superproject(tmp_path, url, served, entry="other")
    assert result.returncode == 1
    assert "dep is a gitlink without a .gitmodules entry" in result.stderr
