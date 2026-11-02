import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "duckdb_family.sh"
IDENTITY = ["-c", "user.name=test", "-c", "user.email=test@example.com"]

FAKE_MAKEFILE = """\
format_venv:
\t@true

capigen_venv:
\t@true

generate-files:
\tcp src/grammar.y src/grammar.cpp

spell_tools:
\tVERSION=1.0.0; \\
\ttrue
"""

FAKE_FORMAT = """\
import argparse
import os
import subprocess
import sys

p = argparse.ArgumentParser()
p.add_argument("revision", nargs="?", default="HEAD")
p.add_argument("--check", action="store_true")
p.add_argument("--fix", action="store_true")
p.add_argument("-a", "--all", action="store_true")
p.add_argument("-d", "--directories", nargs="*", default=[])
p.add_argument("-C", "--workdir")
p.add_argument("-y", "--noconfirm", action="store_true")
p.add_argument("-q", "--silent", action="store_true")
p.add_argument("--staged", action="store_true")
args = p.parse_args()
if args.workdir:
    os.chdir(args.workdir)
if os.path.isfile(args.revision):
    sys.exit(0 if args.revision.endswith(".cpp") else 1)
if args.all:
    files = [os.path.join(d, f) for top in args.directories or ["."] for d, _, fs in os.walk(top)
             for f in fs]
else:
    diff = ["git", "diff", "--name-only", *(["--cached"] if args.staged else []), args.revision]
    files = subprocess.run(diff, capture_output=True, text=True, check=True).stdout.split()
bad = [f for f in files if f.endswith(".cpp") and os.path.isfile(f) and "BAD" in open(f).read()]
for f in bad if args.fix else []:
    with open(f) as src:
        text = src.read()
    with open(f, "w") as dst:
        dst.write(text.replace("BAD", "GOOD"))
sys.exit(1 if bad and not args.fix else 0)
"""


def git(*args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True,
                          text=True).stdout.strip()


class Fork:
    def __init__(self, root, name):
        self.bare = root / f"{name}.git"
        git("init", "-q", "--bare", str(self.bare), cwd=root)
        git("config", "uploadpack.allowAnySHA1InWant", "true", cwd=self.bare)
        git("config", "uploadpack.allowFilter", "true", cwd=self.bare)
        self.work = root / f"{name}-work"
        self.work.mkdir()
        git("init", "-q", cwd=self.work)
        self.url = f"file://{self.bare}"

    def commit(self, files, message="change", branch="v2026.10.24"):
        for name, text in files.items():
            path = self.work / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
            path.chmod(0o755 if name.endswith(".sh") else 0o644)
            git("add", name, cwd=self.work)
        git(*IDENTITY, "commit", "-q", "--allow-empty", "-m", message, cwd=self.work)
        git("push", "-q", "-f", str(self.bare), f"HEAD:refs/heads/{branch}", cwd=self.work)
        return git("rev-parse", "HEAD", cwd=self.work)

    def orphan(self, files, message):
        git("checkout", "-q", "--orphan", "lineage", cwd=self.work)
        return self.commit(files, message, branch="v2026.10.25")


@pytest.fixture()
def family(tmp_path):
    duckdb = Fork(tmp_path, "duckdb")
    inet = Fork(tmp_path, "duckdb-inet")
    root = tmp_path / "super"
    (root / "third_party" / "duckdb_clickhouse" / "src").mkdir(parents=True)
    git("init", "-q", cwd=root)
    (root / ".gitmodules").write_text("".join(
        f'[submodule "third_party/{d}"]\n\tpath = third_party/{d}\n\turl = {f.url}\n'
        for d, f in (("duckdb", duckdb), ("duckdb_inet", inet))))
    (root / "third_party" / "duckdb_clickhouse" / "src" / "c.cpp").write_text("ok\n")
    git("add", ".", cwd=root)

    def commit(gitlinks, files=None):
        for d, sha in gitlinks.items():
            git("update-index", "--add", "--cacheinfo", f"160000,{sha},third_party/{d}", cwd=root)
        for name, text in (files or {}).items():
            (root / name).write_text(text)
            git("add", name, cwd=root)
        git(*IDENTITY, "commit", "-q", "--allow-empty", "-m", "change", cwd=root)

    def run(mode, *args):
        return subprocess.run(["bash", str(SCRIPT), mode, *(args or ["HEAD^1"])], cwd=root,
                              capture_output=True, text=True,
                              env=dict(os.environ, XDG_CACHE_HOME=str(tmp_path / "cache")))

    def sub(d, *args):
        return git(*args, cwd=root / "third_party" / d)

    return duckdb, inet, root, commit, run, sub


def fetched(root, d):
    return (root / "third_party" / d / ".git").exists()


def test_a_moved_gitlink_is_checked_out_with_the_commits_it_moves_over(family):
    duckdb, inet, root, commit, run, sub = family
    d0 = duckdb.commit({"src/a.cpp": "a\n"})
    i = [inet.commit({"src/x.cpp": f"{n}\n"}) for n in range(5)]
    commit({"duckdb": d0, "duckdb_inet": i[1]})
    commit({"duckdb_inet": i[4]})
    res = run("fetch")
    assert res.returncode == 0, res.stderr
    assert sub("duckdb_inet", "rev-parse", "HEAD") == i[4]
    assert sub("duckdb_inet", "rev-list", "HEAD").split() == i[4:0:-1]
    assert sub("duckdb", "rev-list", "HEAD").split() == [d0]
    assert git("status", "--porcelain", cwd=root) == ""


def test_a_gitlink_on_another_lineage_gets_its_commit_and_parent(family):
    duckdb, inet, root, commit, run, sub = family
    d0 = duckdb.commit({"src/a.cpp": "a\n"})
    old = inet.commit({"src/x.cpp": "old\n"})
    first = inet.orphan({"src/x.cpp": "new\n"}, "lineage start")
    new = inet.commit({"src/x.cpp": "newer\n"}, branch="v2026.10.25")
    commit({"duckdb": d0, "duckdb_inet": old})
    commit({"duckdb_inet": new})
    res = run("fetch")
    assert res.returncode == 0, res.stderr
    assert sub("duckdb_inet", "rev-list", "HEAD").split() == [new, first]


def test_a_change_outside_the_family_fetches_nothing(family):
    duckdb, inet, root, commit, run, sub = family
    commit({"duckdb": duckdb.commit({"src/a.cpp": "a\n"}),
            "duckdb_inet": inet.commit({"src/x.cpp": "x\n"})})
    commit({}, {"README": "text\n"})
    res = run("check")
    assert res.returncode == 0, res.stderr
    assert not fetched(root, "duckdb") and not fetched(root, "duckdb_inet")


def test_a_duckdb_clickhouse_change_fetches_duckdb_alone(family):
    duckdb, inet, root, commit, run, sub = family
    d0 = duckdb.commit({"src/a.cpp": "a\n"})
    commit({"duckdb": d0, "duckdb_inet": inet.commit({"src/x.cpp": "x\n"})})
    commit({}, {"third_party/duckdb_clickhouse/src/c.cpp": "changed\n"})
    res = run("fetch")
    assert res.returncode == 0, res.stderr
    assert sub("duckdb", "rev-parse", "HEAD") == d0
    assert not fetched(root, "duckdb_inet")


def test_a_checked_out_submodule_keeps_its_checkout_and_gets_the_missing_commit(family):
    duckdb, inet, root, commit, run, sub = family
    d0 = duckdb.commit({"src/a.cpp": "a\n"})
    i0 = inet.commit({"src/x.cpp": "0\n"})
    commit({"duckdb": d0, "duckdb_inet": i0})
    git("clone", "-q", "-b", "v2026.10.24", inet.url, str(root / "third_party" / "duckdb_inet"),
        cwd=root)
    i1 = inet.commit({"src/x.cpp": "1\n"})
    commit({"duckdb_inet": i1})
    res = run("fetch")
    assert res.returncode == 0, res.stderr
    assert sub("duckdb_inet", "rev-parse", "HEAD") == i0
    assert sub("duckdb_inet", "rev-parse", "--is-shallow-repository") == "false"
    assert sub("duckdb_inet", "cat-file", "-t", i1) == "commit"


@pytest.fixture()
def tools(tmp_path):
    venvs = tmp_path / "cache" / "serenedb-duckdb"
    for venv in ("format-venv", "generate-venv", "capigen-venv"):
        (venvs / venv / "bin").mkdir(parents=True)
        (venvs / venv / "bin" / "python").symlink_to(sys.executable)
    typos = venvs / "format-venv" / "bin" / "typos"
    typos.write_text("#!/bin/sh\necho typos-cli 1.0.0\n")
    typos.chmod(0o755)
    (venvs / "uv-venv" / "bin").mkdir(parents=True)
    uv = venvs / "uv-venv" / "bin" / "uv"
    uv.write_text("#!/bin/sh\n")
    uv.chmod(0o755)
    return {"Makefile": FAKE_MAKEFILE, "scripts/format.py": FAKE_FORMAT,
            "scripts/capi_v2_regen.sh": "#!/bin/sh\n", "scripts/generate_enums.py": "",
            ".clang-format": "", "src/grammar.y": "a\n", "src/grammar.cpp": "a\n"}


def test_check_finds_an_unformatted_fork_commit_and_accepts_a_current_regen_commit(family, tools):
    duckdb, inet, root, commit, run, sub = family
    d0 = duckdb.commit(tools)
    d1 = duckdb.commit({"src/grammar.y": "b\n"})
    d2 = duckdb.commit({"src/grammar.cpp": "b\n"}, "regen: regenerate")
    i0 = inet.commit({".clang-format": "", "src/x.cpp": "ok\n"})
    inet.commit({"src/y.cpp": "BAD\n"}, "feat: unformatted")
    i2 = inet.commit({"src/z.cpp": "ok\n"})
    commit({"duckdb": d0, "duckdb_inet": i0})
    commit({"duckdb": d2, "duckdb_inet": i2})
    res = run("check")
    assert res.returncode == 1
    assert "duckdb: 2 commits checked, 0 not formatted" in res.stdout
    assert "regen: commit matches the generators" in res.stdout
    assert "duckdb_inet: 2 commits checked, 1 not formatted" in res.stdout
    assert "feat: unformatted" in res.stdout
    assert sub("duckdb", "rev-list", "HEAD").split() == [d2, d1, d0]


def test_check_wants_a_regen_commit_when_the_generators_see_the_change(family, tools):
    duckdb, inet, root, commit, run, sub = family
    d0 = duckdb.commit(tools)
    d1 = duckdb.commit({"src/grammar.y": "b\n"})
    commit({"duckdb": d0, "duckdb_inet": inet.commit({".clang-format": ""})})
    commit({"duckdb": d1})
    res = run("check")
    assert res.returncode == 1
    assert "duckdb: 1 commits checked, 0 not formatted" in res.stdout
    assert "HEAD needs a regen: commit" in res.stderr


def test_check_skips_the_range_of_a_gitlink_on_another_lineage(family, tools):
    duckdb, inet, root, commit, run, sub = family
    d0 = duckdb.commit(tools)
    old = inet.commit({".clang-format": "", "src/x.cpp": "BAD\n"})
    new = inet.orphan({".clang-format": "", "src/x.cpp": "ok\n"}, "lineage start")
    commit({"duckdb": d0, "duckdb_inet": old})
    commit({"duckdb_inet": new})
    res = run("check")
    assert res.returncode == 0, res.stdout + res.stderr
    assert f"duckdb_inet: no range to check, {new} does not descend from {old}" in res.stdout


def test_duckdb_clickhouse_is_checked_on_the_files_format_py_formats(family, tools):
    duckdb, inet, root, commit, run, sub = family
    commit({"duckdb": duckdb.commit(tools), "duckdb_inet": inet.commit({".clang-format": ""})})
    commit({}, {"third_party/duckdb_clickhouse/src/c.cpp": "changed\n"})
    assert run("fetch").returncode == 0
    clickhouse = root / "third_party" / "duckdb_clickhouse"
    (clickhouse / "extension_config.cmake").write_text("set(x)\n")
    git("add", "third_party/duckdb_clickhouse/extension_config.cmake", cwd=root)
    res = run("format", "--check", "--staged", "duckdb_clickhouse")
    assert res.returncode == 0, res.stdout + res.stderr
    (clickhouse / "src" / "c.cpp").write_text("BAD\n")
    git("add", "third_party/duckdb_clickhouse/src/c.cpp", cwd=root)
    res = run("format", "--check", "--staged", "duckdb_clickhouse")
    assert res.returncode == 1
    assert "not formatted: duckdb_clickhouse" in res.stderr
    git("reset", "-q", cwd=root)
    res = run("format", "--check", "duckdb_clickhouse")
    assert res.returncode == 1
    assert "not formatted: duckdb_clickhouse" in res.stderr
