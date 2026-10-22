#!/usr/bin/env python3
"""Pre-commit hook: validate third_party submodule gitlinks.

Every gitlink the change sets is checked: the ones that differ from the
merge base with origin/main (all of them when there is none, as in a
shallow CI checkout), except the EXCLUDED paths (submodules whose remote
we do not control and so cannot carry our version branches). Each must
  1. be reachable from one of the fork's version branches (vYYYY.MM.DD),
     i.e. the fork-side PR is merged -- not a feature-branch head, and
  2. not be older than origin/main's gitlink: its newest containing
     version branch must compare >= main's (branch names order by date),
     and within the same branch it must descend from main's gitlink.
     Switching to a newer version branch is fine even though rebased
     lineages share no ancestry.
A submodule origin/main does not have yet (a new dependency) only has to
satisfy rule 1.

A gitlink that breaks rule 1 but is the head of an open fork PR into
main's version branch or a newer one is pending, not failed: the PR is
waiting for that fork PR to land. --allow-pending (the pre-commit hook)
passes it; without the flag the script exits 3, which the PR's pre-commit
job turns into a pending `pre-commit` status. --rev checks the gitlinks
of a commit instead of the index, --base compares with another commit.

Validation prefers the local submodule clone (pure git, works offline);
without one (CI checks out with submodules: false) it falls back to the
GitHub compare API. If neither works the hook fails closed; bypass a
known-good commit with SKIP=check-submodule-pointers.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

VERSION_BRANCH = re.compile(r"^v20\d{2}\.\d{2}\.\d{2}$")
GITHUB_URL = re.compile(r"^https://github\.com/([\w.-]+/[\w.-]+?)(?:\.git)?$")

EXCLUDED = {
    "third_party/yaclib",  # upstream remote, no serenedb version branches
}


def git(*args: str, cwd: str | None = None) -> str | None:
    proc = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True
    )
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def checked_paths(rev: str | None, base: str | None) -> list[str] | None:
    if base:
        if rev:
            out = git("diff-tree", "-r", "--no-renames", base, rev)
        else:
            out = git("diff-index", "--cached", "--no-renames", base)
        mode_field = 1
    else:
        out = git("ls-tree", "-r", rev) if rev else git("ls-files", "-s")
        mode_field = 0
    if out is None:
        return None
    paths = []
    for line in out.splitlines():
        meta, _, path = line.partition("\t")
        if meta.split()[mode_field] == "160000" and path not in EXCLUDED:
            paths.append(path)
    return paths


def gitlink_at(path: str, rev: str | None) -> str | None:
    if rev:
        out = git("ls-tree", rev, "--", path)
        if not out:
            return None
        mode, _, sha = out.split()[:3]
    else:
        out = git("ls-files", "-s", "--", path)
        if not out:
            return None
        mode, sha = out.split()[:2]
    return sha if mode == "160000" else None


def main_gitlink(path: str) -> str | None:
    out = git("ls-tree", "origin/main", "--", path)
    if out is None:
        fetch = ["fetch", "--quiet", "--depth=1", "origin"]
        if git(*fetch, "+refs/heads/main:refs/remotes/origin/main") is None:
            return None
        out = git("ls-tree", "origin/main", "--", path)
        if out is None:
            return None
    if not out:
        return ""
    mode, _, sha = out.split()[:3]
    return sha if mode == "160000" else ""


def submodule_url(path: str, rev: str | None) -> str | None:
    source = ["--blob", f"{rev}:.gitmodules"] if rev else ["-f", ".gitmodules"]
    return git("config", *source, f"submodule.{path}.url")


def version_branches(url: str, pattern: re.Pattern) -> dict[str, str] | None:
    out = git("ls-remote", "--heads", "--", url)
    if out is None:
        return None
    heads = {}
    for line in out.splitlines():
        sha, ref = line.split(None, 1)
        name = ref.removeprefix("refs/heads/")
        if pattern.match(name):
            heads[name] = sha
    return heads


def verdict(
    heads: dict[str, str],
    gitlink: str,
    main_sha: str,
    contains,
    main_is_ancestor,
    fork_pr,
) -> tuple[str, str]:
    """Shared decision: ("ok" | "pending" | "fail", detail).
    `contains(branch, sha)` and `main_is_ancestor()` abstract over the
    local-git and GitHub-API backends; `fork_pr()` finds the open fork PR
    the gitlink heads. Branches older than main's are only searched to
    word a failure."""
    names = sorted(heads, reverse=True)
    ver_main = (
        next((n for n in names if contains(n, main_sha)), None) if main_sha else None
    )
    current = [n for n in names if ver_main is None or n >= ver_main]
    ver_new = next((n for n in current if contains(n, gitlink)), None)
    if ver_new is None:
        pr = fork_pr()
        if pr is not None and (ver_main is None or pr[1] >= ver_main):
            return "pending", f"head of open {pr[0]}"
        older = [n for n in names if n not in current]
        ver_old = next((n for n in older if contains(n, gitlink)), None)
        if ver_old is not None:
            return "fail", f"on {ver_old}, older than main's gitlink branch {ver_main}"
        if pr is not None:
            return (
                "fail",
                f"is the head of {pr[0]}, older than main's gitlink branch {ver_main}",
            )
        return (
            "fail",
            "not reachable from any version branch, and no open fork PR into "
            "a version branch has it as its head",
        )
    if ver_main is None or ver_new > ver_main or main_is_ancestor():
        return "ok", f"on {ver_new}"
    return "fail", f"on {ver_new} but behind main's gitlink {main_sha}"


def local_has_commit(path: str, sha: str) -> bool:
    return git("cat-file", "-e", f"{sha}^{{commit}}", cwd=path) is not None


def is_local_ancestor(path: str, ancestor: str, descendant: str) -> bool:
    proc = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=path,
        capture_output=True,
    )
    return proc.returncode == 0


def validate_local(
    path: str, gitlink: str, main_sha: str, heads: dict[str, str], fork_pr
) -> tuple[str, str] | None:
    """Validate with the local submodule clone; None if it cannot decide."""
    if not os.path.exists(os.path.join(path, ".git")):
        return None
    needed = [gitlink, *heads.values()]
    if main_sha:
        needed.append(main_sha)
    if not all(local_has_commit(path, s) for s in needed):
        git("fetch", "--quiet", "origin", cwd=path)
        if not all(local_has_commit(path, s) for s in needed):
            return None
    return verdict(
        heads,
        gitlink,
        main_sha,
        lambda name, sha: is_local_ancestor(path, sha, heads[name]),
        lambda: is_local_ancestor(path, main_sha, gitlink),
        fork_pr,
    )


def github_repo(url: str) -> str | None:
    m = GITHUB_URL.match(url)
    return m.group(1) if m else None


def api_get(path: str) -> tuple[int, object]:
    req = urllib.request.Request(
        f"https://api.github.com/{path}",
        headers={"Accept": "application/vnd.github+json"},
    )
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    code = 0
    for attempt in range(3):
        if attempt:
            time.sleep(2**attempt)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status, json.load(resp)
        except urllib.error.HTTPError as e:
            if e.code < 500:
                return e.code, None
            code = e.code
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            code = 0
    return code, None


def api_compare(repo: str, base: str, head: str) -> str | None:
    code, body = api_get(f"repos/{repo}/compare/{base}...{head}")
    if code == 404:
        return "unknown"
    if code != 200 or not isinstance(body, dict):
        return None
    return body.get("status")


class Unreachable(Exception):
    pass


def open_fork_pr(repo: str, sha: str) -> tuple[str, str] | None:
    code, body = api_get(f"repos/{repo}/commits/{sha}/pulls")
    if code in (404, 422):
        return None
    if code != 200 or not isinstance(body, list):
        raise Unreachable(f"cannot list the pull requests of {sha} on {repo}")
    for pr in body:
        base = pr.get("base", {}).get("ref", "")
        if (
            pr.get("state") == "open"
            and pr.get("head", {}).get("sha") == sha
            and VERSION_BRANCH.match(base)
        ):
            return f"{repo}#{pr['number']} into {base}", base
    return None


def validate_api(
    repo: str, gitlink: str, main_sha: str, heads: dict[str, str], fork_pr
) -> tuple[str, str] | None:
    """Validate through the GitHub compare API; None if it cannot decide."""
    probe = None
    if main_sha:
        probe = api_compare(repo, main_sha, gitlink)
        if probe is None:
            return None
        if probe == "unknown":
            return "fail", f"is unknown to {repo} (unpushed or dangling commit?)"
    indeterminate = False

    def contains(name: str, sha: str) -> bool:
        nonlocal indeterminate
        status = api_compare(repo, name, sha)
        if status is None:
            indeterminate = True
        return status in ("identical", "behind")

    result = verdict(
        heads,
        gitlink,
        main_sha,
        contains,
        lambda: probe in ("identical", "ahead"),
        fork_pr,
    )
    if result[0] != "ok" and indeterminate:
        return None
    return result


def check(path: str, override_sha: str | None, rev: str | None) -> str:
    pattern = VERSION_BRANCH
    gitlink = override_sha or gitlink_at(path, rev)
    if gitlink is None:
        return "ok"
    main_sha = main_gitlink(path)
    if main_sha is None:
        print(f"{path}: cannot resolve origin/main gitlink", file=sys.stderr)
        return "fail"
    if gitlink == main_sha:
        return "ok"
    url = submodule_url(path, rev)
    if url is None:
        print(f"{path}: no url in .gitmodules", file=sys.stderr)
        return "fail"
    repo = github_repo(url)
    if repo is None:
        print(
            f"{path}: url {url} is not https://github.com/<owner>/<repo>",
            file=sys.stderr,
        )
        return "fail"
    heads = version_branches(url, pattern)
    if heads is None:
        print(
            f"{path}: cannot list version branches on {url} (offline?); "
            "rerun with network or SKIP=check-submodule-pointers",
            file=sys.stderr,
        )
        return "fail"
    if not heads:
        print(f"{path}: {url} has no version branch", file=sys.stderr)
        return "fail"
    fork_pr = lambda: open_fork_pr(repo, gitlink)
    try:
        result = validate_local(path, gitlink, main_sha, heads, fork_pr)
        if result is None:
            result = validate_api(repo, gitlink, main_sha, heads, fork_pr)
    except Unreachable as e:
        print(
            f"{path}: {e}; rerun with network or SKIP=check-submodule-pointers",
            file=sys.stderr,
        )
        return "fail"
    if result is None:
        print(
            f"{path}: cannot validate {gitlink} (no local clone and the "
            "GitHub API is unreachable); rerun with network or "
            "SKIP=check-submodule-pointers",
            file=sys.stderr,
        )
        return "fail"
    status, detail = result
    if status == "pending":
        print(f"{path}: gitlink {gitlink} is pending: {detail}")
    elif status == "fail":
        print(
            f"{path}: gitlink {gitlink} {detail}. Point the submodule at the "
            "head of a fork PR into the current version branch, or at a "
            "commit on it.",
            file=sys.stderr,
        )
    return status


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--path", help="check only this submodule path")
parser.add_argument("--sha", help="validate this sha instead of the staged gitlink")
parser.add_argument("--rev", help="check the gitlinks of this commit, not the index")
parser.add_argument(
    "--base",
    help="check only the gitlinks that differ from this commit "
    "(default: the merge base with origin/main)",
)
parser.add_argument(
    "--allow-pending",
    action="store_true",
    help="pass a gitlink that is the head of an open fork PR",
)
args, _ = parser.parse_known_args()

if args.path:
    paths = [args.path]
else:
    base = args.base or git("merge-base", args.rev or "HEAD", "origin/main")
    paths = checked_paths(args.rev, base)
    if paths is None:
        print(f"cannot list the gitlinks of {args.rev or 'the index'}", file=sys.stderr)
        sys.exit(1)
results = [check(path, args.sha, args.rev) for path in paths]
if "fail" in results:
    sys.exit(1)
if "pending" in results and not args.allow_pending:
    sys.exit(3)
sys.exit(0)
