#!/usr/bin/env python3
"""Pre-commit hook: validate third_party submodule gitlinks.

For each checked submodule the staged gitlink must
  1. be reachable from one of the fork's version branches (vYYYY.MM.DD),
     i.e. the fork-side PR is merged -- not a feature-branch head, and
  2. not be older than origin/main's gitlink: its newest containing
     version branch must compare >= main's (branch names order by date),
     and within the same branch it must descend from main's gitlink.
     Switching to a newer version branch is fine even though rebased
     lineages share no ancestry.

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
import urllib.error
import urllib.request

CHECKED = {
    "third_party/duckdb": re.compile(r"^v20\d{2}\.\d{2}\.\d{2}$"),
}


def git(*args: str, cwd: str | None = None) -> str | None:
    proc = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True
    )
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def staged_gitlink(path: str) -> str | None:
    out = git("ls-files", "-s", "--", path)
    if not out:
        return None
    mode, sha = out.split()[:2]
    return sha if mode == "160000" else None


def main_gitlink(path: str) -> str | None:
    sha = git("rev-parse", f"origin/main:{path}")
    if sha:
        return sha
    if git("fetch", "--quiet", "--depth=1", "origin", "main") is None:
        return None
    return git("rev-parse", f"FETCH_HEAD:{path}")


def submodule_url(path: str) -> str | None:
    return git("config", "-f", ".gitmodules", f"submodule.{path}.url")


def version_branches(url: str, pattern: re.Pattern) -> dict[str, str] | None:
    out = git("ls-remote", "--heads", url)
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
) -> tuple[bool, str]:
    """Shared decision; `contains(branch, sha)` and `main_is_ancestor()`
    abstract over the local-git and GitHub-API backends."""
    names = sorted(heads, reverse=True)
    ver_new = next((n for n in names if contains(n, gitlink)), None)
    if ver_new is None:
        return False, "not reachable from any version branch"
    ver_main = next((n for n in names if contains(n, main_sha)), None)
    if ver_main is None or ver_new > ver_main:
        return True, f"on {ver_new}"
    if ver_new < ver_main:
        return (
            False,
            f"on {ver_new}, older than main's gitlink branch {ver_main}",
        )
    if main_is_ancestor():
        return True, f"on {ver_new}"
    return False, f"on {ver_new} but behind main's gitlink {main_sha}"


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
    path: str, gitlink: str, main_sha: str, heads: dict[str, str]
) -> tuple[bool, str] | None:
    """Validate with the local submodule clone; None if it cannot decide."""
    if git("rev-parse", "--git-dir", cwd=path) is None:
        return None
    needed = [gitlink, main_sha, *heads.values()]
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
    )


def github_repo(url: str) -> str | None:
    m = re.search(r"github\.com[:/](.+?)(?:\.git)?$", url)
    return m.group(1) if m else None


def api_compare(repo: str, base: str, head: str) -> str | None:
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/compare/{base}...{head}",
        headers={"Accept": "application/vnd.github+json"},
    )
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp).get("status")
    except urllib.error.HTTPError as e:
        return "unknown" if e.code == 404 else None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return None


def validate_api(
    url: str, gitlink: str, main_sha: str, heads: dict[str, str]
) -> tuple[bool, str] | None:
    """Validate through the GitHub compare API; None if it cannot decide."""
    repo = github_repo(url)
    if repo is None:
        return None
    probe = api_compare(repo, main_sha, gitlink)
    if probe is None:
        return None
    if probe == "unknown":
        return False, f"is unknown to {repo} (unpushed or dangling commit?)"
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
    )
    if not result[0] and indeterminate:
        return None
    return result


def check(path: str, pattern: re.Pattern, override_sha: str | None) -> bool:
    gitlink = override_sha or staged_gitlink(path)
    if gitlink is None:
        return True
    main_sha = main_gitlink(path)
    if main_sha is None:
        print(f"{path}: cannot resolve origin/main gitlink", file=sys.stderr)
        return False
    if gitlink == main_sha:
        return True
    url = submodule_url(path)
    if url is None:
        print(f"{path}: no url in .gitmodules", file=sys.stderr)
        return False
    heads = version_branches(url, pattern)
    if not heads:
        print(
            f"{path}: cannot list version branches on {url} (offline?); "
            "rerun with network or SKIP=check-submodule-pointers",
            file=sys.stderr,
        )
        return False
    result = validate_local(path, gitlink, main_sha, heads)
    if result is None:
        result = validate_api(url, gitlink, main_sha, heads)
    if result is None:
        print(
            f"{path}: cannot validate {gitlink} (no local clone and the "
            "GitHub API is unreachable); rerun with network or "
            "SKIP=check-submodule-pointers",
            file=sys.stderr,
        )
        return False
    ok, detail = result
    if not ok:
        print(
            f"{path}: gitlink {gitlink} {detail}. Merge the fork-side PR "
            "into the current version branch and point the submodule at "
            "the merged commit.",
            file=sys.stderr,
        )
    return ok


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--path", help="check only this submodule path")
parser.add_argument("--sha", help="validate this sha instead of the staged gitlink")
args, _ = parser.parse_known_args()

paths = {args.path: CHECKED[args.path]} if args.path else CHECKED
failed = False
for path, pattern in paths.items():
    if not check(path, pattern, args.sha):
        failed = True

sys.exit(1 if failed else 0)
