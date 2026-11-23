#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH = os.path.join(ROOT, "resources", "contributors.txt")


def git(*args):
    return subprocess.run(["git", "-C", ROOT, *args], check=True,
                          capture_output=True, text=True).stdout


def ordered(names):
    return sorted(set(names), key=lambda name: (name.casefold(), name))


def authors():
    if git("rev-parse", "--is-shallow-repository").strip() == "true":
        sys.exit("contributors.py reads the whole history: run git fetch --unshallow")
    names = (line.strip() for line in git("log", "--format=%aN", "HEAD").splitlines())
    return ordered(name for name in names if name and not name.endswith("[bot]"))


def problems(names):
    found = []
    if names != ordered(names):
        found.append("names are not sorted and unique")
    if any(not name or name != name.strip() for name in names):
        found.append("a name is blank or padded")
    if any(name.endswith("[bot]") for name in names):
        found.append("a bot is listed")
    return found


def main():
    parser = argparse.ArgumentParser(
        description="Write resources/contributors.txt, the commit authors "
        "sdb_contributors lists, from the git history and .mailmap.")
    parser.add_argument("--check", action="store_true",
                        help="check the file's form, without reading the history")
    args = parser.parse_args()
    if args.check:
        with open(PATH, encoding="utf-8") as f:
            found = problems(f.read().splitlines())
        for problem in found:
            print(f"{PATH}: {problem}", file=sys.stderr)
        return 1 if found else 0
    with open(PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(authors()) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
