#!/usr/bin/env python3
"""Stage or unstage specific line ranges of a tracked file.

The scriptable equivalent of an editor's "stage selected lines" / "unstage
selected lines" -- line-level `git add -p` without the interactive prompt. It
takes the relevant diff, keeps only the changes on the requested lines, and
applies that minimal patch to the index. The working tree is never touched.

Usage:
    scripts/git_stage_lines.py stage   <file> <ranges>...
    scripts/git_stage_lines.py unstage <file> <ranges>...

Ranges are 1-based and inclusive; the forms below may be mixed and comma- or
space-separated:
    40-50      lines 40 through 50
    25         line 25
    40-        line 40 to end of file
    -10        start of file to line 10
    12,18,30-33

Line numbers refer to the file you are looking at:
  * stage   -- working-tree lines (what Read shows / what you just edited);
               only unstaged changes on those lines get staged.
  * unstage -- staged (index) lines; only staged changes on those lines are
               removed from the index. With no further unstaged edits on top,
               these are the same numbers you see on disk.

A deleted line has no number of its own on the side you select, so it is
attached to the change block it belongs to: selecting the line a deletion sits
at -- for a one-line modification, the changed line itself -- moves it too.

Options:
    -n, --dry-run   print the patch that would be applied; apply nothing
    -v, --verbose   print the patch, then apply it
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys

HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


def run_git(args, *, check=True, stdin=None):
    proc = subprocess.run(
        ["git", *args],
        input=stdin,
        capture_output=True,
        text=True,
    )
    if check and proc.returncode != 0:
        if proc.stdout:
            sys.stderr.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        sys.exit(proc.returncode or 1)
    return proc


def parse_ranges(tokens):
    ranges = []
    for tok in tokens:
        for part in re.split(r"[,\s]+", tok.strip()):
            if not part:
                continue
            m = re.fullmatch(r"(\d+)?-(\d+)?|(\d+)", part)
            if not m or part == "-":
                sys.exit(f"error: invalid line range '{part}'")
            if m.group(3) is not None:
                lo = hi = int(m.group(3))
            else:
                lo = int(m.group(1)) if m.group(1) else 1
                hi = int(m.group(2)) if m.group(2) else sys.maxsize
            if lo > hi:
                sys.exit(f"error: invalid line range '{part}' (start > end)")
            ranges.append((lo, hi))
    if not ranges:
        sys.exit("error: no line ranges given")
    return ranges


def in_ranges(line, ranges):
    return any(lo <= line <= hi for lo, hi in ranges)


def split_hunks(diff_text):
    lines = diff_text.splitlines(keepends=True)
    i = 0
    preamble = []
    while i < len(lines) and not lines[i].startswith("@@"):
        preamble.append(lines[i])
        i += 1
    hunks = []
    while i < len(lines):
        header = lines[i]
        i += 1
        body = []
        while i < len(lines) and not lines[i].startswith("@@"):
            body.append(lines[i])
            i += 1
        hunks.append((header, body))
    return preamble, hunks


def prefix_of(raw):
    if raw and raw[0] in " +-\\":
        return raw[0]
    return " "


def filter_hunk(header, body, ranges, side):
    m = HUNK_RE.match(header)
    if not m:
        sys.exit(f"error: cannot parse hunk header: {header!r}")
    old = int(m.group(1))
    new = int(m.group(3))

    recs = []
    for raw in body:
        p = prefix_of(raw)
        recs.append({"p": p, "raw": raw, "o": old, "n": new})
        if p == " ":
            old += 1
            new += 1
        elif p == "-":
            old += 1
        elif p == "+":
            new += 1

    idx = 0
    while idx < len(recs):
        if recs[idx]["p"] not in "+-":
            idx += 1
            continue
        start = idx
        while idx < len(recs) and recs[idx]["p"] in "+-":
            idx += 1
        block_o = recs[start]["o"]
        block_n = recs[start]["n"]
        for r in recs[start:idx]:
            r["block_o"] = block_o
            r["block_n"] = block_n

    out = []
    kept_prev = True
    for r in recs:
        p = r["p"]
        if p == "\\":
            if kept_prev:
                out.append(r["raw"])
            continue
        if p == " ":
            out.append(r["raw"])
            kept_prev = True
            continue
        if p == "+":
            pos = r["n"] if side == "new" else r["block_o"]
        else:
            pos = r["o"] if side == "old" else r["block_n"]
        if in_ranges(pos, ranges):
            out.append(r["raw"])
            kept_prev = True
        elif p == "+":
            kept_prev = False
        else:
            out.append(" " + r["raw"][1:])
            kept_prev = True

    if not any(prefix_of(l) in "+-" for l in out):
        return None

    old_len = sum(1 for l in out if prefix_of(l) in " -")
    new_len = sum(1 for l in out if prefix_of(l) in " +")
    new_header = f"@@ -{int(m.group(1))},{old_len} +{int(m.group(3))},{new_len} @@\n"
    return new_header + "".join(out)


def build_patch(diff_args, ranges, side):
    diff = run_git(diff_args).stdout
    if not diff.strip():
        return None
    preamble, hunks = split_hunks(diff)
    kept = [filter_hunk(h, b, ranges, side) for h, b in hunks]
    kept = [k for k in kept if k is not None]
    if not kept:
        return ""
    return "".join(preamble) + "".join(kept)


def is_untracked(path):
    out = run_git(["status", "--porcelain", "--", path]).stdout
    return out.startswith("?? ")


def main():
    parser = argparse.ArgumentParser(
        description="Stage/unstage specific line ranges of a tracked file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name, aliases, help_text in (
        ("stage", ["add"], "stage changes on the given working-tree lines"),
        ("unstage", ["reset"], "unstage changes on the given staged lines"),
    ):
        sp = sub.add_parser(name, aliases=aliases, help=help_text)
        sp.add_argument("file")
        sp.add_argument("ranges", nargs="+", help="e.g. 40-50 25 60-")
        sp.add_argument("-n", "--dry-run", action="store_true",
                        help="print the patch; apply nothing")
        sp.add_argument("-v", "--verbose", action="store_true",
                        help="print the patch, then apply it")

    args = parser.parse_args()
    cmd = "stage" if args.cmd in ("stage", "add") else "unstage"
    ranges = parse_ranges(args.ranges)

    if cmd == "stage":
        diff_args = ["diff", "--no-color", "--no-ext-diff", "--no-textconv",
                     "--", args.file]
        side = "new"
    else:
        diff_args = ["diff", "--cached", "-R", "--no-color", "--no-ext-diff",
                     "--no-textconv", "--", args.file]
        side = "old"

    patch = build_patch(diff_args, ranges, side)

    if patch is None:
        if cmd == "stage" and is_untracked(args.file):
            sys.exit(
                f"error: '{args.file}' is untracked; run `git add -N {args.file}` "
                "first, then stage the lines you want."
            )
        where = "unstaged" if cmd == "stage" else "staged"
        print(f"nothing to {cmd}: no {where} changes on {args.file}")
        return
    if patch == "":
        print(f"nothing to {cmd}: no changes on the requested lines of {args.file}")
        return

    if args.dry_run or args.verbose:
        sys.stdout.write(patch if patch.endswith("\n") else patch + "\n")
    if args.dry_run:
        return

    run_git(
        ["apply", "--cached", "--recount", "--whitespace=nowarn", "-"],
        stdin=patch,
    )
    def fmt(lo, hi):
        if hi == sys.maxsize:
            return f"{lo}-"
        return str(lo) if lo == hi else f"{lo}-{hi}"

    pretty = ", ".join(fmt(lo, hi) for lo, hi in ranges)
    print(f"{cmd}d {args.file} (lines {pretty})")


if __name__ == "__main__":
    main()
