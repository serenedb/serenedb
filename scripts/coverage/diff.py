#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = re.compile(r"\.(c|cc|cpp|cxx|h|hh|hpp|hxx|ipp|tpp|inc)$")
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")


def git(*args, cwd=ROOT):
    return subprocess.run(["git", "-C", str(cwd), *args], capture_output=True, text=True,
                          check=True).stdout


def changed_lines(base):
    lines = defaultdict(set)

    def collect(diff, prefix=""):
        current = None
        for line in diff.splitlines():
            if line.startswith("+++ "):
                path = line[4:]
                current = prefix + path[2:] if path.startswith("b/") else None
                if current and not SOURCE.search(current):
                    current = None
            elif current:
                m = HUNK.match(line)
                if m:
                    start, count = int(m.group(1)), int(m.group(2) or 1)
                    lines[current].update(range(start, start + count))

    merge_base = git("merge-base", base, "HEAD").strip()
    collect(git("diff", "-U0", "--no-renames", "--diff-filter=AM", merge_base, "HEAD"))
    for entry in git("diff", "--raw", "--no-renames", merge_base, "HEAD").splitlines():
        meta, _, path = entry.partition("\t")
        fields = meta.split()
        if len(fields) < 4 or fields[0] != ":160000" or fields[1] != "160000":
            continue
        old, new = fields[2], fields[3]
        sub = ROOT / path
        try:
            git("cat-file", "-e", old, cwd=sub)
        except subprocess.CalledProcessError:
            subprocess.run(["git", "-C", str(sub), "fetch", "-q", "--depth", "1", "origin", old],
                           check=False)
        try:
            collect(git("diff", "-U0", "--no-renames", "--diff-filter=AM", old, new, cwd=sub),
                    prefix=path + "/")
        except subprocess.CalledProcessError as e:
            print(f"warning: cannot diff {path} {old}..{new}: {e.stderr.strip()}", file=sys.stderr)
    return lines


def export(fmt, profdata, objects, sources):
    cmd = ["llvm-cov-23", "export", f"-format={fmt}", f"-instr-profile={profdata}", objects[0]]
    for o in objects[1:]:
        cmd += ["-object", o]
    cmd += ["-sources", *sources]
    res = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if res.returncode != 0:
        raise SystemExit(f"llvm-cov export failed: {res.stderr[-2000:]}")
    return res.stdout


def parse_lcov(text):
    files = {}
    current = None
    for line in text.splitlines():
        if line.startswith("SF:"):
            current = files.setdefault(line[3:], {"lines": {}, "branches": defaultdict(list),
                                                  "functions": {}})
        elif current is None:
            continue
        elif line.startswith("DA:"):
            no, count = line[3:].split(",")[:2]
            current["lines"][int(no)] = max(current["lines"].get(int(no), 0), int(count))
        elif line.startswith("BRDA:"):
            no, _block, _branch, taken = line[5:].split(",")
            current["branches"][int(no)].append(0 if taken == "-" else int(taken))
        elif line.startswith("FN:"):
            no, name = line[3:].split(",", 1)
            current["functions"].setdefault(name, [int(no), 0])
        elif line.startswith("FNDA:"):
            count, name = line[5:].split(",", 1)
            if name in current["functions"]:
                current["functions"][name][1] = max(current["functions"][name][1], int(count))
        elif line == "end_of_record":
            current = None
    return files


def normalize(path):
    p = Path(path)
    if p.is_absolute():
        try:
            return str(p.resolve().relative_to(ROOT))
        except ValueError:
            return str(p)
    return str(Path(*[part for part in p.parts if part != "."]))


def source_line(path, no):
    try:
        with open(ROOT / path, errors="replace") as fh:
            for i, text in enumerate(fh, 1):
                if i == no:
                    return text.rstrip("\n")[:200]
    except OSError:
        pass
    return ""


def ranges(numbers):
    out = []
    for n in sorted(numbers):
        if out and n == out[-1][1] + 1:
            out[-1][1] = n
        else:
            out.append([n, n])
    return ",".join(f"{a}-{b}" if a != b else f"{a}" for a, b in out)


def main():
    ap = argparse.ArgumentParser(description="Coverage of the lines a change touches.")
    ap.add_argument("--profdata", required=True)
    ap.add_argument("--objects", nargs="+", required=True)
    ap.add_argument("--base", default="origin/main")
    ap.add_argument("--label", default="coverage")
    ap.add_argument("--out", required=True, help="output directory")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    changed = changed_lines(args.base)
    sources = [p for p in changed if (ROOT / p).is_file()]
    summary = {"lines": [0, 0], "functions": [0, 0], "branches": [0, 0]}
    details = []
    uncovered_ranges = {}
    if sources:
        report = {normalize(k): v for k, v in parse_lcov(export("lcov", args.profdata, args.objects,
                                                                  sources)).items()}
        for path in sorted(sources):
            data = report.get(path)
            if not data:
                continue
            touched = changed[path]
            missed = []
            for no in sorted(touched & set(data["lines"])):
                summary["lines"][1] += 1
                if data["lines"][no] > 0:
                    summary["lines"][0] += 1
                else:
                    missed.append(no)
                    details.append({"kind": "line", "file": path, "line": no,
                                    "source": source_line(path, no)})
            for no in sorted(touched & set(data["branches"])):
                for idx, taken in enumerate(data["branches"][no]):
                    summary["branches"][1] += 1
                    if taken > 0:
                        summary["branches"][0] += 1
                    else:
                        details.append({"kind": "branch", "file": path, "line": no,
                                        "direction": "true" if idx % 2 == 0 else "false",
                                        "source": source_line(path, no)})
            for name, (start, count) in data["functions"].items():
                if start in touched:
                    summary["functions"][1] += 1
                    if count > 0:
                        summary["functions"][0] += 1
                    else:
                        details.append({"kind": "function", "file": path, "line": start,
                                        "function": name, "source": source_line(path, start)})
            if missed:
                uncovered_ranges[path] = ranges(missed)
    text = (f"{args.label}: lines {summary['lines'][0]}/{summary['lines'][1]}, "
            f"functions {summary['functions'][0]}/{summary['functions'][1]}, "
            f"branches {summary['branches'][0]}/{summary['branches'][1]}")
    if uncovered_ranges:
        text += "\nuncovered: " + "  ".join(f"{p}:{r}" for p, r in uncovered_ranges.items())
    (out / "summary.txt").write_text(text + "\n")
    (out / "coverage.json").write_text(json.dumps({"summary": summary, "uncovered": details},
                                                  indent=1))
    print(text)


if __name__ == "__main__":
    main()
