#!/usr/bin/env python3
import argparse
import io
import json
import re
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path

import diff

ROOT = Path(__file__).resolve().parents[2]
ARTIFACT = "coverage-nightly"
IGNORE = "-ignore-filename-regex=(^|/)(tests|build[^/]*|_deps)/|^third_party/.*/(test|testing)/"
SOURCE = re.compile(r"\.(c|cc|cpp|cxx)$")
DUCKDB_FAMILY = re.compile(r"^(duckdb_.*|database-connector)$")
OWN = ("server", "iresearch", "duckdb")
CATEGORIES = ("server", "iresearch", "duckdb", "re2", "other")
KINDS = ("lines", "functions", "branches")
COUNTS = {"LH": ("lines", 0), "LF": ("lines", 1), "FNH": ("functions", 0), "FNF": ("functions", 1),
          "BRH": ("branches", 0), "BRF": ("branches", 1)}
SPLIT_LINES = 20000
SUBDIR_LINES = 1000
WARN_POINTS = 1.0
WARN_LINES = 20
WARN_TOTAL_POINTS = 0.2
HEADER = ["| {} | lines | Δ | functions | branches | uncovered lines |",
          "| --- | ---: | ---: | ---: | ---: | ---: |"]


def unwrap(rest):
    if rest[:1] == ["src"]:
        rest = rest[1:]
    return rest[1:] if rest[:1] == ["include"] else rest


def place(path):
    parts = path.split("/")
    if parts[0] in ("server", "iresearch"):
        return parts[0], parts[0], parts[1:]
    if parts[0] != "third_party" or len(parts) < 3 or "third_party" in parts[2:]:
        return None
    project, rest = parts[1], parts[2:]
    if project == "duckdb":
        if rest[0] == "extension" and len(rest) > 2:
            return "duckdb", f"duckdb/extension/{rest[1]}", unwrap(rest[2:])
        if rest[:2] == ["src", "include"]:
            rest = rest[3:] if rest[2:3] == ["duckdb"] and len(rest) > 4 else ["main", rest[-1]]
        elif rest[0] == "src":
            rest = rest[1:]
        return "duckdb", project, rest
    category = "duckdb" if DUCKDB_FAMILY.match(project) else "re2" if project == "re2" else "other"
    return category, project, unwrap(rest)


def locate(path):
    found = place(path)
    if not found:
        return None
    category, project, rest = found
    component = project
    if project in OWN and len(rest) > 1:
        component, rest = f"{project}/{rest[0]}", rest[1:]
    return category, project, component, rest[0] if len(rest) > 1 else ""


def instrumented(lines):
    projects = set()
    for line in lines:
        if line.startswith("SF:"):
            path = diff.normalize(line[3:].strip())
            found = place(path)
            if found and SOURCE.search(path):
                projects.add(found[1])
    return projects


def collect(text, write):
    projects = instrumented(io.StringIO(text))
    components = {}
    targets = None
    for line in io.StringIO(text):
        if line.startswith("SF:"):
            path = diff.normalize(line[3:].strip())
            where = locate(path)
            targets = None
            if where and where[1] in projects:
                category, _, component, subdir = where
                entry = components.setdefault(component, {"category": category, "subdirs": {}})
                targets = (entry, entry["subdirs"].setdefault(subdir, {}))
                line = f"SF:{path}\n"
        if targets is None:
            continue
        write(line)
        key, _, value = line.rstrip("\n").partition(":")
        if key in COUNTS:
            kind, index = COUNTS[key]
            for target in targets:
                target.setdefault(kind, [0, 0])[index] += int(value)
    return components


def summed(entries):
    total = {kind: [0, 0] for kind in KINDS}
    for entry in entries:
        for kind in KINDS:
            hit, found = entry.get(kind, (0, 0))
            total[kind][0] += hit
            total[kind][1] += found
    return total


def rows(meta):
    components = meta["components"]
    out = [("*", "all", summed(components.values()), "total")]
    for category in CATEGORIES:
        names = sorted(n for n, c in components.items() if c["category"] == category)
        if not names:
            continue
        out.append((f"[{category}]", category, summed(components[n] for n in names), "category"))
        if names == [category]:
            continue
        for name in names:
            entry = components[name]
            out.append((name, f"{name} (files)" if name in CATEGORIES else name, entry, "component"))
            if entry.get("lines", (0, 0))[1] < SPLIT_LINES:
                continue
            subdirs = entry["subdirs"]
            shown = sorted(s for s in subdirs if s and subdirs[s].get("lines", (0, 0))[1] >= SUBDIR_LINES)
            if not shown:
                continue
            out += [(f"{name}/{s}", f"{name}/{s}", subdirs[s], "subdir") for s in shown]
            rest = [subdirs[s] for s in subdirs if s not in shown]
            if rest:
                out.append((f"{name} (rest)", f"{name} (rest)", summed(rest), "subdir"))
    return out


def percent(counts):
    hit, found = counts.get("lines", (0, 0)) if isinstance(counts, dict) else counts
    return 100 * hit / found if found else None


def shown(value):
    return f"{value:.1f}%" if value is not None else "-"


def uncovered(counts):
    hit, found = counts.get("lines", (0, 0))
    return found - hit


def compare(meta, previous):
    before = {key: counts for key, _, counts, _ in rows(previous)} if previous else {}
    table = []
    warnings = []
    for key, label, counts, level in rows(meta):
        old = before.get(key)
        now, then = percent(counts), percent(old) if old else None
        delta = now - then if now is not None and then is not None else None
        grown = uncovered(counts) - uncovered(old) if old else 0
        table.append((label, counts, delta, level))
        if delta is None:
            continue
        if level == "total" and delta <= -WARN_TOTAL_POINTS:
            warnings.append(f"all: lines {shown(now)} ({delta:+.1f} since {previous['date']}), "
                            f"{grown} more uncovered lines")
        elif level == "component" and delta <= -WARN_POINTS and grown >= WARN_LINES:
            warnings.append(f"{label}: lines {shown(now)} ({delta:+.1f} since {previous['date']}), "
                            f"{grown} more uncovered lines")
    return table, warnings


def table_row(label, counts, delta):
    functions = counts.get("functions", (0, 0))
    branches = counts.get("branches", (0, 0))
    return (f"| {label} | {shown(percent(counts))} | {f'{delta:+.1f}' if delta is not None else ''} | "
            f"{shown(percent(functions))} | {shown(percent(branches))} | {uncovered(counts)} |")


def render(meta, previous):
    table, warnings = compare(meta, previous)
    against = (f"against the nightly of {previous['date']} (`{previous['sha'][:9]}`)" if previous
               else "no earlier nightly coverage to compare with")
    lines = [f"**Coverage** of `{meta['sha'][:9]}` ({meta['label']}), {against}", ""]
    lines += [f"- ⚠ {w}" for w in warnings] + ([""] if warnings else [])
    lines += [HEADER[0].format(""), HEADER[1]]
    lines += [table_row(label, counts, delta) for label, counts, delta, level in table
              if level in ("total", "category")]
    details = defaultdict(list)
    category = None
    for label, counts, delta, level in table:
        if level == "category":
            category = label
        elif level in ("component", "subdir"):
            details[category].append((table_row(label, counts, delta), level))
    for category, found in details.items():
        components = sum(level == "component" for _, level in found)
        lines += ["", f"<details><summary>{category}: {components} components</summary>", "",
                  HEADER[0].format("component"), HEADER[1], *(row for row, _ in found), "", "</details>"]
    lines += ["", f"`scripts/coverage/nightly.py show <component or path>` lists the uncovered lines from "
                  f"the run's `{ARTIFACT}` artifact"]
    return "\n".join(lines) + "\n", warnings


def artifacts(repo):
    out = subprocess.run(["gh", "api", f"repos/{repo}/actions/artifacts?name={ARTIFACT}&per_page=100", "--jq",
                          ".artifacts[] | select(.expired | not) "
                          "| [.workflow_run.id, .workflow_run.head_branch, .created_at] | @tsv"],
                         capture_output=True, text=True, check=True).stdout
    return [line.split("\t") for line in out.splitlines() if line]


def latest_run(found, exclude=None):
    runs = sorted((r for r in found if r[1] == "main" and r[0] != str(exclude)), key=lambda r: r[2],
                  reverse=True)
    return runs[0][0] if runs else None


def download(repo, run, dest):
    subprocess.run(["gh", "run", "download", str(run), "-R", repo, "-n", ARTIFACT, "-D", str(dest)],
                   check=True)


def previous_meta(repo, exclude):
    try:
        run = latest_run(artifacts(repo), exclude)
        if not run:
            return None
        with tempfile.TemporaryDirectory() as tmp:
            download(repo, run, tmp)
            return json.loads((Path(tmp) / "components.json").read_text())
    except (subprocess.CalledProcessError, OSError, ValueError) as e:
        print(f"warning: no earlier nightly coverage to compare with: {e}", file=sys.stderr)
        return None


def cmd_report(args):
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    profdata = diff.merge(args.profiles, out / "merged.profdata")
    text = diff.export("lcov", profdata, diff.objects(args.build_dir), extra=[IGNORE])
    profdata.unlink()
    with subprocess.Popen(["zstd", "-q", "-19", "-T0", "-f", "-o", str(out / "coverage.lcov.zst")],
                          stdin=subprocess.PIPE, text=True) as zstd:
        components = collect(text, zstd.stdin.write)
        zstd.stdin.close()
    if zstd.returncode:
        raise SystemExit("zstd failed")
    meta = {"run": args.run, "sha": args.sha, "label": args.label, "components": components,
            "date": time.strftime("%Y-%m-%d", time.gmtime())}
    (out / "components.json").write_text(json.dumps(meta, indent=1, sort_keys=True))
    print(render(meta, None)[0])


def cmd_render(args):
    meta = json.loads((Path(args.current) / "components.json").read_text())
    if args.previous:
        previous = json.loads((Path(args.previous) / "components.json").read_text())
    else:
        previous = previous_meta(args.repo, args.exclude_run) if args.repo else None
    body, warnings = render(meta, previous)
    Path(args.out).write_text(body)
    if args.warnings:
        Path(args.warnings).write_text("".join(f"⚠ {w}\n" for w in warnings))
    print(body)


def matches(path, what):
    if path == what or path.startswith(what.rstrip("/") + "/"):
        return True
    where = locate(path)
    return bool(where) and what in (where[2], f"{where[2]}/{where[3]}")


def gaps(lines, what):
    files = {}
    current = None
    for line in lines:
        if line.startswith("SF:"):
            path = line[3:].strip()
            current = None
            if matches(path, what):
                current = files.setdefault(path, {"lines": [0, 0], "missed": [], "branches": 0})
        elif current is None:
            continue
        elif line.startswith("DA:"):
            number, count = line[3:].split(",")[:2]
            if int(count) == 0:
                current["missed"].append(int(number))
        elif line.startswith("BRDA:"):
            if line.rstrip("\n").rsplit(",", 1)[1] in ("-", "0"):
                current["branches"] += 1
        elif line.startswith(("LF:", "LH:")):
            current["lines"][line.startswith("LF:")] += int(line[3:])
    return files


def cmd_show(args):
    run = args.run or latest_run(artifacts(args.repo))
    if not run:
        raise SystemExit("no nightly coverage of main is left: the artifacts expire after 30 days")
    cache = ROOT / "out" / f"{ARTIFACT}-{run}"
    if not (cache / "coverage.lcov.zst").is_file():
        download(args.repo, run, cache)
    meta = json.loads((cache / "components.json").read_text())
    print(f"nightly {meta['date']} · {meta['sha'][:9]} · run {run} ({meta['label']})")
    what = args.what.rstrip("/")
    with subprocess.Popen(["zstd", "-dc", str(cache / "coverage.lcov.zst")], stdout=subprocess.PIPE,
                          text=True) as unzstd:
        files = gaps(unzstd.stdout, what)
    if not files:
        raise SystemExit(f"nothing in the report matches {what}")
    total = summed({"lines": f["lines"]} for f in files.values())
    print(f"{what}: lines {shown(percent(total))} of {total['lines'][1]}, "
          f"{uncovered(total)} uncovered, in {len(files)} files")
    groups = defaultdict(lambda: [0, 0])
    for path, f in files.items():
        _, _, component, subdir = locate(path) or (None, None, path.rsplit("/", 1)[0], "")
        group = groups[f"{component}/{subdir}" if subdir else f"{component} (files)"]
        group[0] += f["lines"][0]
        group[1] += f["lines"][1]
    if len(groups) > 1:
        for name, counts in sorted(groups.items(), key=lambda kv: kv[1][0] - kv[1][1]):
            print(f"  {name}: {shown(percent(counts))} of {counts[1]}, {counts[1] - counts[0]} uncovered")
    ranked = sorted(files.items(), key=lambda kv: -len(kv[1]["missed"]))
    for path, f in ranked[:args.limit]:
        if f["missed"] or f["branches"]:
            print(f"{path}  {len(f['missed'])} uncovered lines: {diff.ranges(f['missed']) or '-'}; "
                  f"{f['branches']} branch directions never taken")
    if len(ranked) > args.limit:
        print(f"... {len(ranked) - args.limit} more files (--limit)")


def main():
    ap = argparse.ArgumentParser(description="Coverage of the whole system, by component, from the "
                                             "nightly's dev profiles.")
    sub = ap.add_subparsers(required=True)
    p = sub.add_parser("report", help="merge the profiles, export every line's hits and count them by "
                                      "component")
    p.add_argument("--build-dir", required=True, help="the coverage build (SDB_COVERAGE=On) or its bundle")
    p.add_argument("--profiles", nargs="+", required=True)
    p.add_argument("--label", required=True)
    p.add_argument("--run", required=True)
    p.add_argument("--sha", required=True)
    p.add_argument("--out", required=True)
    p.set_defaults(fn=cmd_report)
    p = sub.add_parser("render", help="the report's Markdown, against the last earlier nightly of main")
    p.add_argument("--current", required=True, help="the directory with this run's components.json")
    p.add_argument("--previous", help="the directory with an earlier components.json")
    p.add_argument("--repo", help="find the earlier one among this repository's artifacts")
    p.add_argument("--exclude-run", help="the run of --current, which is not an earlier one")
    p.add_argument("--out", required=True)
    p.add_argument("--warnings", help="also write each warning as a line here")
    p.set_defaults(fn=cmd_render)
    p = sub.add_parser("show", help="the uncovered lines of a component, subdirectory or path, from the "
                                    "latest nightly of main")
    p.add_argument("what", help="a component (duckdb/common), its subdirectory (duckdb/common/types) or "
                                "a path prefix (third_party/duckdb/src/common)")
    p.add_argument("--run", help="a nightly run id; the latest one by default")
    p.add_argument("--repo", default="serenedb/serenedb")
    p.add_argument("--limit", type=int, default=40, help="list at most this many files")
    p.set_defaults(fn=cmd_show)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
