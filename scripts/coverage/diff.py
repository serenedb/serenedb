#!/usr/bin/env python3
import argparse
import functools
import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = re.compile(r"\.(c|cc|cpp|cxx|h|hh|hpp|hxx|ipp|tpp|inc)$")
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")
ASSERTIONS = {"SDB_ASSERT", "SDB_VERIFY", "SDB_ENSURE", "SDB_UNREACHABLE", "D_ASSERT", "D_ASSERT_V",
              "ALWAYS_ASSERT", "DUCKDB_DEBUG_VERIFY_GUARD", "DUCKDB_DEBUG_VERIFY_ENABLED"}
UNREACHABLE = re.compile(r"\b(?:SDB_UNREACHABLE|std::unreachable|(?:std::)?abort)\(\)")
IDENTIFIER = re.compile(r"[A-Za-z_]\w*")
INTERNAL = re.compile(r"^([^:]+:)(_Z.*)$")
SWITCH = re.compile(r"\s*switch\b")
CASE = re.compile(r"\s*(?:case\b|default\s*:)")
FOREVER = re.compile(r"\s*(?:\}\s*)?while\s*\(\s*true\s*\)")
BRACE = re.compile(r"\s*\};?\s*$")
USING = re.compile(r"\s*using\b[^;]*;\s*$")
CODE, EXPANSION, MCDC_BRANCH = 0, 1, 6


def is_test(path):
    return path.startswith(("tests/", "resources/tests/")) or (
        path.startswith("third_party/") and ("/test/" in path or "/testing/" in path))


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


def objects(build_dir):
    build_dir = Path(build_dir)
    candidates = sorted((build_dir / "bin").iterdir()) if (build_dir / "bin").is_dir() else []
    found = []
    for b in [*candidates, build_dir / "third_party" / "duckdb" / "test" / "unittest"]:
        if not b.is_file():
            continue
        sections = subprocess.run(["llvm-readelf-23", "-S", str(b)], capture_output=True).stdout
        if b"__llvm_covmap" in sections:
            found.append(str(b))
    if not found:
        raise SystemExit(f"no coverage-instrumented binaries in {build_dir}")
    return found


def merge(profiles, dest):
    files = []
    for p in map(Path, profiles):
        if p.is_dir():
            files += sorted(str(f) for f in p.iterdir() if f.suffix in (".profraw", ".profdata"))
        else:
            files.append(str(p))
    if not files:
        raise SystemExit(f"no profiles in {' '.join(profiles)}")
    subprocess.run(["llvm-profdata-23", "merge", "-sparse", "-failure-mode=any", "-o", str(dest),
                    *files], check=True)
    return dest


def _with_objects(cmd, objects, sources):
    cmd.append(objects[0])
    for o in objects[1:]:
        cmd += ["-object", o]
    return cmd + (["-sources", *sources] if sources else [])


def export(fmt, profdata, objects, sources=(), extra=()):
    cmd = _with_objects(["llvm-cov-23", "export", f"-format={fmt}", f"-instr-profile={profdata}",
                         *extra], objects, sources)
    res = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if res.returncode != 0:
        raise SystemExit(f"llvm-cov export failed: {res.stderr[-2000:]}")
    return res.stdout


def html(profdata, objects, sources, dest):
    cmd = _with_objects(["llvm-cov-23", "show", "-format=html", f"-output-dir={dest}",
                         "-show-mcdc", "-show-branches=count", f"-instr-profile={profdata}"],
                        objects, sources)
    subprocess.run(cmd, check=True, cwd=ROOT)


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


def demangle(names):
    if not names:
        return {}
    res = subprocess.run(["llvm-cxxfilt-23"], input="\n".join(names), capture_output=True,
                         text=True)
    out = res.stdout.splitlines() if res.returncode == 0 else names
    return dict(zip(names, out))


def readable(names):
    parts = {n: m.groups() if (m := INTERNAL.match(n)) else ("", n) for n in names}
    plain = demangle(sorted({symbol for _, symbol in parts.values()}))
    return {n: prefix + plain.get(symbol, symbol) for n, (prefix, symbol) in parts.items()}


def parse_json(text):
    files = {}
    data = json.loads(text)["data"][0]
    for f in data["files"]:
        files[f["filename"]] = {"mcdc": {}, "functions": [], "excluded": set(), "code": []}
    names = demangle(sorted({fn["name"] for fn in data["functions"]}))
    for fn in data["functions"]:
        if not fn["regions"] or not fn["filenames"]:
            continue
        body = fn["regions"][0]
        entry = files.get(fn["filenames"][body[5]])
        if entry is not None:
            entry["functions"].append((body[0], body[2], names.get(fn["name"], fn["name"]),
                                       fn["name"]))
        exclude_assertions(fn, files)
        collect_mcdc(fn, files)
    return files


def macro_at(path, line, column):
    found = IDENTIFIER.match(source_text(path, line), column - 1)
    return found.group(0) if found else ""


def exclude_assertions(fn, files):
    paths, regions = fn["filenames"], fn["regions"]
    for r in regions:
        entry = files.get(paths[r[5]])
        if r[7] == CODE and entry is not None:
            entry["code"].append(r)
        if r[7] != EXPANSION:
            continue
        macro = macro_at(paths[r[5]], r[0], r[1])
        if macro not in ASSERTIONS:
            continue
        expanded = {r[6]}
        while nested := {e[6] for e in regions if e[7] == EXPANSION and e[5] in expanded} - expanded:
            expanded |= nested
        for e in regions:
            if e[5] in expanded and paths[e[5]] in files:
                files[paths[e[5]]]["excluded"].update(range(e[0], e[2] + 1))
        if entry is None:
            continue
        entry["excluded"].update(range(r[0], r[2] + 1))
        guarded = macro == "DUCKDB_DEBUG_VERIFY_GUARD" or (
            macro == "DUCKDB_DEBUG_VERIFY_ENABLED"
            and re.search(r"\bif\s*\(\s*$", source_text(paths[r[5]], r[0])[:r[1] - 1]))
        after = [c for c in regions if c[7] == CODE and c[5] == r[5] and (c[0], c[1]) >= (r[2], r[3])]
        if guarded and after:
            block = min(after, key=lambda c: (c[0], c[1]))
            if block[0] <= r[2] + 1:
                entry["excluded"].update(range(block[0], block[2] + 1))


def collect_mcdc(fn, files):
    paths = fn["filenames"]
    parents = {r[6]: r for r in fn["regions"] if r[7] == EXPANSION}
    for record in fn.get("mcdc_records", []):
        site, file_id, macros = None, record[6], []
        while file_id in parents:
            site = parents[file_id]
            file_id = site[5]
            macros.append(macro_at(paths[file_id], site[0], site[1]))
        entry = files.get(paths[file_id])
        if entry is None or ASSERTIONS.intersection(macros):
            continue
        key = (tuple(site[:4]) if site else None, tuple(record[:4]))
        seen = entry["mcdc"].get(key)
        if seen:
            seen["shown"] = [a or b for a, b in zip(seen["shown"], record[9])]
            seen["vectors"] += record[10]
            continue
        conditions = sorted(b for b in fn["branches"]
                            if b[8] == MCDC_BRANCH and b[6] == record[6]
                            and (record[0], record[1]) <= (b[0], b[1])
                            and (b[2], b[3]) <= (record[2], record[3]))
        entry["mcdc"][key] = {"decision": record[:4], "site": site[:4] if site else None,
                              "conditions": conditions, "shown": list(record[9]),
                              "vectors": list(record[10])}


def unreachable(path, touched, code):
    lines = set()
    for no in touched:
        for found in UNREACHABLE.finditer(source_text(path, no)):
            point = (no, found.start() + 1)
            inside = [c for c in code if (c[0], c[1]) <= point <= (c[2], c[3])]
            if not inside:
                continue
            size = min((c[2] - c[0], c[3] - c[1]) for c in inside)
            innermost = [c for c in inside if (c[2] - c[0], c[3] - c[1]) == size]
            if all(c[4] == 0 for c in innermost):
                region = innermost[0]
                before = any((region[0], region[1]) < (c[0], c[1]) < point for c in code)
                lines.update(range(no if before else region[0], region[2] + 1))
    return lines


def enclosing(functions, no):
    inside = [f for f in functions if f[0] <= no <= f[1]]
    return min(inside, key=lambda f: f[1] - f[0])[2] if inside else ""


def snippet(path, line_start, col_start, line_end, col_end):
    text = source_line(path, line_start)
    if line_start == line_end:
        return text[col_start - 1:col_end - 1]
    return text[col_start - 1:]


def normalize(path):
    p = Path(path)
    if p.is_absolute():
        try:
            return str(p.resolve().relative_to(ROOT))
        except ValueError:
            return str(p)
    return str(Path(*[part for part in p.parts if part != "."]))


@functools.cache
def source_lines(path):
    try:
        with open(ROOT / path, errors="replace") as fh:
            return fh.read().splitlines()
    except OSError:
        return []


def source_text(path, no):
    lines = source_lines(path)
    return lines[no - 1] if 0 < no <= len(lines) else ""


def source_line(path, no):
    return source_text(path, no)[:200]


def ranges(numbers):
    out = []
    for n in sorted(numbers):
        if out and n == out[-1][1] + 1:
            out[-1][1] = n
        else:
            out.append([n, n])
    return ",".join(f"{a}-{b}" if a != b else f"{a}" for a, b in out)


def cover(path, touched, data, spans, summary, details):
    skipped = spans["excluded"] | unreachable(path, touched, spans["code"])
    touched = touched - skipped
    missed = []
    for no in sorted(touched & set(data["lines"])):
        text = source_text(path, no)
        if BRACE.match(text) or USING.match(text):
            continue
        summary["lines"][1] += 1
        if data["lines"][no] > 0:
            summary["lines"][0] += 1
        else:
            missed.append(no)
            details.append({"kind": "line", "file": path, "line": no,
                            "function": enclosing(spans["functions"], no),
                            "source": source_line(path, no)})
    for no in sorted(touched & set(data["branches"])):
        text = source_text(path, no)
        for idx, taken in enumerate(data["branches"][no]):
            if SWITCH.match(text) or (idx % 2 and (CASE.match(text) or FOREVER.match(text))):
                continue
            summary["branches"][1] += 1
            if taken > 0:
                summary["branches"][0] += 1
            else:
                details.append({"kind": "branch", "file": path, "line": no,
                                "direction": "true" if idx % 2 == 0 else "false",
                                "function": enclosing(spans["functions"], no),
                                "source": source_line(path, no)})
    for name, (start, count) in data["functions"].items():
        if start in touched:
            summary["functions"][1] += 1
            if count > 0:
                summary["functions"][0] += 1
            else:
                details.append({"kind": "function", "file": path, "line": start,
                                "function": name, "source": source_line(path, start)})
    for m in spans["mcdc"].values():
        decision = m["site"] or m["decision"]
        if decision[0] in skipped or not touched & set(range(decision[0], decision[2] + 1)):
            continue
        if not any(vector.get("executed") for vector in m["vectors"]):
            continue
        for idx, shown in enumerate(m["shown"]):
            summary["mcdc"][1] += 1
            if shown:
                summary["mcdc"][0] += 1
                continue
            where = m["site"] or (m["conditions"][idx] if idx < len(m["conditions"]) else decision)
            details.append({
                "kind": "mcdc", "file": path, "line": where[0], "column": where[1],
                "condition": idx + 1,
                "condition_source": snippet(path, *where[:4]),
                "decision_source": snippet(path, *decision),
                "function": enclosing(spans["functions"], decision[0]),
                "test_vectors": [v for v in m["vectors"] if v.get("executed")],
            })
    return missed


def main():
    ap = argparse.ArgumentParser(description="Coverage of the lines a change touches.")
    profile = ap.add_mutually_exclusive_group(required=True)
    profile.add_argument("--profdata", help="a merged profile")
    profile.add_argument("--profiles", nargs="+",
                         help=".profraw/.profdata files or directories of them, to merge")
    binaries = ap.add_mutually_exclusive_group(required=True)
    binaries.add_argument("--objects", nargs="+", help="the instrumented binaries")
    binaries.add_argument("--build-dir", help="a coverage build (SDB_COVERAGE=On) or its bundle")
    ap.add_argument("--base", default="origin/main")
    ap.add_argument("--label", default="coverage")
    ap.add_argument("--html", action="store_true",
                    help="also render the changed files as HTML into <out>/html")
    ap.add_argument("--out", required=True, help="output directory")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    args.profdata = args.profdata or merge(args.profiles, out / "merged.profdata")
    args.objects = args.objects or objects(args.build_dir)
    changed = changed_lines(args.base)
    sources = [p for p in changed if (ROOT / p).is_file()]
    reported = [p for p in sources if not is_test(p)]
    if args.html and reported:
        html(args.profdata, args.objects, reported, out / "html")
    summary = {"lines": [0, 0], "functions": [0, 0], "branches": [0, 0], "mcdc": [0, 0]}
    details = []
    uncovered_ranges = {}
    if reported:
        regions = {normalize(k): v for k, v in parse_json(export("text", args.profdata,
                                                                  args.objects, reported)).items()}
        report = {normalize(k): v for k, v in parse_lcov(export("lcov", args.profdata, args.objects,
                                                                  reported)).items()}
        for path in sorted(reported):
            data = report.get(path)
            if data:
                spans = regions.get(path, {"mcdc": {}, "functions": [], "excluded": set(), "code": []})
                missed = cover(path, changed[path], data, spans, summary, details)
                if missed:
                    uncovered_ranges[path] = ranges(missed)
    names = readable({d["function"] for d in details if d["kind"] == "function"})
    for d in details:
        if d["kind"] == "function":
            d["function"] = names[d["function"]]
    text = (f"{args.label}: lines {summary['lines'][0]}/{summary['lines'][1]}, "
            f"functions {summary['functions'][0]}/{summary['functions'][1]}, "
            f"branches {summary['branches'][0]}/{summary['branches'][1]}, "
            f"MC/DC {summary['mcdc'][0]}/{summary['mcdc'][1]}")
    if uncovered_ranges:
        text += "\nuncovered: " + "  ".join(f"{p}:{r}" for p, r in uncovered_ranges.items())
    (out / "summary.txt").write_text(text + "\n")
    (out / "coverage.json").write_text(json.dumps({"summary": summary, "uncovered": details},
                                                  indent=1))
    print(text)


if __name__ == "__main__":
    main()
