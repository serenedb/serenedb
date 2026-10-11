#!/usr/bin/env python3

import argparse
import json
import pathlib
import re
import subprocess
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent

DEFAULT_PATHS = [
    "iresearch/index/merge/",
    "iresearch/index/compaction/",
    "iresearch/index/merge_writer.cpp",
    "iresearch/formats/column/merge.cpp",
    "server/search/compaction/",
    "server/connector/segment_rewrite/",
    "server/connector/parallel/",
    "server/connector/functions/sdb_compact.cpp",
    "server/connector/functions/sdb_segments.cpp",
    "server/pg/sdb_catalog/sdb_merges.cpp",
]


def relative(path):
    try:
        return str(pathlib.Path(path).resolve().relative_to(REPO))
    except ValueError:
        return path


def selected(path, prefixes):
    return any(path == p or path.startswith(p) for p in prefixes)


def objects_for(build_dir, prefixes):
    sources = []
    for prefix in prefixes:
        path = REPO / prefix
        if path.is_file():
            sources.append(path)
        elif path.is_dir():
            sources += sorted(path.rglob("*.cpp"))
    objects = []
    for source in sources:
        relative_source = source.relative_to(REPO)
        tail = pathlib.Path(*relative_source.parts[1:])
        objects += sorted(
            str(o)
            for o in build_dir.rglob(f"{tail.name}.o")
            if str(o).endswith(f"/{tail}.o")
        )
    return objects


def llvm_cov(tool, command, profdata, binaries, fmt):
    args = [tool, command, f"-format={fmt}", f"-instr-profile={profdata}"]
    args.append(binaries[0])
    for binary in binaries[1:]:
        args += ["-object", binary]
    done = subprocess.run(args, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"{' '.join(args)} failed:\n{done.stderr}")
    return done.stdout


def read_allowlist(path):
    allowed = set()
    if not path:
        return allowed
    for raw in pathlib.Path(path).read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        location = line.split()[0]
        file, _, numbers = location.rpartition(":")
        first, _, last = numbers.partition("-")
        for number in range(int(first), int(last or first) + 1):
            allowed.add((file, number))
    return allowed


def uncovered_lines_from_lcov(text, prefixes):
    lines = {}
    current = None
    for raw in text.splitlines():
        if raw.startswith("SF:"):
            path = relative(raw[3:])
            current = path if selected(path, prefixes) else None
        elif current and raw.startswith("DA:"):
            number, count = raw[3:].split(",")[:2]
            if int(count) == 0:
                lines.setdefault(current, set()).add(int(number))
    return lines


def source_lines(path):
    try:
        return (REPO / path).read_text().splitlines()
    except OSError:
        return []


NOISE_MACROS = re.compile(r"\bSDB_(ASSERT|TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\(")


def region_text(text, region):
    line, start, end_line, end = region[:4]
    if line != end_line or not 0 < line <= len(text):
        return ""
    return text[line - 1][start - 1 : end - 1]


def branches_of(entry, text):
    merged = {}

    def add(key, line, taken, not_taken):
        old = merged.get(key, (line, 0, 0))
        merged[key] = (line, max(old[1], taken), max(old[2], not_taken))

    for b in entry["branches"]:
        if region_text(text, b) in ("true", "false"):
            continue
        add(("file", *b[:4]), b[0], b[4], b[5])
    for expansion in entry["expansions"]:
        site = expansion["source_region"][0]
        if 0 < site <= len(text) and NOISE_MACROS.search(text[site - 1]):
            continue
        for b in expansion["branches"]:
            add(("macro", *expansion["source_region"][:2], *b[:4]), site, b[4], b[5])
    return list(merged.values())


def mcdc_of(entry):
    merged = {}
    for record in entry.get("mcdc_records", []):
        key = tuple(record[:4])
        flags = record[-1]
        old = merged.get(key, [False] * len(flags))
        merged[key] = [a or b for a, b in zip(old, flags)]
    return [(key[0], flags) for key, flags in merged.items()]


def main():
    parser = argparse.ArgumentParser(
        description="Coverage gate for the merge engine: fails unless every "
        "line, branch and MC/DC condition of the selected files is covered. "
        "Lines that cannot be covered go on an allowlist, one "
        "'path:line reason' per line."
    )
    parser.add_argument("--llvm-cov", default="llvm-cov")
    parser.add_argument("--profdata", required=True)
    parser.add_argument("--binary", action="append", default=[])
    parser.add_argument(
        "--build-dir",
        help="read the coverage mapping from the selected sources' object "
        "files in this build tree instead of the linked binaries",
    )
    parser.add_argument("--path", action="append")
    parser.add_argument("--allowlist")
    parser.add_argument("--report-only", action="store_true")
    args = parser.parse_args()
    prefixes = args.path or DEFAULT_PATHS
    allowed = read_allowlist(args.allowlist)
    inputs = (
        objects_for(pathlib.Path(args.build_dir), prefixes)
        if args.build_dir
        else args.binary
    )
    if not inputs:
        sys.exit("no binaries or object files to read coverage from")

    report = json.loads(
        llvm_cov(args.llvm_cov, "export", args.profdata, inputs, "text")
    )
    rows = []
    for data in report["data"]:
        for entry in data["files"]:
            path = relative(entry["filename"])
            if not selected(path, prefixes):
                continue
            text = source_lines(path)
            branches = branches_of(entry, text)
            decisions = mcdc_of(entry)
            branch_summary = {
                "count": 2 * len(branches),
                "covered": sum((t > 0) + (f > 0) for _, t, f in branches),
            }
            mcdc_summary = {
                "count": sum(len(flags) for _, flags in decisions),
                "covered": sum(sum(flags) for _, flags in decisions),
            }
            rows.append(
                (
                    path,
                    entry["summary"]["lines"],
                    branch_summary,
                    mcdc_summary,
                    sorted({n for n, t, f in branches if t == 0 or f == 0}),
                    sorted(
                        (n, [i + 1 for i, ok in enumerate(flags) if not ok])
                        for n, flags in decisions
                        if not all(flags)
                    ),
                )
            )
    rows.sort()

    lines = uncovered_lines_from_lcov(
        llvm_cov(args.llvm_cov, "export", args.profdata, inputs, "lcov"),
        prefixes,
    )

    def ratio(part):
        if part["count"] == 0:
            return "-"
        return f"{100.0 * part['covered'] / part['count']:.1f}% ({part['covered']}/{part['count']})"

    width = max([len(r[0]) for r in rows] + [4])
    print(f"{'file':<{width}}  {'lines':>20}  {'branches':>20}  {'mc/dc':>20}")
    failed = False
    for path, line_summary, branch_summary, mcdc_summary, branch_gaps, mcdc_gaps in rows:
        print(
            f"{path:<{width}}  {ratio(line_summary):>20}  "
            f"{ratio(branch_summary):>20}  {ratio(mcdc_summary):>20}"
        )
        missing_lines = sorted(
            n for n in lines.get(path, ()) if (path, n) not in allowed
        )
        missing_branches = [n for n in branch_gaps if (path, n) not in allowed]
        missing_mcdc = [(n, c) for n, c in mcdc_gaps if (path, n) not in allowed]
        if missing_lines or missing_branches or missing_mcdc:
            failed = True
        if missing_lines:
            print(f"  uncovered lines: {', '.join(map(str, missing_lines))}")
        if missing_branches:
            print(f"  uncovered branches at lines: {', '.join(map(str, missing_branches))}")
        if missing_mcdc:
            listed = ", ".join(
                f"{n} (C{'/C'.join(map(str, c))})" for n, c in missing_mcdc
            )
            print(f"  uncovered MC/DC conditions at lines: {listed}")
    if not rows:
        print("no selected files in the coverage data")
        failed = True
    if failed and not args.report_only:
        print("coverage gate FAILED")
        return 1
    print("coverage gate " + ("passed" if not failed else "report only"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
