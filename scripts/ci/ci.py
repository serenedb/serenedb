#!/usr/bin/env python3
import argparse
import fnmatch
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "scripts" / "ci"), str(ROOT / "tests" / "drivers" / "harness")]

from testlib import selection, store  # noqa: E402
from testlib.scheduler import BAD  # noqa: E402

DIGEST_VERSION = "2"
NOT_BUILD_INPUTS = (
    ".github/*", ".claude/*", "packages/*", "serene-ui/*", "apps/*",
    "tests/drivers/*", "tests/duckdb/*", "scripts/ci/testlib/*", "scripts/ci/skips/*",
    "scripts/ci/test.py", "scripts/ci/ci.py", "scripts/ci/steps/*", "scripts/gtest-parallel/*",
    "scripts/ci/run_shard.sh", "scripts/ci/coverage_report.sh", "scripts/ci/bugfix_check.sh",
    "scripts/ci/selection_map.sh", "scripts/ci/report.py", "scripts/ci/select_suites.sh",
    "scripts/ci/classify-changes.sh", "scripts/ci/check_gitlinks.sh", "scripts/ci/config.sh",
    "scripts/ci/build-image.txt", "scripts/coverage/*", "scripts/land.sh",
    "scripts/check_submodule_pointers.py",
    "*.md", ".pre-commit-config.yaml", ".clang-format", ".clang-tidy", ".actrc", ".lldbinit",
    "lldb_addons.py", "LICENSE",
)
BUILD_INPUT_EXCEPTIONS = ("docs/*", "tests/sqllogic/sdb/pg/site_docs/*", "tests/sqllogic/recovery/site_docs/*")
SQLLOGIC_TEST_DIR = "tests/sqllogic/"
BUNDLE_PATHS = (
    "bin/serened", "bin/serenedb-tests", "bin/iresearch-tests", "bin/iresearch-load-tests",
    "third_party/duckdb/test/unittest", "third_party/re2/CTestTestfile.cmake", "CMakeCache.txt",
)
BUNDLE_CACHE = Path(os.environ.get("SDB_BUNDLE_CACHE", "/mnt/data/sdb-ci/bundles"))


def _is_build_input(path):
    if any(fnmatch.fnmatchcase(path, p) for p in BUILD_INPUT_EXCEPTIONS):
        return True
    if path.startswith(SQLLOGIC_TEST_DIR):
        return False
    return not any(fnmatch.fnmatchcase(path, p) for p in NOT_BUILD_INPUTS)


def digest(config, flags, image, rev="HEAD"):
    out = subprocess.run(["git", "-C", str(ROOT), "ls-tree", "-r", rev], capture_output=True,
                         text=True, check=True).stdout
    h = hashlib.sha256()
    h.update(f"{DIGEST_VERSION}\n{config}\n{flags}\n{image}\n".encode())
    for line in out.splitlines():
        meta, _, path = line.partition("\t")
        if _is_build_input(path):
            h.update(f"{meta}\t{path}\n".encode())
    return h.hexdigest()[:24]


def bundle_name(config, arch, dig):
    return f"bundles/{config}-{arch}-{dig}.tar.zst"


def image_id(image):
    inspect = ["docker", "image", "inspect", "--format", "{{.Id}}", image]
    res = subprocess.run(inspect, capture_output=True, text=True)
    if res.returncode != 0:
        subprocess.run(["docker", "pull", "-q", image], capture_output=True, check=True)
        res = subprocess.run(inspect, capture_output=True, text=True, check=True)
    return res.stdout.strip()


def cmd_digest(args):
    print(digest(args.config, args.flags, image_id(args.image) if args.image else "", args.rev))


def cmd_pack(args):
    build = Path(args.build_dir).resolve()
    re2 = build / "third_party" / "re2"
    paths = [p for p in BUNDLE_PATHS if (build / p).exists()]
    if re2.is_dir():
        paths += [str((re2 / n).relative_to(build)) for n in sorted(os.listdir(re2))
                  if n.endswith("_test") and (re2 / n).is_file()]
    runner = []
    if args.runner:
        source = Path(args.runner).resolve()
        runner = ["-C", str(source.parent), f"--transform=s,^{source.name}$,bin/sqllogictest,",
                  source.name]
    out = Path(args.out).resolve()
    subprocess.run(["tar", "-C", str(build), "-I", "zstd -T0 -3", "-cf", str(out), *paths,
                    *runner], check=True)
    print(f"{out} {out.stat().st_size >> 20} MB")


def fetch_bundle(config, arch, dig, dest):
    dest = Path(dest)
    marker = dest / ".complete"
    if marker.is_file():
        return dest
    BUNDLE_CACHE.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(dir=BUNDLE_CACHE, prefix=".fetch-"))
    try:
        archive = tmp_dir / "bundle.tar.zst"
        if not store.get(bundle_name(config, arch, dig), archive):
            raise SystemExit(f"no bundle {bundle_name(config, arch, dig)} in the store")
        unpacked = tmp_dir / "unpacked"
        unpacked.mkdir()
        owner = os.environ.get("SDB_DOCKER_OWNER")
        subprocess.run(["docker", "run", "--rm", *(["--label", f"serenedb.owner={owner}"] if owner else []),
                        "--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{BUNDLE_CACHE}:{BUNDLE_CACHE}",
                        os.environ["BUILD_IMAGE"], "tar", "-C", str(unpacked), "-I", "zstd -T0",
                        "-xf", str(archive)], check=True)
        (unpacked / ".complete").write_text(dig)
        if dest.exists():
            shutil.rmtree(dest)
        unpacked.rename(dest)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    return dest


def prune_bundles(keep_days=2):
    if not BUNDLE_CACHE.is_dir():
        return
    cutoff = time.time() - keep_days * 86400
    for d in BUNDLE_CACHE.iterdir():
        try:
            if d.stat().st_mtime < cutoff:
                shutil.rmtree(d, ignore_errors=True)
        except OSError:
            pass


def cmd_fetch(args):
    prune_bundles()
    dest = BUNDLE_CACHE / f"{args.config}-{args.arch}-{args.digest}"
    path = fetch_bundle(args.config, args.arch, args.digest, dest)
    try:
        os.utime(path)
    except PermissionError:
        pass
    print(path)


def cmd_upload(args):
    store.put(args.dest, args.src)


def cmd_download(args):
    dest = Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)
    for name in store.files(args.dir):
        if name.startswith(args.prefix):
            store.get(f"{args.dir.rstrip('/')}/{name}", dest / name)
            print(name)


def cmd_exists(args):
    try:
        found = store.exists(args.path)
    except (OSError, RuntimeError) as e:
        print(f"cannot ask the store about {args.path}: {e}", file=sys.stderr)
        sys.exit(2)
    sys.exit(0 if found else 1)


def cmd_shards(args):
    extra = args.test_args[1:] if args.test_args[:1] == ["--"] else args.test_args
    res = subprocess.run([sys.executable, str(ROOT / "scripts" / "ci" / "test.py"), "--build-dir",
                          args.build_dir, "--config", args.config, "--timing", args.timing,
                          "--list", *extra], capture_output=True, text=True)
    if res.returncode != 0:
        sys.stderr.write(res.stderr)
        sys.exit(res.returncode)
    total = 0.0
    biggest = 0.0
    for line in res.stdout.splitlines():
        parts = line.split()
        if parts and parts[0].endswith("s"):
            cost = float(parts[0][:-1])
            total += cost * int(parts[2])
            biggest = max(biggest, cost)
    cpus = args.cpus
    wall = total / cpus
    count = max(1, min(args.max_shards, round(wall / args.target_seconds + 0.5)))
    print(json.dumps([{"spec": f"{i}/{count}", "name": f"{i}of{count}"} for i in range(1, count + 1)]))
    print(f"{total:.0f}s of work, biggest unit {biggest:.0f}s, {count} shard(s)", file=sys.stderr)


def cmd_timing(args):
    key = f"timing/{args.config}-{args.arch}.parquet"
    tmp = Path(tempfile.mkdtemp())
    try:
        parquet = tmp / "timing.parquet"
        if store.reachable() and store.get(key, parquet):
            store.timing_snapshot(parquet, args.out)
        else:
            Path(args.out).write_text(json.dumps({"units": {}}))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def cmd_update_timing(args):
    tmp = Path(tempfile.mkdtemp())
    try:
        wanted = []
        for key in store.recent("results", args.days):
            name = key.rsplit("/", 1)[1]
            if f"-{args.config}-{args.arch}-" in name and (
                    f"-pr{args.pr}-" in name if args.pr else "-pr" not in name):
                wanted.append(key)
        files = store.fetch_all(wanted, tmp)
        if not files:
            print("no results to learn timings from")
            return
        old = tmp / "old.parquet"
        key = f"timing/{args.config}-{args.arch}.parquet"
        have_old = store.get(key, old)
        attempts = (f"SELECT unit, coalesce(run, concat(sha, ':', started)) AS r, "
                    f"CASE WHEN status = 'stuck' THEN 2 * duration ELSE duration END AS d "
                    f"FROM read_parquet([{store.quoted(files)}], union_by_name = true) "
                    f"WHERE coalesce(pr, '') = '{args.pr}' AND NOT coalesce(attribute, false) "
                    f"AND NOT wrapper AND duration > 0 "
                    f"AND status IN ('passed', 'failed', 'flaky', 'stuck')")
        runs = (f"SELECT regexp_replace(unit, '#[0-9]+/[0-9]+$', '') AS unit, sum(d) AS d "
                f"FROM (SELECT unit, r, max(d) AS d FROM ({attempts}) GROUP BY ALL) GROUP BY 1, r")
        fresh = (f"SELECT unit, quantile_cont(d, 0.95) AS p95, count(*) AS n FROM ({runs}) "
                 f"GROUP BY unit")
        keep = (f" UNION ALL SELECT unit, p95, n FROM read_parquet('{old}') "
                f"WHERE unit NOT IN (SELECT unit FROM fresh)") if have_old else ""
        new = tmp / "new.parquet"
        store.shell(f"COPY (WITH fresh AS ({fresh}) SELECT unit, p95, n FROM fresh{keep}) "
                    f"TO '{new}' (FORMAT parquet, COMPRESSION zstd)", tmp)
        store.put(key, new)
        print(f"{key}: learned from {len(files)} result file(s)")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def cmd_nearest(args):
    path = Path(args.coverage)
    data = json.loads(path.read_text())
    files = sorted({d["file"] for d in data["uncovered"]})
    if not files:
        return
    tmp = Path(tempfile.mkdtemp())
    try:
        local = tmp / "map.parquet"
        if not store.get(f"selection/dev-{args.arch}.parquet", local):
            print("no selection map in the store yet")
            return
        rows = store.shell(f"SELECT file, start, \"end\", unit FROM read_parquet('{local}') "
                           f"WHERE file IN ({store.quoted(files)}) AND rank <= {selection.CHEAPEST} "
                           f"ORDER BY rank, unit", tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    spans = defaultdict(dict)
    for r in rows:
        units = spans[r["file"]].setdefault((r["start"], r["end"]), [])
        if r["unit"] not in units and len(units) < selection.CHEAPEST:
            units.append(r["unit"])
    for d in data["uncovered"]:
        found = spans.get(d["file"])
        if not found:
            continue
        line = d["line"]
        inside = [s for s in found if s[0] <= line <= s[1]]
        span = (max(inside, key=lambda s: (s[0], -s[1])) if inside
                else min(found, key=lambda s: min(abs(line - s[0]), abs(line - s[1]))))
        d["tests"] = found[span]
        d["tests_run"] = f"{d['file']}:{span[0]}-{span[1]}"
    path.write_text(json.dumps(data, indent=1))


def cmd_selection_map(args):
    work = Path(args.work).resolve()
    work.mkdir(parents=True, exist_ok=True)
    stats = selection.build_map(args.run, args.config, args.arch, args.bundle, work)
    print(f"selection/{args.config}-{args.arch}.parquet: {stats['functions']} functions, "
          f"{stats['units']} units, {stats['rows']} rows")


def _rows(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def _is_failure(row):
    return row["status"] in BAD and row["attempt"] == 0


def _failures(rows):
    return [r for r in rows if _is_failure(r)]


def _unit_key(unit):
    return re.sub(r"#\d+/\d+$", "", unit)


def cmd_elsewhere(args):
    src = Path(args.results).resolve()
    rows = _rows(src)
    meta = json.loads((src.parent / "meta.json").read_text())
    units = sorted({_unit_key(r["unit"]) for r in _failures(rows)})
    if not units:
        return
    tmp = Path(tempfile.mkdtemp())
    try:
        own = f"{meta['run']}-"
        files = store.fetch_all([k for k in store.recent("failures", args.days)
                                 if not k.rsplit("/", 1)[1].startswith(own)], tmp)
        if not files:
            return
        found = store.shell(
            f"SELECT unit, pr, count(DISTINCT run) AS runs, "
            f"string_agg(DISTINCT config, ',' ORDER BY config) AS configs, "
            f"strftime(to_timestamp(max(started)), '%Y-%m-%d') AS last FROM ("
            f"SELECT regexp_replace(unit, '#[0-9]+/[0-9]+$', '') AS unit, coalesce(pr, '') AS pr, "
            f"coalesce(branch, '') AS branch, run, config, started "
            f"FROM read_parquet([{store.quoted(files)}], union_by_name = true)) "
            f"WHERE unit IN ({store.quoted(units)}) AND (pr NOT IN ('', {store.quoted([meta['pr']])}) "
            f"OR (pr = '' AND branch = 'main')) GROUP BY unit, pr", tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    found.sort(key=lambda f: f["last"], reverse=True)
    found.sort(key=lambda f: f["pr"] != "")
    seen = defaultdict(list)
    for f in found:
        where = f"#{f['pr']}" if f["pr"] else "main"
        seen[f["unit"]].append(f"{where} ({f['configs']}, {f['runs']} run(s), last {f['last']})")
    if not seen:
        return
    with open(src, "w") as fh:
        for r in rows:
            places = seen.get(_unit_key(r["unit"]))
            if places and _is_failure(r):
                r["elsewhere"] = "; ".join(places[:4])
            fh.write(json.dumps(r) + "\n")
    print(f"{len(seen)} of {len(units)} failed unit(s) failed elsewhere too")


def cmd_results(args):
    src = Path(args.results).resolve()
    meta = json.loads((src.parent / "meta.json").read_text())
    parquet = src.with_suffix(".parquet")
    store.jsonl_to_parquet(src, parquet)
    day = time.strftime("%Y-%m-%d", time.gmtime(meta["started"]))
    shard = f"{meta['config']}-{meta['arch']}-{meta['shard'].replace('/', 'of')}"
    pr = f"pr{meta['pr']}-" if meta.get("pr") else ""
    name = f"results/{day}/{args.run}-{pr}{shard}.parquet"
    store.put(name, parquet)
    print(name)
    failures = _failures(_rows(src))
    if failures:
        bad = src.with_name("failures.jsonl")
        bad.write_text("".join(json.dumps(r) + "\n" for r in failures))
        store.jsonl_to_parquet(bad, bad.with_suffix(".parquet"))
        store.put(f"failures/{day}/{args.run}-{pr}{shard}.parquet", bad.with_suffix(".parquet"))
        print(f"failures/{day}/{args.run}-{pr}{shard}.parquet")
    attribution = src.parent / "attribution.parquet"
    if attribution.is_file():
        store.put(f"attribution/{args.run}/{shard}.parquet", attribution)
        print(f"attribution/{args.run}/{shard}.parquet")


def main(argv):
    ap = argparse.ArgumentParser(prog="scripts/ci/ci.py")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("digest", help="hash of everything a config's build reads")
    p.add_argument("--config", required=True)
    p.add_argument("--flags", default="")
    p.add_argument("--image", default="")
    p.add_argument("--rev", default="HEAD")
    p.set_defaults(fn=cmd_digest)
    p = sub.add_parser("pack", help="pack what tests need from a build dir")
    p.add_argument("--build-dir", required=True)
    p.add_argument("--runner", default="")
    p.add_argument("--out", required=True)
    p.set_defaults(fn=cmd_pack)
    p = sub.add_parser("fetch", help="fetch and unpack a bundle into the local cache")
    p.add_argument("--config", required=True)
    p.add_argument("--arch", required=True)
    p.add_argument("--digest", required=True)
    p.set_defaults(fn=cmd_fetch)
    p = sub.add_parser("upload", help="put a file into the store")
    p.add_argument("src")
    p.add_argument("dest")
    p.set_defaults(fn=cmd_upload)
    p = sub.add_parser("download", help="get the files of a store directory whose names start "
                                        "with a prefix")
    p.add_argument("dir")
    p.add_argument("prefix")
    p.add_argument("dest")
    p.set_defaults(fn=cmd_download)
    p = sub.add_parser("exists", help="exit 0 when the store has the path")
    p.add_argument("path")
    p.set_defaults(fn=cmd_exists)
    p = sub.add_parser("shards", help="print the shard list for a config as JSON")
    p.add_argument("--build-dir", required=True)
    p.add_argument("--config", required=True)
    p.add_argument("--timing", required=True)
    p.add_argument("--cpus", type=int, default=32)
    p.add_argument("--target-seconds", type=float, default=900.0)
    p.add_argument("--max-shards", type=int, default=12)
    p.add_argument("test_args", nargs=argparse.REMAINDER,
                   help="test.py selection arguments after --")
    p.set_defaults(fn=cmd_shards)
    p = sub.add_parser("timing", help="write the store's timing snapshot for a config as JSON")
    p.add_argument("--config", required=True)
    p.add_argument("--arch", required=True)
    p.add_argument("--out", required=True)
    p.set_defaults(fn=cmd_timing)
    p = sub.add_parser("update-timing", help="learn a config's timings from main's recent results")
    p.add_argument("--config", required=True)
    p.add_argument("--arch", required=True)
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--pr", default="", help="learn from this PR's runs instead (a calibration PR)")
    p.set_defaults(fn=cmd_update_timing)
    p = sub.add_parser("selection-map", help="build the test selection map from a run's attribution")
    p.add_argument("--run", required=True)
    p.add_argument("--config", default="dev")
    p.add_argument("--arch", required=True)
    p.add_argument("--bundle", required=True, help="the unpacked coverage bundle of that run")
    p.add_argument("--work", required=True)
    p.set_defaults(fn=cmd_selection_map)
    p = sub.add_parser("nearest", help="name the units that run the code nearest to each "
                                       "uncovered line of a coverage report")
    p.add_argument("--coverage", required=True)
    p.add_argument("--arch", required=True)
    p.set_defaults(fn=cmd_nearest)
    p = sub.add_parser("results", help="store a shard's results as Parquet")
    p.add_argument("--results", required=True)
    p.add_argument("--run", required=True)
    p.set_defaults(fn=cmd_results)
    p = sub.add_parser("elsewhere", help="mark a shard's failures that failed on main or in "
                                         "other PRs too")
    p.add_argument("--results", required=True)
    p.add_argument("--days", type=int, default=7)
    p.set_defaults(fn=cmd_elsewhere)
    args = ap.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main(sys.argv[1:])
