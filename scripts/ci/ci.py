#!/usr/bin/env python3
import argparse
import fcntl
import fnmatch
import hashlib
import json
import os
import platform
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

import services  # noqa: E402
from testlib import store  # noqa: E402
from testlib.config import CONFIGS  # noqa: E402
from testlib.scheduler import BAD  # noqa: E402

DIGEST_VERSION = "2"
NOT_BUILD_INPUTS = (
    ".github/*", ".claude/*", "packages/*", "serene-ui/*", "apps/*",
    "tests/drivers/*", "tests/duckdb/*", "scripts/ci/testlib/*", "scripts/ci/skips/*",
    "scripts/ci/test.py", "scripts/ci/ci.py", "scripts/ci/steps/*", "scripts/gtest-parallel/*",
    "scripts/ci/run_shard.sh", "scripts/ci/bugfix_check.sh", "scripts/ci/report.py",
    "scripts/ci/classify-changes.sh", "scripts/ci/config.sh",
    "scripts/coverage/*", "scripts/land.sh",
    "scripts/check_submodule_pointers.py",
    "*.md", ".pre-commit-config.yaml", ".clang-format", ".clang-tidy", ".lldbinit",
    "lldb_addons.py", "LICENSE",
)
BUILD_INPUT_EXCEPTIONS = ("docs/*", "tests/sqllogic/sdb/pg/site_docs/*", "tests/sqllogic/recovery/site_docs/*")
SQLLOGIC_TEST_DIR = "tests/sqllogic/"
BUNDLE_PATHS = (
    "bin/serened", "bin/serenedb-tests", "bin/iresearch-tests", "bin/iresearch-load-tests",
    "bin/re2-tests", "third_party/duckdb/test/unittest", "CMakeCache.txt",
)
BUNDLE_CACHE = Path(os.environ.get("SDB_BUNDLE_CACHE", "/mnt/data/sdb-ci/bundles"))
BUNDLE_MIN_FREE = 200 << 30
ARCHES = ("x86_64", "aarch64")
DOCKER_ARCHES = {"x86_64": "amd64", "aarch64": "arm64"}
IMAGE_REPO = "serenedb/serenedb-build-ubuntu"
FIXTURE_IMAGE_REPO = "serenedb/serenedb-test-"
IMAGE_DOCKERFILE = "scripts/ci/build-ubuntu.Dockerfile"
IMAGE_CONTEXTS = {"": "scripts/ci", "drivers": "tests/drivers"}


def image_inputs():
    paths = [IMAGE_DOCKERFILE]
    for line in (ROOT / IMAGE_DOCKERFILE).read_text().splitlines():
        words = line.split()
        if words[:1] != ["COPY"]:
            continue
        context = ""
        sources = []
        for word in words[1:-1]:
            if word.startswith("--from="):
                context = word.removeprefix("--from=")
            elif not word.startswith("--"):
                sources.append(word)
        if context in IMAGE_CONTEXTS:
            paths += [f"{IMAGE_CONTEXTS[context]}/{s}" for s in sources]
    return paths


def image_name():
    files = subprocess.run(["git", "-C", str(ROOT), "ls-files", "-z", "-c", "-o", "--exclude-standard", "--",
                            *image_inputs()], capture_output=True, text=True, check=True).stdout
    h = hashlib.sha256()
    for path in sorted(set(filter(None, files.split("\0")))):
        if (ROOT / path).is_file():
            h.update(f"{path}\t{hashlib.sha256((ROOT / path).read_bytes()).hexdigest()}\n".encode())
    return f"{IMAGE_REPO}:{h.hexdigest()[:12]}"


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


def cmd_image(args):
    print(image_name())


def cached_bundle(config, arch, dig):
    return BUNDLE_CACHE / f"{config}-{arch}-{dig}"


def cmd_locate(args):
    if (cached_bundle(args.config, args.arch, args.digest) / ".complete").is_file():
        print("cache")
    elif store.exists(bundle_name(args.config, args.arch, args.digest)):
        print("store")
    else:
        print("none")


def cmd_stage(args):
    build = Path(args.build_dir).resolve()
    files = [(build / p, p) for p in BUNDLE_PATHS if (build / p).exists()]
    if args.runner:
        files.append((Path(args.runner).resolve(), "bin/sqllogictest"))
    shared_cache()
    prune_bundles()
    dest = cached_bundle(args.config, args.arch, args.digest)
    tmp_dir = Path(tempfile.mkdtemp(dir=BUNDLE_CACHE, prefix=".stage-"))
    try:
        for source, rel in files:
            (tmp_dir / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, tmp_dir / rel)
        (tmp_dir / ".complete").write_text(args.digest)
        tmp_dir.chmod(0o755)
        if dest.exists():
            shutil.rmtree(dest)
        tmp_dir.rename(dest)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    print(dest)


def cmd_publish(args):
    name = bundle_name(args.config, args.arch, args.digest)
    if store.exists(name):
        print(f"{name} is in the store")
        return
    src = fetched(args.config, args.arch, args.digest)
    tmp_dir = Path(tempfile.mkdtemp(dir=BUNDLE_CACHE, prefix=".publish-"))
    try:
        archive = tmp_dir / "bundle.tar.zst"
        owner = os.environ.get("SDB_DOCKER_OWNER")
        subprocess.run(["docker", "run", "--rm", *(["--label", f"serenedb.owner={owner}"] if owner else []),
                        "--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{BUNDLE_CACHE}:{BUNDLE_CACHE}",
                        image_name(), "tar", "-C", str(src), "--exclude=./.complete",
                        "-I", "zstd -T0 -3", "-cf", str(archive), "."], check=True)
        start = time.monotonic()
        store.put(name, archive)
        print(f"stored {name} ({archive.stat().st_size >> 20} MB) in {time.monotonic() - start:.0f}s")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def shared_cache():
    BUNDLE_CACHE.mkdir(parents=True, exist_ok=True)
    info = BUNDLE_CACHE.stat()
    if info.st_uid == os.getuid() and info.st_mode & 0o1777 != 0o1777:
        BUNDLE_CACHE.chmod(0o1777)


def fetch_bundle(config, arch, dig, dest):
    dest = Path(dest)
    marker = dest / ".complete"
    if marker.is_file():
        return dest
    shared_cache()
    tmp_dir = Path(tempfile.mkdtemp(dir=BUNDLE_CACHE, prefix=".fetch-"))
    try:
        archive = tmp_dir / "bundle.tar.zst"
        name = bundle_name(config, arch, dig)
        print(f"downloading {name} from the store", file=sys.stderr, flush=True)
        start = time.monotonic()
        if not store.get(name, archive):
            raise SystemExit(f"no bundle {name} in the store")
        print(f"downloaded {archive.stat().st_size >> 20} MB in {time.monotonic() - start:.0f}s",
              file=sys.stderr, flush=True)
        unpacked = tmp_dir / "unpacked"
        unpacked.mkdir()
        owner = os.environ.get("SDB_DOCKER_OWNER")
        start = time.monotonic()
        subprocess.run(["docker", "run", "--rm", *(["--label", f"serenedb.owner={owner}"] if owner else []),
                        "--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{BUNDLE_CACHE}:{BUNDLE_CACHE}",
                        image_name(), "tar", "-C", str(unpacked), "-I", "zstd -T0",
                        "-xf", str(archive)], check=True)
        print(f"unpacked into {dest} in {time.monotonic() - start:.0f}s", file=sys.stderr, flush=True)
        (unpacked / ".complete").write_text(dig)
        unpacked.chmod(0o755)
        if dest.exists():
            shutil.rmtree(dest)
        unpacked.rename(dest)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    return dest


def drop_bundle(path):
    if not (path / ".complete").is_file():
        shutil.rmtree(path, ignore_errors=True)
        return
    with open(path / ".complete") as marker:
        fcntl.flock(marker, fcntl.LOCK_EX | fcntl.LOCK_NB)
        shutil.rmtree(path, ignore_errors=True)


def prune_bundles(keep_days=2, min_free=BUNDLE_MIN_FREE):
    if not BUNDLE_CACHE.is_dir():
        return
    cutoff = time.time() - keep_days * 86400
    recent = []
    for d in BUNDLE_CACHE.iterdir():
        try:
            info = d.stat()
            if info.st_uid != os.getuid():
                continue
            if info.st_mtime < cutoff:
                drop_bundle(d)
            elif not d.name.startswith(".") and (d / ".complete").is_file():
                recent.append((info.st_mtime, d))
        except OSError:
            pass
    for _, d in sorted(recent):
        if shutil.disk_usage(BUNDLE_CACHE).free >= min_free:
            return
        try:
            drop_bundle(d)
        except OSError:
            pass


def checkout_images():
    fixtures = ROOT / "tests" / "sqllogic" / "fixtures"
    return {image_name(), services.KEV_IMAGE,
            *(f"{FIXTURE_IMAGE_REPO}{d.parent.name}:{services.fixture_image_tag(d.parent.name)}"
              for d in fixtures.glob("*/Dockerfile"))}


def stale_images(rows, wanted, arch):
    foreign = tuple(f"-{other}" for other in DOCKER_ARCHES.values() if other != arch)
    newest = {}
    stale = []
    for ref, _ in sorted(rows, key=lambda row: row[1], reverse=True):
        repo, _, tag = ref.rpartition(":")
        if tag == "<none>" or (repo != IMAGE_REPO and not repo.startswith(FIXTURE_IMAGE_REPO)):
            continue
        if tag.endswith(foreign):
            stale.append(ref)
            continue
        base = ref.removesuffix(f"-{arch}")
        if newest.setdefault(repo, base) != base and base not in wanted:
            stale.append(ref)
    return stale


def prune_images():
    listing = subprocess.run(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}\t{{.CreatedAt}}"],
                             capture_output=True, text=True)
    if listing.returncode != 0:
        return
    rows = [line.split("\t") for line in listing.stdout.splitlines()]
    stale = stale_images(rows, checkout_images(), DOCKER_ARCHES[platform.machine()])
    if stale:
        subprocess.run(["docker", "rmi", *stale], capture_output=True)
    subprocess.run(["docker", "image", "prune", "-f"], capture_output=True)


def cmd_prune(args):
    prune_bundles()
    prune_images()


def fetched(config, arch, dig):
    prune_bundles()
    path = fetch_bundle(config, arch, dig, cached_bundle(config, arch, dig))
    try:
        os.utime(path)
    except PermissionError:
        pass
    return path


def cmd_fetch(args):
    print(fetched(args.config, args.arch, args.digest))


def cmd_upload(args):
    store.put(args.dest, args.src)


def cmd_download(args):
    dest = Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)
    keys = store.run_files(args.kind, args.run, args.prefix)
    if not keys:
        sys.exit(f"the store has no {args.kind} files of run {args.run} starting with {args.prefix!r}")
    for key in keys:
        name = key.rsplit("/", 1)[1]
        if not store.get(key, dest / name):
            sys.exit(f"{key} left the store while it was downloaded")
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
    plan = next(line.split()[1] for line in res.stderr.splitlines() if line.startswith("plan "))
    cpus = args.cpus
    wall = total / cpus
    count = max(1, min(args.max_shards, round(wall / args.target_seconds + 0.5)))
    prefix = f"{args.check}-" if args.check else ""
    print(json.dumps([{"spec": f"{i}/{count}", "name": f"{prefix}{i}of{count}", "plan": plan,
                       "check": args.check} for i in range(1, count + 1)]))
    print(f"{total:.0f}s of work, biggest unit {biggest:.0f}s, {count} shard(s)", file=sys.stderr)


def plan_runs(main, *extra):
    runs = [*main, *(r for shards in extra for r in shards)]
    return {"inline": len(main) <= 1, "names": [r["name"] for r in runs],
            "runs": {r["name"]: {k: v for k, v in r.items() if k != "name"} for r in runs}}


def cmd_runs(args):
    planned = plan_runs(*(json.loads(s) for s in args.shards))
    if args.tsv:
        Path(args.tsv).write_text("".join(
            f"{name}\t{r['spec']}\t{r['plan']}\t{r['check']}\n" for name, r in planned["runs"].items()))
    print(f"remote={str(not planned['inline']).lower()}")
    print(f"shards={json.dumps(planned['names'])}")
    print(f"runs={json.dumps(planned['runs'])}")


def timing_key(config, arch):
    return f"timing/{config}-{arch}.parquet"


def _learns_from(name, args):
    pr = re.search(r"-pr(\d+)-", name)
    return pr.group(1) == args.pr if pr else not args.pr


def cmd_timing(args):
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp())
    try:
        sources = []
        if store.reachable():
            own = CONFIGS[args.config].slowdown
            other = next(a for a in ARCHES if a != args.arch)
            for name, config in CONFIGS.items():
                local = tmp / f"{name}.parquet"
                if store.get(timing_key(name, args.arch), local):
                    sources.append((local, own / config.slowdown, name == args.config))
                elif store.get(timing_key(name, other), local):
                    sources.append((local, own / config.slowdown, False))
        store.timing_snapshot(tmp, sources, args.out)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def cmd_update_timing(args):
    tmp = Path(tempfile.mkdtemp())
    try:
        shard = re.compile(rf"-{re.escape(args.config)}-{re.escape(args.arch)}-\d+of\d+\.parquet$")
        wanted = [key for key in store.recent("results", args.days)
                  if shard.search(key) and _learns_from(key.rsplit("/", 1)[1], args)]
        files = store.fetch_all(wanted, tmp)
        if not files:
            print("no results to learn timings from")
            return
        old = tmp / "old.parquet"
        key = timing_key(args.config, args.arch)
        have_old = store.get(key, old)
        attempts = (f"SELECT unit, coalesce(run, concat(sha, ':', started)) AS r, duration AS d, "
                    f"started AS s FROM read_parquet([{store.quoted(files)}], union_by_name = true) "
                    f"WHERE coalesce(pr, '') = '{args.pr}' "
                    f"AND status IN ('passed', 'failed', 'flaky', 'stuck', 'setup') "
                    f"AND NOT wrapper AND duration > 0 AND coalesce(signature, '') <> 'budget'")
        runs = (f"SELECT regexp_replace(unit, '#[0-9]+/[0-9]+$', '') AS unit, sum(d) AS d, max(s) AS s "
                f"FROM (SELECT unit, r, max(d) AS d, max(s) AS s FROM ({attempts}) GROUP BY ALL) "
                f"GROUP BY 1, r")
        fresh = (f"SELECT unit, quantile_cont(d, 0.95) AS p95, count(*) AS n, max(s) AS seen "
                 f"FROM ({runs}) GROUP BY unit")
        keep = (f" UNION ALL SELECT unit, p95, n, seen FROM read_parquet('{old}') "
                f"WHERE unit NOT IN (SELECT unit FROM fresh) "
                f"AND seen >= {time.time() - args.keep_days * 86400}") if have_old else ""
        new = tmp / "new.parquet"
        store.shell(f"COPY (WITH fresh AS ({fresh}) SELECT unit, p95, n, seen FROM fresh{keep}) "
                    f"TO '{new}' (FORMAT parquet, COMPRESSION zstd)", tmp)
        store.put(key, new)
        print(f"{key}: learned from {len(files)} result file(s)")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def cmd_select_suites(args):
    out = ["--corpus"] if args.corpus else []
    if not args.everything:
        out += ["--suite", "sqllogic", "--suite", "recovery", "--suite", "cancellation",
                "--suite", "gtest", "--suite", "drivers", "--suite", "stress"]
        if args.iresearch:
            out += ["--suite", "iresearch", "--suite", "iresearch-load"]
        if args.re2:
            out += ["--suite", "re2"]
        suites = [s for s in args.duckdb_suites.split(",") if s]
        if suites and not args.no_duckdb:
            out += ["--suite", "duckdb"]
            for s in suites:
                out += ["--duckdb-suite", s]
        if "core" in suites:
            out += ["--suite", "sqlsmith"]
    if args.postgres == "all":
        out += ["--postgres", "all"]
    elif args.postgres == "changed":
        out += ["--postgres", "changed", "--changed", "origin/main"]
    print(" ".join(out))


def run_profiles(config, arch, run, profiles):
    profiles.mkdir(parents=True)
    got = 0
    for key in store.run_files("profdata", run, f"{config}-{arch}-"):
        store.get(key, profiles / key.rsplit("/", 1)[1])
        got += 1
    return got


def in_build_image(bundle, *cmd):
    subprocess.run(["docker", "run", "--rm", "--user", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp",
                    "-v", f"{ROOT}:{ROOT}", "-w", str(ROOT), "-v", f"{bundle}:{bundle}", image_name(), *cmd],
                   check=True)


def cmd_coverage_report(args):
    arch = platform.machine()
    bundle = fetched(args.config, arch, args.digest)
    out = ROOT / "out" / f"coverage-{args.config}"
    shutil.rmtree(out, ignore_errors=True)
    profiles = out / "profiles"
    got = run_profiles(args.config, arch, args.run, profiles)
    label = f"coverage {args.config}/{arch}"
    if got < args.shards:
        label += f" (partial: profiles of {got} of {args.shards} shards)"
    in_build_image(bundle, "python3", "scripts/coverage/diff.py", "--build-dir", str(bundle),
                   "--profiles", str(profiles), "--base", args.base, "--label", label, "--out", str(out))
    shutil.rmtree(profiles)
    (out / "merged.profdata").unlink(missing_ok=True)


def cmd_coverage_nightly(args):
    arch = platform.machine()
    bundle = fetched("dev", arch, args.digest)
    out = ROOT / "out" / "coverage-nightly"
    shutil.rmtree(out, ignore_errors=True)
    profiles = out / "profiles"
    got = run_profiles("dev", arch, args.run, profiles)
    label = f"dev/{arch}, " + (f"partial: profiles of {got} of {args.shards} shards" if got < args.shards
                               else f"{got} shards")
    sha = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True,
                         check=True).stdout.strip()
    in_build_image(bundle, "python3", "scripts/coverage/nightly.py", "report", "--build-dir", str(bundle),
                   "--profiles", str(profiles), "--label", label, "--run", args.run, "--sha", sha,
                   "--out", str(out))
    shutil.rmtree(profiles)


def cmd_wait_check(args):
    deadline = time.monotonic() + args.timeout
    while True:
        res = subprocess.run(["gh", "api", f"repos/{args.repo}/commits/{args.sha}/check-runs",
                              "-X", "GET", "-f", f"check_name={args.name}", "--jq",
                              '.check_runs | sort_by(.id) | last | select(.) '
                              '| [.status, .conclusion // ""] | @tsv'],
                             capture_output=True, text=True)
        status, _, conclusion = res.stdout.strip().partition("\t")
        if res.returncode == 0 and status == "completed":
            if conclusion in ("success", "neutral", "skipped"):
                print(f"{args.name} on {args.sha}: {conclusion}")
                return
            raise SystemExit(f"{args.name} on {args.sha} concluded {conclusion}")
        if time.monotonic() > deadline:
            raise SystemExit(f"{args.name} on {args.sha} did not finish within {args.timeout:.0f}s")
        time.sleep(15)


def _rows(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def _is_failure(row):
    return row["status"] in BAD and row["attempt"] == 0 and row.get("check") != "bugfix"


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
    check = f"{meta['check']}-" if meta.get("check") else ""
    shard = f"{meta['config']}-{meta['arch']}-{check}{meta['shard'].replace('/', 'of')}"
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


def main(argv):
    ap = argparse.ArgumentParser(prog="scripts/ci/ci.py")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("image", help="the build image, tagged with a hash of the files its Dockerfile reads")
    p.set_defaults(fn=cmd_image)
    p = sub.add_parser("digest", help="hash of everything a config's build reads")
    p.add_argument("--config", required=True)
    p.add_argument("--flags", default="")
    p.add_argument("--image", default="")
    p.add_argument("--rev", default="HEAD")
    p.set_defaults(fn=cmd_digest)
    p = sub.add_parser("locate", help="print where a bundle is: cache (this box), store or none")
    p.add_argument("--config", required=True)
    p.add_argument("--arch", required=True)
    p.add_argument("--digest", required=True)
    p.set_defaults(fn=cmd_locate)
    p = sub.add_parser("stage", help="put what tests need from a build dir into the local cache")
    p.add_argument("--config", required=True)
    p.add_argument("--arch", required=True)
    p.add_argument("--digest", required=True)
    p.add_argument("--build-dir", required=True)
    p.add_argument("--runner", default="")
    p.set_defaults(fn=cmd_stage)
    p = sub.add_parser("publish", help="put a bundle of the local cache into the store")
    p.add_argument("--config", required=True)
    p.add_argument("--arch", required=True)
    p.add_argument("--digest", required=True)
    p.set_defaults(fn=cmd_publish)
    p = sub.add_parser("fetch", help="fetch and unpack a bundle into the local cache")
    p.add_argument("--config", required=True)
    p.add_argument("--arch", required=True)
    p.add_argument("--digest", required=True)
    p.set_defaults(fn=cmd_fetch)
    p = sub.add_parser("prune", help="drop this box's stale bundles and the CI images its checkout does not use")
    p.set_defaults(fn=cmd_prune)
    p = sub.add_parser("upload", help="put a file into the store")
    p.add_argument("src")
    p.add_argument("dest")
    p.set_defaults(fn=cmd_upload)
    p = sub.add_parser("download", help="get a run's files of a kind whose names start with a "
                                        "prefix, each from the latest attempt that has it")
    p.add_argument("kind")
    p.add_argument("run", help="<run id>-<attempt>")
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
    p.add_argument("--max-shards", type=int, default=64)
    p.add_argument("--check", default="", help="a second test run on the build (biglake): names "
                                               "its shards and labels their results")
    p.add_argument("test_args", nargs=argparse.REMAINDER,
                   help="test.py selection arguments after --")
    p.set_defaults(fn=cmd_shards)
    p = sub.add_parser("runs", help="join a build's shard lists (main first) into the runs of its "
                                    "tests, and say whether the build job runs them itself")
    p.add_argument("shards", nargs="+", help="shard lists `shards` printed")
    p.add_argument("--tsv", default="", help="also write the runs here, one per line")
    p.set_defaults(fn=cmd_runs)
    p = sub.add_parser("timing", help="write the store's timing snapshot for a config as JSON")
    p.add_argument("--config", required=True)
    p.add_argument("--arch", required=True)
    p.add_argument("--out", required=True)
    p.set_defaults(fn=cmd_timing)
    p = sub.add_parser("update-timing", help="learn a config's timings from main's recent results")
    p.add_argument("--config", required=True)
    p.add_argument("--arch", required=True)
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--keep-days", type=int, default=30,
                   help="how long a unit missing from the recent results keeps its timing")
    p.add_argument("--pr", default="", help="learn from this PR's runs instead (a calibration PR)")
    p.set_defaults(fn=cmd_update_timing)
    p = sub.add_parser("select-suites", help="print the test.py arguments for the suites a diff "
                                             "opens (scripts/ci/classify-changes.sh)")
    p.add_argument("--everything", action="store_true", help="every suite, whatever the diff")
    p.add_argument("--corpus", action="store_true", help="the sqlite corpus too")
    p.add_argument("--iresearch", action="store_true")
    p.add_argument("--re2", action="store_true")
    p.add_argument("--duckdb-suites", default="", help="comma-separated DuckDB suites")
    p.add_argument("--no-duckdb", action="store_true",
                   help="leave out DuckDB's suites the diff opens (sqlsmith still follows core)")
    p.add_argument("--postgres", choices=("all", "changed"),
                   help="also validate the pg/ and any/pg/ tests against PostgreSQL: all of them, or "
                        "those the diff against origin/main changes")
    p.set_defaults(fn=cmd_select_suites)
    p = sub.add_parser("coverage-report", help="report the coverage of a change from a run's profiles")
    p.add_argument("--config", default="dev")
    p.add_argument("--digest", required=True, help="the bundle the run tested")
    p.add_argument("--run", required=True, help="<run id>-<attempt>")
    p.add_argument("--base", default="origin/main")
    p.add_argument("--shards", type=int, default=0, help="how many shards the run had")
    p.set_defaults(fn=cmd_coverage_report)
    p = sub.add_parser("coverage-nightly", help="report the coverage of the whole system by component "
                                                "from a nightly's dev profiles")
    p.add_argument("--digest", required=True, help="the dev bundle the run tested")
    p.add_argument("--run", required=True, help="<run id>-<attempt>")
    p.add_argument("--shards", type=int, default=0, help="how many dev shards the run had")
    p.set_defaults(fn=cmd_coverage_nightly)
    p = sub.add_parser("wait-check", help="wait for a commit's check run to finish; fail unless it "
                                          "succeeded")
    p.add_argument("--repo", required=True)
    p.add_argument("--sha", required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--timeout", type=float, default=3600.0)
    p.set_defaults(fn=cmd_wait_check)
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
