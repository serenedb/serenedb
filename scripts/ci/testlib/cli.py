import argparse
import fnmatch
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

from . import attribution, report, selection, store
from .config import CONFIGS, Build, cpu_count, memory_gb
from .scheduler import Context, Scheduler, install_signal_handlers
from .suites import load_skips
from .suites import drivers, duckdb, fuzz, gtest, sqllogic
from .timing import Timing

ROOT = Path(__file__).resolve().parents[3]
_GENERATED = re.compile(r"^(.*?)(?:\.\d+)?\.gen\.test$")
SUITES = ("sqllogic", "recovery", "gtest", "iresearch", "iresearch-load", "drivers", "re2",
          "duckdb", "sqlsmith", "stress")


def parse_args(argv):
    ap = argparse.ArgumentParser(prog="scripts/ci/test.py",
                                 description="Runs SereneDB's test suites as one queue of units.")
    ap.add_argument("--build-dir", required=True, type=Path)
    ap.add_argument("--config", choices=sorted(CONFIGS),
                    help="config name for timings and results (default: from the build)")
    ap.add_argument("--suite", action="append", choices=SUITES,
                    help="suite to run (repeatable; default: every suite the build supports)")
    ap.add_argument("--test", action="append", default=[],
                    help="only units whose id matches this glob (repeatable)")
    ap.add_argument("--exclude", action="append", default=[],
                    help="drop units whose id matches this glob (repeatable)")
    ap.add_argument("--fast", action="store_true", help="skip .test_slow files")
    ap.add_argument("--biglake", action="store_true",
                    help="run the iceberg tests against Google BigLake (needs BIGLAKE_* env)")
    ap.add_argument("--duckdb-suite", dest="duckdb_suites", action="append",
                    choices=duckdb.SUITES, help="DuckDB suite (repeatable; default: all)")
    ap.add_argument("--fuzz-seconds", type=float, default=300.0, help="sqlsmith time budget")
    ap.add_argument("--stress-profile", default="smoke", help="tests/drivers/stress profile")
    ap.add_argument("--jobs", type=int, default=0, help="CPU slots (default: all CPUs)")
    ap.add_argument("--mem", type=float, default=0, help="memory budget in GB (default: 75%% of RAM)")
    ap.add_argument("--servers", type=int, default=0, help="shared serened count for sqllogic")
    ap.add_argument("--shard", default="1/1", help="run shard I of N (1-based), like 2/5")
    ap.add_argument("--out", type=Path, default=None, help="output directory (default: out/test)")
    ap.add_argument("--timing", default="", help="timing snapshot (JSON) for ordering and timeouts")
    ap.add_argument("--relabel", type=float, default=600.0,
                    help="seconds to spend rerunning failed units to label them flaky")
    ap.add_argument("--changed", default="",
                    help="base ref: units whose test files changed since its merge-base")
    ap.add_argument("--repeat", type=int, default=0,
                    help="run the --changed units this many more times; a failure is flaky")
    ap.add_argument("--repeat-budget", type=float, default=900.0,
                    help="seconds the --repeat rounds may take")
    ap.add_argument("--only-changed", action="store_true",
                    help="run only the --changed units")
    ap.add_argument("--expect-fail", action="store_true",
                    help="every unit must fail (a fix's tests against the build without it)")
    ap.add_argument("--attribute", action="store_true",
                    help="coverage builds: every unit gets its own server and profile, and the "
                         "functions it runs are recorded for test selection")
    ap.add_argument("--selection-report", type=Path, default=None,
                    help="write which units the store's selection map picks for the --changed "
                         "diff (report only: the run still runs every unit)")
    ap.add_argument("--list", action="store_true", help="print the units and exit")
    ap.add_argument("--keep", action="store_true", help="keep the directories of passed units")
    ap.add_argument("--verbose", action="store_true", help="log every unit")
    return ap.parse_args(argv)


def build_units(ctx, args, suites, skips):
    units = []
    if "sqllogic" in suites:
        units += sqllogic.sqllogic_units(ctx, args, skips)
    if "recovery" in suites and ctx.build.fault_injection:
        units += sqllogic.recovery_units(ctx, args, skips)
    if "gtest" in suites:
        units += gtest.gtest_units(ctx, args, skips)
    if "iresearch" in suites:
        units += gtest.iresearch_units(ctx, args, skips)
    if "drivers" in suites:
        units += drivers.driver_units(ctx, args, skips)
    if "re2" in suites:
        units += drivers.re2_units(ctx, args, skips)
    if "duckdb" in suites:
        units += duckdb.duckdb_units(ctx, args, skips)
    if "sqlsmith" in suites:
        units += fuzz.sqlsmith_units(ctx, args, skips)
    if "stress" in suites:
        units += fuzz.stress_units(ctx, args, skips)
    if "iresearch-load" in suites:
        units += fuzz.iresearch_load_units(ctx, args, skips)
    if args.test:
        units = [u for u in units if any(fnmatch.fnmatchcase(u.id, p) for p in args.test)
                 or any(fnmatch.fnmatchcase(m, p) for m in u.members for p in args.test)]
    if args.exclude:
        units = [u for u in units if not any(fnmatch.fnmatchcase(u.id, p) for p in args.exclude)]
    return units


def _git(repo, *args, check=True):
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True,
                          check=check)


def changed_files(base):
    merge_base = _git(ROOT, "merge-base", base, "HEAD").stdout.strip()
    names = _git(ROOT, "diff", "--name-only", "--diff-filter=AMR", merge_base,
                 "HEAD").stdout.splitlines()
    raw = _git(ROOT, "diff", "--raw", "--no-abbrev", merge_base, "HEAD", "--", "third_party")
    for line in raw.stdout.splitlines():
        meta, _, path = line.partition("\t")
        fields = meta.split()
        if len(fields) < 4 or fields[0] != ":160000" or fields[1] != "160000":
            continue
        old, new = fields[2], fields[3]
        sub = ROOT / path
        if _git(sub, "cat-file", "-e", f"{old}^{{commit}}", check=False).returncode != 0:
            _git(sub, "fetch", "-q", "--no-tags", "--depth=1", "origin", old, check=False)
        diff = _git(sub, "diff", "--name-only", "--diff-filter=AMR", old, new, check=False)
        if diff.returncode != 0:
            print(f"cannot diff {path} {old[:12]}..{new[:12]}; its changed tests are not picked",
                  file=sys.stderr)
            continue
        names += [f"{path}/{n}" for n in diff.stdout.splitlines()]
    return names


def changed_units(units, base):
    names = changed_files(base)
    keys = set()
    generated = set()
    for name in names:
        if name.startswith("tests/sqllogic/generators/") and name.endswith((".py", ".recorded")):
            generated.add(name[len("tests/sqllogic/generators/"):].rsplit(".", 1)[0])
            continue
        if name.startswith("tests/sqllogic/"):
            keys.add(name[len("tests/sqllogic/"):])
        elif name.startswith("tests/drivers/"):
            keys.add("drivers:" + name.split("/")[2])
        elif "/test/" in name and name.startswith("third_party/"):
            keys.add(str(ROOT / name))
            keys.add(name.split("/", 2)[2])
    picked = []
    for u in units:
        tail = u.id.split(":", 2)[-1].split("#", 1)[0]
        source = _GENERATED.match(tail)
        if (tail in keys or u.id in keys or (source and source.group(1) in generated)
                or any(m.split(":", 2)[-1] in keys for m in u.members)):
            picked.append(u)
    return picked


def write_selection(args, units, changed, out):
    work = out / "selection"
    work.mkdir(parents=True, exist_ok=True)
    local = work / "map.parquet"
    if not store.reachable() or not store.get(f"selection/dev-{platform.machine()}.parquet", local):
        result = {"mode": "no map"}
    else:
        result = selection.report(units, args.changed, changed_files(args.changed), changed,
                                  local, work, pr=os.environ.get("SDB_PR", ""))
    args.selection_report.write_text(json.dumps(result, indent=1))


def shard(units, index, count):
    if count == 1:
        return units
    loads = [0.0] * count
    picked = []
    for u in sorted(units, key=lambda u: (-u.cost, u.id)):
        k = min(range(count), key=lambda i: (loads[i], i))
        loads[k] += u.cost
        if k == index:
            picked.append(u)
    return picked


def main(argv):
    args = parse_args(argv)
    build_dir = args.build_dir if args.build_dir.is_absolute() else ROOT / args.build_dir
    build = Build.load(ROOT, build_dir.resolve())
    config = CONFIGS[args.config or build.config_name()]
    index, count = (int(x) for x in args.shard.split("/"))
    out = args.out or ROOT / "out" / "test"
    out = out if out.is_absolute() else ROOT / out
    if out.exists() and not args.list:
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "profiles").mkdir(exist_ok=True)
    jobs = args.jobs or cpu_count()
    base_env = dict(os.environ)
    base_env["RESOURCES"] = str(ROOT / "resources")
    base_env["WORKSPACE"] = str(ROOT)
    base_env["BUILD_DIR"] = os.path.relpath(build.dir, ROOT)
    base_env["SERENED"] = str(build.bin("serened"))
    base_env["SDB_DRV_SERENED_BIN"] = str(build.bin("serened"))
    base_env.setdefault("SDB_DOCKER_OWNER", f"test-py-{os.getpid()}")
    os.environ["SDB_DOCKER_OWNER"] = base_env["SDB_DOCKER_OWNER"]
    if args.attribute and not build.coverage:
        raise SystemExit("--attribute needs a coverage build (SDB_COVERAGE=On)")
    ctx = Context(root=ROOT, build=build, config=config, out_dir=out,
                  timing=Timing.load(args.timing), jobs=jobs,
                  mem_gb=args.mem or memory_gb() * 0.75, verbose=args.verbose or args.keep,
                  base_env=base_env, attribute=args.attribute)
    suites = args.suite or list(SUITES)
    skips = load_skips(ROOT, config.name)
    units = build_units(ctx, args, suites, skips)
    changed = changed_units(units, args.changed) if args.changed else []
    if args.only_changed:
        units = changed
    if args.selection_report:
        if not args.changed:
            raise SystemExit("--selection-report needs --changed")
        write_selection(args, units, changed, out)
    plan = hashlib.sha256("".join(sorted(f"{u.id}\t{u.cost!r}\n" for u in units)).encode()).hexdigest()
    units = shard(units, index - 1, count)
    changed_ids = {u.id for u in changed}
    changed = [u for u in units if u.id in changed_ids]
    if args.list:
        for u in sorted(units, key=lambda u: (-u.cost, u.id)):
            print(f"{u.cost:8.1f}s {u.timeout:7.0f}s {u.cpus} {u.id}")
        print(f"{len(units)} units, {sum(u.cost for u in units):.0f}s expected", file=sys.stderr)
        return 0
    factories = {}
    factories.update(sqllogic.fixture_factories(ctx, args))
    factories.update(drivers.fixture_factories(ctx, args))
    factories.update(duckdb.fixture_factories(ctx, args))
    factories.update(fuzz.fixture_factories(ctx, args))
    if any(u.suite == "duckdb" for u in units):
        duckdb.clean_scratch(ctx)
    sched = Scheduler(ctx, units, factories)
    install_signal_handlers(sched)
    started = time.time()
    print(f"{config.name}: {len(units)} units on {jobs} CPUs (shard {index}/{count}), "
          f"output in {out}", flush=True)
    try:
        sched.run_pass(sched.units)
        if args.expect_fail:
            sched.invert()
        else:
            sched.relabel(args.relabel)
            if args.repeat and changed:
                sched.repeat(changed, args.repeat, args.repeat_budget)
    finally:
        sched.stop_fixtures()
    results = sched.results
    meta = {
        "config": config.name, "arch": platform.machine(), "shard": args.shard,
        "started": started, "duration": time.time() - started,
        "sha": os.environ.get("SDB_SHA", ""), "pr": os.environ.get("SDB_PR", ""),
        "branch": os.environ.get("SDB_BRANCH", ""), "run": os.environ.get("SDB_RUN_ID", ""),
        "attribute": args.attribute, "plan": plan,
    }
    report.write(out, meta, results, ctx.skips)
    if args.attribute and (attribution.directory(ctx) / "index.jsonl").is_file():
        store.attribution_parquet(attribution.directory(ctx), out / "attribution.parquet")
    print(report.summary(meta, results, ctx.skips))
    return 1 if report.failed(results) else 0
