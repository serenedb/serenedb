import argparse
import fnmatch
import hashlib
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

from . import report
from .config import CONFIGS, Build, cpu_count, memory_gb
from .scheduler import INTERRUPTED_EXIT, Context, Scheduler, install_signal_handlers
from .suites import load_skips
from .suites import drivers, duckdb, fuzz, gtest, sqllogic
from .timing import Timing

ROOT = Path(__file__).resolve().parents[3]
_GENERATED = re.compile(r"^(.*?)(?:\.\d+)?\.gen\.test$")
_GTEST_CASE = re.compile(r"^\s*(?:TYPED_)?TEST(?:_F|_P)?\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)", re.M)
_CATCH_CASE = re.compile(r'^\s*TEST_CASE\s*\(\s*"((?:[^"\\]|\\.)*)"', re.M)
SCRIPT_TESTS = ("sqllogic:", "recovery:", "postgres:")
DRIVER_DIRS = {"network": ("network:hba",), "spec": ("drivers:*",), "run.sh": ("drivers:*",),
               "stress": ("stress:*",), "sqlsmith": ("sqlsmith",)}
DUCKDB_DIRS = {d: s for s, d in duckdb.SUITE_DIRS.items() if s != "cpp"}
DUCKDB_TESTS = (".test", ".test_slow", ".test_coverage")
CPP_SOURCES = (".cpp", ".cc", ".h", ".hpp")
SUITES = ("sqllogic", "recovery", "cancellation", "gtest", "iresearch", "iresearch-load", "drivers",
          "re2", "croaring", "duckdb", "sqlsmith", "stress", "postgres")
DEFAULT_SUITES = tuple(s for s in SUITES if s != "postgres")


def parse_args(argv):
    ap = argparse.ArgumentParser(prog="scripts/ci/test.py",
                                 description="Runs SereneDB's test suites as one queue of units.")
    ap.add_argument("--build-dir", type=Path,
                    help="the build to test (not needed for the postgres suite or with --server)")
    ap.add_argument("--config", choices=sorted(CONFIGS),
                    help="config name for timings and results (default: from the build)")
    ap.add_argument("--suite", action="append", choices=SUITES,
                    help="suite to run (repeatable; default: every suite the build supports but "
                         "postgres, which runs the PostgreSQL-compatible tests against PostgreSQL)")
    ap.add_argument("--server", metavar="HOST:PORT",
                    help="run the sqllogic or postgres units against this running server instead "
                         "of starting servers")
    ap.add_argument("--server-ssl-port", type=int, default=None,
                    help="the --server's port for encrypted connections")
    ap.add_argument("--override", action="store_true",
                    help="rewrite the sqllogic test files with the results the server gives, on "
                         "one wire engine, and record the generated tests' values")
    ap.add_argument("--test", action="append", default=[],
                    help="only units whose id matches this glob (repeatable)")
    ap.add_argument("--exclude", action="append", default=[],
                    help="drop units whose id matches this glob (repeatable)")
    ap.add_argument("--fast", action="store_true", help="skip .test_slow files")
    ap.add_argument("--corpus", action="store_true",
                    help="also run the sqlite corpus (tests/sqllogic/any/pg/sqlite), "
                         "checking the submodule out when it is not")
    ap.add_argument("--biglake", action="store_true",
                    help="run the iceberg tests against Google BigLake (needs BIGLAKE_* env)")
    ap.add_argument("--long-tests", action="store_true",
                    help="also run the gtests too long for a PR run (SDB_LONG_TESTS)")
    ap.add_argument("--duckdb-suite", dest="duckdb_suites", action="append",
                    choices=duckdb.SUITES, help="DuckDB suite (repeatable; default: all)")
    ap.add_argument("--fuzz-seconds", type=float, default=300.0,
                    help="time budget of each sqlsmith and cancellation unit")
    ap.add_argument("--stress-profile", default="smoke", help="tests/drivers/stress profile")
    ap.add_argument("--jobs", type=int, default=0, help="CPU slots (default: all CPUs)")
    ap.add_argument("--mem", type=float, default=0, help="memory budget in GB (default: 75%% of RAM)")
    ap.add_argument("--servers", type=int, default=0, help="shared serened count for sqllogic")
    ap.add_argument("--shard", default="1/1", help="run shard I of N (1-based), like 2/5")
    ap.add_argument("--out", type=Path, default=None, help="output directory (default: out/test)")
    ap.add_argument("--timing", default="", help="timing snapshot (JSON) for ordering and timeouts")
    ap.add_argument("--budget", type=float, default=0,
                    help="seconds the run may take: no unit starts later, the units still running "
                         "then are stopped with their stacks, and the ones not started fail")
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
    ap.add_argument("--postgres", choices=("all", "changed"),
                    help="also run the postgres suite: all of it, or only its --changed tests")
    ap.add_argument("--expect-fail", action="store_true",
                    help="every unit must fail (a fix's tests against the build without it)")
    ap.add_argument("--list", action="store_true",
                    help="print the units and exit, and the digest of the plan to stderr")
    ap.add_argument("--plan", default="",
                    help="the digest --list printed for this run: a shard that plans other units "
                         "fails instead of running them")
    ap.add_argument("--keep", action="store_true", help="keep the directories of passed units")
    ap.add_argument("--verbose", action="store_true", help="log every unit")
    return ap.parse_args(argv)


def build_units(ctx, args, suites, skips):
    units = []
    if "sqllogic" in suites:
        units += sqllogic.sqllogic_units(ctx, args, skips)
    if "recovery" in suites and ctx.build.fault_injection:
        units += sqllogic.recovery_units(ctx, args, skips)
    if "cancellation" in suites:
        units += fuzz.cancellation_units(ctx, args, skips)
    if "postgres" in suites:
        units += sqllogic.postgres_units(ctx, args, skips)
    if "gtest" in suites:
        units += gtest.gtest_units(ctx, args, skips)
    if "iresearch" in suites:
        units += gtest.iresearch_units(ctx, args, skips)
    if "drivers" in suites:
        units += drivers.driver_units(ctx, args, skips)
    if "re2" in suites:
        units += gtest.re2_units(ctx, args, skips)
    if "croaring" in suites:
        units += gtest.croaring_units(ctx, args, skips)
    if "duckdb" in suites:
        units += duckdb.duckdb_units(ctx, args, skips)
    if "sqlsmith" in suites:
        units += fuzz.sqlsmith_units(ctx, args, skips)
    if "stress" in suites:
        units += fuzz.stress_units(ctx, args, skips)
        units += fuzz.stress_harness_units(ctx, args, skips)
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


class ChangedTests:
    def __init__(self, names, compiled):
        self.ids = defaultdict(set)
        self.globs = defaultdict(set)
        self.scripts = {}
        self.generated = {}
        self.gtest_cases = defaultdict(set)
        self.catch_cases = defaultdict(set)
        for name in names:
            self._add(name, compiled)

    def _add(self, name, compiled):
        parts = name.split("/")
        if name.startswith("tests/sqllogic/generators/") and name.endswith((".py", ".recorded")):
            rel = name[len("tests/sqllogic/generators/"):].rsplit(".", 1)[0]
            self.generated[os.path.realpath(ROOT / "tests" / "sqllogic" / rel)] = name
        elif name.startswith("tests/sqllogic/"):
            self.scripts[os.path.realpath(ROOT / name)] = name
        elif name.startswith("tests/drivers/"):
            uids = DRIVER_DIRS.get(parts[2])
            if uids is None:
                stem = parts[3].rsplit(".", 1)[0] if len(parts) == 4 else ""
                uids = (f"drivers:{parts[2]}",
                        f"drivers:{parts[2]}:{stem if stem.startswith('test_') else '*'}")
            for uid in uids:
                (self.globs if "*" in uid else self.ids)[uid].add(name)
        elif name.startswith("third_party/") and "/test/" in name and name.endswith(DUCKDB_TESTS):
            suite = DUCKDB_DIRS.get("/".join(parts[:2]))
            if suite:
                self.ids[f"duckdb:{suite}:{'/'.join(parts[2:])}"].add(name)
        if compiled and name.startswith("third_party/croaring/tests/") and name.endswith((".c", ".cpp")):
            self.ids[f"croaring:{parts[-1].rsplit('.', 1)[0]}"].add(name)
            return
        if not compiled or not name.endswith(CPP_SOURCES):
            return
        if name.startswith(("tests/", "third_party/re2/re2/testing/")):
            for case in _GTEST_CASE.findall((ROOT / name).read_text(errors="replace")):
                self.gtest_cases[case].add(name)
        elif name.startswith("third_party/duckdb/test/"):
            for case in _CATCH_CASE.findall((ROOT / name).read_text(errors="replace")):
                self.catch_cases[re.sub(r"\\(.)", r"\1", case)].add(name)

    def sources(self, uid):
        found = set(self.ids.get(uid.partition("#")[0], ()))
        for glob, names in self.globs.items():
            if fnmatch.fnmatchcase(uid, glob):
                found |= names
        kind, _, rest = uid.partition(":")
        if kind == "gtest":
            heads, _, tail = rest.partition(":")[2].partition(".")
            test = tail.split("/", 1)[0]
            for suite in heads.split("/"):
                found |= self.gtest_cases.get((suite, test), set())
        elif uid.startswith("duckdb:cpp:"):
            found |= self.catch_cases.get(uid[len("duckdb:cpp:"):], set())
        elif uid.startswith(SCRIPT_TESTS):
            base = ROOT / "tests" / "sqllogic"
            rel = uid.split(":", 2)[-1].split("#", 1)[0]
            source = _GENERATED.match(rel)
            for path in (self.scripts.get(os.path.realpath(base / rel)),
                         source and self.generated.get(os.path.realpath(base / source.group(1)))):
                if path:
                    found.add(path)
        return found


def generated_sources(units):
    base = ROOT / "tests" / "sqllogic"
    return {os.path.realpath(base / source.group(1)) for u in units if u.suite in ("sqllogic", "postgres")
            and (source := _GENERATED.match(u.id.split(":", 2)[-1].split("#", 1)[0]))}


def changed_units(units, base, compiled=True):
    changed = ChangedTests(changed_files(base), compiled)
    picked = []
    tests = set()
    for u in units:
        members = {uid for uid in (*u.members, u.id) if changed.sources(uid)}
        if members:
            picked.append(u)
            tests |= members - {u.id} or {u.id}
    return picked, tests


def keep_changed_postgres(units, changed):
    picked = {u.id for u in changed}
    return [u for u in units if u.suite != "postgres" or u.id in picked]


def shard(units, index, count, startup=lambda fixture: 0.0):
    if count == 1:
        return units
    loads = [0.0] * count
    started = [set() for _ in range(count)]
    picked = []
    for u in sorted(units, key=lambda u: (-u.cost * u.cpus, u.id)):
        fixtures = {*u.fixtures, *u.shared}
        k = min(range(count),
                key=lambda i: (loads[i] + sum(startup(f) for f in fixtures - started[i]), i))
        loads[k] += u.cost * u.cpus
        started[k] |= fixtures
        if k == index:
            picked.append(u)
    return picked


def main(argv):
    args = parse_args(argv)
    budget_end = time.monotonic() + args.budget if args.budget else math.inf
    suites = args.suite or list(DEFAULT_SUITES)
    if args.postgres and "postgres" not in suites:
        suites = [*suites, "postgres"]
    if args.server and {"sqllogic", "postgres"} <= set(suites):
        raise SystemExit("--server serves either the sqllogic or the postgres suite, not both")
    if args.override and {"sqllogic", "postgres"} <= set(suites):
        raise SystemExit("--override records either the sqllogic or the postgres suite, not both")
    if args.server and "recovery" in suites:
        raise SystemExit("recovery units restart their own servers: run them without --server")
    needs_build = [s for s in suites if s != "postgres" and not (s == "sqllogic" and args.server)]
    if args.build_dir:
        build_dir = args.build_dir if args.build_dir.is_absolute() else ROOT / args.build_dir
        build = Build.load(ROOT, build_dir.resolve())
    elif needs_build:
        raise SystemExit(f"--build-dir is needed for {', '.join(needs_build)}")
    else:
        build = Build.absent(ROOT)
    config = CONFIGS[args.config or (build.config_name() if build.dir else "perf")]
    if os.environ.get("SLT_TEST_DIR_SHARED"):
        os.umask(0)
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
    if args.long_tests:
        base_env["SDB_LONG_TESTS"] = "1"
    base_env["WORKSPACE"] = str(ROOT)
    if build.dir:
        base_env["BUILD_DIR"] = os.path.relpath(build.dir, ROOT)
        base_env["SERENED"] = str(build.bin("serened"))
        base_env["SDB_DRV_SERENED_BIN"] = str(build.bin("serened"))
        base_env["SDB_FAULT_INJECTION"] = "1" if build.fault_injection else "0"
    base_env.setdefault("SDB_DOCKER_OWNER", f"test-py-{os.getpid()}")
    os.environ["SDB_DOCKER_OWNER"] = base_env["SDB_DOCKER_OWNER"]
    base_env.update(build.coverage_env(out / "profiles"))
    os.environ.update(build.coverage_env(out / "profiles"))
    ctx = Context(root=ROOT, build=build, config=config, out_dir=out,
                  timing=Timing.load(args.timing), jobs=jobs,
                  mem_gb=args.mem or memory_gb() * 0.75, verbose=args.verbose or args.keep,
                  base_env=base_env, budget_end=budget_end)
    skips = load_skips(ROOT, config.name)
    units = build_units(ctx, args, suites, skips)
    if (args.expect_fail or args.postgres == "changed") and not args.changed:
        raise SystemExit("--expect-fail and --postgres changed need --changed")
    changed, changed_tests = (changed_units(units, args.changed, compiled=not args.expect_fail)
                              if args.changed else ([], set()))
    if args.expect_fail and not changed:
        raise SystemExit("the diff changes no test that can fail on the merge base: the bugfix check "
                         "runs the changed sqllogic, recovery, driver and DuckDB sqllogic tests, "
                         "while gtest, DuckDB C++, re2 and CRoaring tests are compiled into its "
                         "binaries")
    if args.only_changed:
        units = changed
    elif args.postgres == "changed":
        units = keep_changed_postgres(units, changed)
    plan =hashlib.sha256("".join(sorted(f"{u.id}\t{u.cost!r}\n" for u in units)).encode()).hexdigest()
    if args.plan and args.plan != plan:
        raise SystemExit(f"this shard planned other units than the build job (plan {plan[:12]}, "
                         f"not {args.plan[:12]}): the shards would run some units twice and others "
                         "never")
    units = shard(units, index - 1, count, ctx.fixture_startup)
    changed_ids = {u.id for u in changed}
    changed = [u for u in units if u.id in changed_ids]
    if args.list:
        for u in sorted(units, key=lambda u: (-u.cost, u.id)):
            print(f"{u.cost:8.1f}s {u.timeout:7.0f}s {u.cpus} {u.id}")
        print(f"{len(units)} units, {sum(u.cost for u in units):.0f}s expected", file=sys.stderr)
        print(f"plan {plan}", file=sys.stderr)
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
        if not args.expect_fail and not args.override:
            sched.relabel(args.relabel)
            if args.repeat and changed:
                sched.repeat(changed, args.repeat, args.repeat_budget)
    finally:
        sched.stop_fixtures()
    if args.override:
        sqllogic.generated_tests(ctx, "record", only=generated_sources(units),
                                 engine="postgres" if "postgres" in suites else "serenedb")
    if args.expect_fail:
        sched.invert(changed_tests)
    results = sched.results
    meta = {
        "config": config.name, "arch": platform.machine(), "shard": args.shard,
        "started": started, "duration": time.time() - started,
        "sha": os.environ.get("SDB_SHA", ""), "pr": os.environ.get("SDB_PR", ""),
        "branch": os.environ.get("SDB_BRANCH", ""), "run": os.environ.get("SDB_RUN_ID", ""),
        "plan": plan, "check": os.environ.get("SDB_CHECK", ""), "interrupted": sched.interrupted,
    }
    shard_skips = ctx.skips if index == 1 else []
    report.write(out, meta, results, shard_skips)
    print(report.summary(meta, results, shard_skips))
    if sched.interrupted:
        return INTERRUPTED_EXIT
    return 1 if report.failed(results) else 0
