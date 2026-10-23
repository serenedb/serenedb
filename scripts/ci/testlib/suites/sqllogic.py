import os
import re
import subprocess
import threading
from pathlib import Path

import services

from .. import reports
from ..scheduler import CRASHED, FAILED, PASSED, STUCK, Fixture, Outcome, Unit
from ..servers import RestartingServer, ServerPool
from . import plan, skipped

ENGINES = ("pg-wire-simple", "pg-wire-extended")
PLANNED_EXITS = (0, 75)
STATEFUL_SERVICES = ("minio", "azurite", "iceberg", "postgres", "clickhouse")
CORPUS = "sdb/pg/any/sqlite/"
PART_SECONDS = 10.0
MAX_PARTS = 64
_runner_lock = threading.Lock()
_runner_path = None


def sqllogic_dir(ctx):
    return ctx.root / "tests" / "sqllogic"


def runner_binary(ctx):
    global _runner_path
    with _runner_lock:
        if _runner_path:
            return _runner_path
        bundled = ctx.build.bin("sqllogictest")
        if bundled.is_file():
            _runner_path = bundled
            return bundled
        target = Path(os.environ.get("CARGO_TARGET_DIR", ctx.root / ".cache" / "cargo-target"))
        manifest = ctx.root / "third_party" / "sqllogictest-rs" / "sqllogictest-bin" / "Cargo.toml"
        subprocess.run(["cargo", "build", "--manifest-path", str(manifest), "--target-dir",
                        str(target), "--release", "--quiet"], check=True)
        _runner_path = target / "release" / "sqllogictest"
        return _runner_path


def generate_tests(ctx):
    base = sqllogic_dir(ctx)
    for generator in sorted((base / "generators").rglob("*.py")):
        rel = generator.relative_to(base / "generators")
        target = base / rel.with_suffix(".gen.test")
        subprocess.run(["python3", str(base / "generated.py"), "write", str(generator), str(target)],
                       check=True)


def find_tests(root, fast):
    found = []
    seen = set()
    for dirpath, dirnames, filenames in os.walk(root, followlinks=True):
        real = os.path.realpath(dirpath)
        if real in seen:
            dirnames[:] = []
            continue
        seen.add(real)
        dirnames.sort()
        for name in sorted(filenames):
            if name.endswith(".test") or (not fast and name.endswith(".test_slow")):
                found.append(Path(dirpath) / name)
    return found


class ServicesFixture(Fixture):
    def __init__(self, ctx, names):
        self.names = names
        self.stateful = any(n in STATEFUL_SERVICES for n in names)
        self.services = services.ServiceSet(log_dir=ctx.out_dir / "services")
        self._env = {}

    def start(self, ctx):
        self._env = self.services.start(self.names)

    def env(self):
        return dict(self._env)

    def stop(self):
        self.services.stop()


def service_fixture_names(path, biglake):
    names = services.services_for_test(str(path))
    if biglake:
        names = ["biglake" if n == "iceberg" else n for n in names]
    return tuple(f"svc:{n}" for n in names)


def service_factories(biglake):
    factories = {}
    for name in ("minio", "azurite", "iceberg", "ollama", "kev", "postgres", "clickhouse",
                 "iceberg-data", "biglake"):
        factories[f"svc:{name}"] = (lambda n: lambda ctx: ServicesFixture(ctx, [n]))(name)
    return factories


def _runner_failure(log):
    text = reports.tail(log, 60000)
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.strip() == "Caused by:":
            what = next((ln.strip() for ln in lines[i + 1:] if ln.strip()), "")
            where = next((ln.strip() for ln in lines[i + 1:] if ln.strip().startswith("at ")), "")
            return f"{what.rstrip(':')} {where}".strip()[:300]
    tail = [ln for ln in lines if ln.strip()]
    return tail[-1][:300] if tail else ""


def _runner_args(ctx, rel, engine, port, ssl_port, env, part=None):
    argv = [runner_binary(ctx), rel, "--host", "127.0.0.1", "--port", str(port),
            "--engine", engine, "--jobs", "1", "--label", "serenedb", "--shutdown-timeout", "60"]
    if ssl_port:
        argv += ["--ssl-port", str(ssl_port)]
    if env.get("PGSOCKETDIR"):
        argv += ["--label", "pgsocket"]
    if CORPUS in rel:
        argv += ["--show-column-names", "false", "--flat-values", "true", "--hash-threshold", "8"]
    if part:
        argv += ["--part", f"{part[0]}/{part[1]}"]
    return argv


def corpus_parts(ctx, uid):
    cost = ctx.timing.expect(uid)
    if cost is None:
        return 1
    return max(1, min(MAX_PARTS, round(cost / (PART_SECONDS * ctx.config.slowdown))))


def sqllogic_units(ctx, args, skips):
    base = sqllogic_dir(ctx)
    generate_tests(ctx)
    units = []
    for path in find_tests(base / "sdb", args.fast):
        rel = str(path.relative_to(base))
        try:
            fixtures = service_fixture_names(path, args.biglake)
        except ValueError as e:
            raise SystemExit(str(e))
        if args.biglake and path.name.endswith("_fixture_iceberg.test_slow"):
            continue
        if args.biglake and "svc:biglake" not in fixtures:
            continue
        for engine in ENGINES:
            uid = f"sqllogic:{engine}:{rel}"
            skip = skipped(skips, uid)
            if skip:
                ctx.skips.append((uid, skip))
                continue
            cost, timeout = plan(ctx, uid, 2.0)
            if CORPUS not in rel:
                units.append(Unit(uid, "sqllogic", _sqllogic_run(ctx, rel, engine), cpus=1,
                                  mem_gb=0.3, fixtures=("servers",) + fixtures, cost=cost,
                                  timeout=timeout, group=f"sqllogic:{rel}"))
                continue
            parts = corpus_parts(ctx, uid)
            for k in range(1, parts + 1):
                part = (k, parts) if parts > 1 else None
                units.append(Unit(f"{uid}#{k}/{parts}" if part else uid, "sqllogic",
                                  _sqllogic_run(ctx, rel, engine, part), cpus=1, mem_gb=0.3,
                                  fixtures=("servers",) + fixtures, cost=cost / parts,
                                  timeout=max(ctx.timeout_floor, timeout / parts)))
    return units


def _sqllogic_run(ctx, rel, engine, part=None):
    def run(r):
        pool = r.fixture("servers")
        i, generation = pool.acquire()
        try:
            server = pool.server(i)
            r.stack_pids = lambda: (server.pid(),)
            res = r.exec(_runner_args(ctx, rel, engine, server.port, server.ssl_port, r.env, part),
                         cwd=sqllogic_dir(ctx))
        finally:
            pool.release(i)
        out = r.outcome(res)
        crash = pool.crashed_during(i, generation)
        if crash and out.status != PASSED:
            return Outcome(CRASHED, f"shared serened died during this unit: {crash}", signature=crash)
        if out.status == FAILED and not out.signature:
            out.detail = _runner_failure(r.log) or out.detail
        return out

    return run


def recovery_units(ctx, args, skips):
    base = sqllogic_dir(ctx)
    units = []
    for path in sorted((base / "recovery").iterdir()):
        if not (path.name.endswith(".test") or (not args.fast and path.name.endswith(".test_slow"))):
            continue
        rel = str(path.relative_to(base))
        try:
            fixtures = service_fixture_names(path, args.biglake)
        except ValueError as e:
            raise SystemExit(str(e))
        if args.biglake and (path.name.endswith("_fixture_iceberg.test_slow")
                             or "svc:biglake" not in fixtures):
            continue
        uid = f"recovery:{rel}"
        skip = skipped(skips, uid)
        if skip:
            ctx.skips.append((uid, skip))
            continue
        cost, timeout = plan(ctx, uid, 10.0)
        units.append(Unit(uid, "recovery", _recovery_run(ctx, rel), cpus=2, mem_gb=1.5,
                          fixtures=fixtures, cost=cost, timeout=timeout))
    return units


def _recovery_run(ctx, rel):
    def run(r):
        server = RestartingServer(ctx, r.dir / "server").start()
        r.stack_pids = lambda: (server.pid(),)
        env = {"SDB_DATADIR": str(server.datadir), "RETRY_ATTEMPTS": "60",
               "BACKOFF_DURATION": "500ms"}
        try:
            res = r.exec(_runner_args(ctx, rel, "pg-wire-simple", server.port, None, r.env),
                         cwd=sqllogic_dir(ctx), env=env)
        finally:
            stop = server.stop()
        out = r.outcome(res, san_dirs=(server.server.san_dir,))
        if out.status in (STUCK, CRASHED):
            return out
        unplanned = sorted({rc for rc in server.exits if rc not in PLANNED_EXITS})
        if stop and stop[0] == "stuck":
            return Outcome(STUCK, "serened did not stop on SIGTERM at the end of the test",
                           signature="serened stop stuck")
        if unplanned:
            sig = server.server.crash_signature()
            return Outcome(CRASHED, f"serened exited with {unplanned} (planned: 0, 75): {sig}",
                           signature=sig)
        if out.status == FAILED and not out.signature:
            out.detail = _runner_failure(r.log) or out.detail
        return out

    return run


def fixture_factories(ctx, args):
    factories = service_factories(args.biglake)
    factories["servers"] = lambda c: ServerPool(c, count=args.servers or max(
        1, c.jobs // c.config.units_per_server), threads=0,
        extra_env={"POSTGRES_PASSWORD": "postgres"})
    return factories
