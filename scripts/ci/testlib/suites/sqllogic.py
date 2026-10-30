import os
import re
import subprocess
import threading
from pathlib import Path

import services

from .. import reports
from ..scheduler import CRASHED, FAILED, PASSED, STUCK, Fixture, Outcome, Unit
from ..servers import RestartingServer, Server, ServerPool
from . import plan, skipped

ENGINES = ("pg-wire-simple", "pg-wire-extended")
PLANNED_EXITS = (0, 75)
STATEFUL_SERVICES = ("minio", "azurite", "iceberg", "postgres", "clickhouse")
INFERENCE_CPUS = {"svc:ollama": 8, "svc:kev": 8}
INFERENCE_TIMEOUT = 900.0
CORPUS = "any/sqlite/"
CORPUS_SECONDS_PER_MB = 18.0
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
        bundled = ctx.build.bin("sqllogictest") if ctx.build.dir else None
        if bundled and bundled.is_file():
            _runner_path = bundled
            return bundled
        target = Path(os.environ.get("CARGO_TARGET_DIR", ctx.root / ".cache" / "cargo-target"))
        manifest = ctx.root / "third_party" / "sqllogictest-rs" / "sqllogictest-bin" / "Cargo.toml"
        subprocess.run(["cargo", "build", "--manifest-path", str(manifest), "--target-dir",
                        str(target), "--release", "--quiet"], check=True)
        _runner_path = target / "release" / "sqllogictest"
        return _runner_path


def generated_tests(ctx, command, only=None, engine="serenedb"):
    base = sqllogic_dir(ctx)
    for generator in sorted((base / "generators").rglob("*.py")):
        rel = generator.relative_to(base / "generators")
        if only is not None and os.path.realpath(base / rel.with_suffix("")) not in only:
            continue
        target = base / rel.with_suffix(".gen.test")
        subprocess.run(["python3", str(base / "generated.py"), command, str(generator), str(target),
                        engine], check=True)


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


class ExternalServer(Fixture):
    def __init__(self, address, ssl_port=None):
        self.host, _, port = (address or "").rpartition(":")
        self.port = int(port) if port else None
        self.ssl_port = ssl_port

    def start(self, ctx):
        ctx.tmp_root.chmod(0o755)
        if not services.tcp_open(self.host, self.port):
            raise RuntimeError(f"nothing listens on {self.host}:{self.port}")

    def env(self):
        return {"SLT_TEST_DIR_SHARED": "1"}

    def acquire(self):
        return 0, 0

    def release(self, i):
        pass

    def server(self, i):
        return self

    def watch(self, r, i):
        return self

    def pid(self):
        return None

    def crashed_during(self, i, generation):
        return None


class PostgresReference(ExternalServer):
    stateful = True

    def __init__(self, ctx):
        super().__init__(None)
        self.services = services.ServiceSet(log_dir=ctx.out_dir / "services", prefix="pgref")

    def start(self, ctx):
        image = f"serenedb/serenedb-test-postgres:{services.fixture_image_tag('postgres')}"
        ctx.tmp_root.chmod(0o755)
        name = self.services.run("pgref", image, "-c", "max_connections=900", ports=(5432,), extra=[
            "--tmpfs", "/var/lib/postgresql:size=16g",
            "-v", f"{ctx.tmp_root}:{ctx.tmp_root}",
            "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=postgres"])
        services.wait_for("PostgreSQL", lambda: subprocess.run(
            ["docker", "exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
            capture_output=True).returncode == 0, 120)
        host, port = self.services.address(name, 5432)
        self.host, self.port = host, int(port)

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


def _runner_args(ctx, args, rel, engine, server, env, part=None, label="serenedb"):
    argv = [runner_binary(ctx), rel, "--host", server.host, "--port", str(server.port),
            "--engine", engine, "--jobs", "1", "--label", label, "--shutdown-timeout", "60"]
    if server.ssl_port:
        argv += ["--ssl-port", str(server.ssl_port)]
    if env.get("PGSOCKETDIR"):
        argv += ["--label", "pgsocket"]
    if CORPUS in rel:
        argv += ["--show-column-names", "false", "--flat-values", "true", "--hash-threshold", "8"]
    if part:
        argv += ["--part", f"{part[0]}/{part[1]}"]
    if args.override:
        argv += ["--override"]
    return argv


def corpus_parts(ctx, args, cost):
    if args.override:
        return 1
    return max(1, min(MAX_PARTS, round(cost / (PART_SECONDS * ctx.config.slowdown))))


def default_seconds(path, rel):
    if CORPUS in rel:
        return max(2.0, path.stat().st_size / 1e6 * CORPUS_SECONDS_PER_MB)
    return 30.0 if rel.endswith(".test_slow") else 2.0


def check_out_corpus(ctx):
    subprocess.run(["git", "-C", str(ctx.root), "submodule", "update", "--init", "--checkout",
                    "--depth=1", "tests/sqllogic/any/pg/sqlite"], check=True)
    subprocess.run(["git", "-C", str(sqllogic_dir(ctx) / "any" / "pg" / "sqlite"), "checkout-index",
                    "--all", "--quiet"], check=True)


def engines(args):
    return ENGINES[:1] if args.override else ENGINES


def sqllogic_units(ctx, args, skips):
    base = sqllogic_dir(ctx)
    generated_tests(ctx, "write")
    if args.corpus:
        check_out_corpus(ctx)
    units = []
    pool = ("servers",) if args.server else ()
    shared = () if args.server else ("servers",)
    for path in find_tests(base / "sdb", args.fast):
        rel = str(path.relative_to(base))
        if CORPUS in rel and not args.corpus:
            continue
        try:
            fixtures = service_fixture_names(path, args.biglake)
        except ValueError as e:
            raise SystemExit(str(e))
        if args.biglake and path.name.endswith("_fixture_iceberg.test_slow"):
            continue
        if args.biglake and "svc:biglake" not in fixtures:
            continue
        for engine in engines(args):
            uid = f"sqllogic:{engine}:{rel}"
            skip = skipped(skips, uid)
            if skip:
                ctx.skips.append((uid, skip))
                continue
            cost, timeout = plan(ctx, uid, default_seconds(path, rel))
            if CORPUS not in rel:
                inference = [f for f in fixtures if f in INFERENCE_CPUS]
                cpus = 1 + sum(INFERENCE_CPUS[f] for f in inference)
                if inference:
                    timeout = max(timeout, INFERENCE_TIMEOUT)
                units.append(Unit(uid, "sqllogic", _sqllogic_run(ctx, args, rel, engine), cpus=cpus,
                                  mem_gb=0.3, fixtures=pool + fixtures, shared=shared, cost=cost,
                                  timeout=timeout, group=f"sqllogic:{rel}"))
                continue
            parts = corpus_parts(ctx, args, cost)
            for k in range(1, parts + 1):
                part = (k, parts) if parts > 1 else None
                units.append(Unit(f"{uid}#{k}/{parts}" if part else uid, "sqllogic",
                                  _sqllogic_run(ctx, args, rel, engine, part), cpus=1, mem_gb=0.3,
                                  fixtures=pool + fixtures, shared=shared, cost=cost / parts,
                                  timeout=max(ctx.timeout_floor, timeout / parts)))
    return units


def postgres_units(ctx, args, skips):
    base = sqllogic_dir(ctx)
    generated_tests(ctx, "write")
    if args.corpus:
        check_out_corpus(ctx)
    units = []
    for path in find_tests(base / "pg", args.fast):
        rel = str(path.relative_to(base))
        if CORPUS in rel and not args.corpus:
            continue
        for engine in engines(args):
            uid = f"postgres:{engine}:{rel}"
            skip = skipped(skips, uid)
            if skip:
                ctx.skips.append((uid, skip))
                continue
            cost, timeout = plan(ctx, uid, default_seconds(path, rel))
            units.append(Unit(uid, "postgres", _shared_run(ctx, args, rel, engine, "pgref", "postgres"),
                              cpus=1, mem_gb=0.3, fixtures=("pgref",), cost=cost, timeout=timeout,
                              group=f"postgres:{rel}"))
    return units


def _private_run(ctx, args, rel, engine, part):
    def run(r):
        server = Server(ctx, r.dir / "server", listen_ssl=True, profile_dir=r.profile_dir,
                        extra_env={"POSTGRES_PASSWORD": "postgres"}).start()
        r.stack_pids = lambda: (server.pid(),)
        try:
            res = r.exec(_runner_args(ctx, args, rel, engine, server, r.env, part),
                         cwd=sqllogic_dir(ctx))
        finally:
            stop = server.stop()
        out = r.outcome(res, san_dirs=(server.san_dir,))
        if out.status in (STUCK, CRASHED):
            return out
        if stop and stop[0] == "stuck":
            return Outcome(STUCK, "serened did not stop on SIGTERM at the end of the test",
                           signature="serened stop stuck")
        if stop and stop[1] not in (0, None):
            sig = server.crash_signature()
            return Outcome(CRASHED, f"serened exited with {stop[1]}: {sig}", signature=sig)
        if out.status == FAILED and not out.signature:
            out.detail = _runner_failure(r.log) or out.detail
        return out

    return run


def _shared_run(ctx, args, rel, engine, fixture, label, part=None):
    def run(r):
        pool = r.fixture(fixture)
        i, generation = pool.acquire()
        try:
            server = pool.watch(r, i)
            res = r.exec(_runner_args(ctx, args, rel, engine, server, r.env, part, label),
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


def _sqllogic_run(ctx, args, rel, engine, part=None):
    private = _private_run(ctx, args, rel, engine, part)
    shared = _shared_run(ctx, args, rel, engine, "servers", "serenedb", part)

    def run(r):
        if r.alone and not args.server:
            return private(r)
        return shared(r)

    return run


def recovery_units(ctx, args, skips):
    base = sqllogic_dir(ctx)
    generated_tests(ctx, "write")
    units = []
    for path in find_tests(base / "recovery", args.fast):
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
        cost, timeout = plan(ctx, uid, 60.0 if rel.endswith(".test_slow") else 10.0)
        units.append(Unit(uid, "recovery", _recovery_run(ctx, args, rel), cpus=2, mem_gb=1.5,
                          fixtures=fixtures, cost=cost, timeout=timeout))
    return units


def _recovery_run(ctx, args, rel):
    def run(r):
        server = RestartingServer(ctx, r.dir / "server", profile_dir=r.profile_dir).start()
        r.stack_pids = lambda: (server.pid(),)
        env = {"SDB_DATADIR": str(server.datadir), "RETRY_ATTEMPTS": "60",
               "BACKOFF_DURATION": "500ms"}
        try:
            res = r.exec(_runner_args(ctx, args, rel, "pg-wire-simple", server.server, r.env),
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
    if args.server:
        factories["servers"] = lambda c: ExternalServer(args.server, args.server_ssl_port)
        factories["pgref"] = lambda c: ExternalServer(args.server, args.server_ssl_port)
        return factories
    factories["servers"] = lambda c: ServerPool(c, count=args.servers or max(
        1, c.jobs // c.config.units_per_server), threads=0,
        extra_env={"POSTGRES_PASSWORD": "postgres"})
    factories["pgref"] = PostgresReference
    return factories
