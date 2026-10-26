import importlib.util
import os
import random
import signal
import subprocess
import time

from serened import pg_startup_reachable

from .. import proc, reports
from ..scheduler import CRASHED, FAILED, PASSED, STUCK, Fixture, Outcome, Unit
from ..servers import Server
from . import gtest, plan, skipped, sqllogic

CANCEL_FILES = 4
CANCEL_ROUND_FILES = 32
CANCEL_FILE_SECONDS = 0.5
CANCEL_STOP_SECONDS = 120.0
ACTIVE_QUERIES = ("SELECT count(*) FROM pg_stat_activity "
                  "WHERE state = 'active' AND pid NOT IN (0, pg_backend_pid())")


def _psql(server, sql, timeout=60):
    if not server.alive() or not pg_startup_reachable("127.0.0.1", server.port, timeout=10):
        return None
    try:
        res = subprocess.run(["psql", f"host=127.0.0.1 port={server.port} user=postgres dbname=postgres "
                              "connect_timeout=10", "-tAc", sql], capture_output=True,
                             text=True, timeout=timeout, env={**os.environ, "PGPASSWORD": "postgres"})
    except subprocess.TimeoutExpired:
        return None
    return res.stdout.strip() if res.returncode == 0 else None


def _responsive(server, timeout=60):
    return _psql(server, "SELECT 1", timeout) == "1"


def sqlsmith_units(ctx, args, skips):
    uid = "sqlsmith"
    if skipped(skips, uid):
        return []
    seconds = args.fuzz_seconds
    timeout = seconds + max(ctx.timeout_floor, 300 * ctx.config.slowdown)
    return [Unit(uid, "fuzz", _sqlsmith_run(ctx, seconds), cpus=4, mem_gb=6.0, cost=seconds,
                 timeout=timeout, label=False)]


def _sqlsmith_run(ctx, seconds):
    def run(r):
        server = Server(ctx, r.dir / "server", profile_dir=r.profile_dir).start()
        r.stack_pids = lambda: (server.pid(),)
        env = {"SDB_DRV_HOST": "127.0.0.1", "SDB_DRV_PORT": str(server.port),
               "SDB_DRV_DATABASE": "postgres", "SDB_DRV_USER": "postgres",
               "SDB_FUZZ_SECONDS": str(seconds), "SDB_FUZZ_PARALLEL": "4",
               "SDB_FUZZ_QUERIES": "100000000", "SDB_SERVER_LOG": str(server.log),
               "SDB_DRV_JUNIT": str(r.dir / "junit")}
        try:
            res = r.exec([ctx.root / "tests" / "drivers" / "sqlsmith" / "run.sh"], env=env)
            if not res.stuck and server.alive() and not _responsive(server):
                stacks = proc.dump_all_stacks([server.pid()], r.dir, prefix="server-stacks")
                return Outcome(STUCK, "serened stopped answering after the fuzz run; stacks saved",
                               signature="serened unresponsive after sqlsmith",
                               artifacts=[str(s) for s in stacks])
        finally:
            stop = server.stop()
        out = r.outcome(res, san_dirs=(server.san_dir,))
        if out.status in (STUCK, CRASHED):
            return out
        if stop and stop[0] == "exit" and stop[1] not in (0, None):
            sig = server.crash_signature()
            return Outcome(CRASHED, f"serened exited with {stop[1]}: {sig}", signature=sig)
        if out.status == FAILED:
            text = r.log.read_text(errors="replace") if r.log.exists() else ""
            fails = [ln for ln in text.splitlines() if "FAIL:" in ln]
            out.detail = fails[0][:300] if fails else out.detail
        return out

    return run


def _cancellable_files(ctx):
    base = sqllogic.sqllogic_dir(ctx)
    files = []
    for path in sqllogic.find_tests(base / "sdb", True):
        rel = str(path.relative_to(base))
        if sqllogic.CORPUS in rel or path.name.endswith(".gen.test"):
            continue
        try:
            if sqllogic.service_fixture_names(path, False):
                continue
        except ValueError:
            continue
        files.append(rel)
    return files


def cancellation_units(ctx, args, skips):
    files = _cancellable_files(ctx)
    if not files:
        return []
    seconds = args.fuzz_seconds
    timeout = seconds + max(ctx.timeout_floor, 2 * CANCEL_STOP_SECONDS * ctx.config.slowdown)
    units = []
    for engine in sqllogic.ENGINES:
        uid = f"cancellation:{engine}"
        if skipped(skips, uid):
            continue
        units.append(Unit(uid, "cancellation", _cancellation_run(ctx, files, engine, seconds),
                          cpus=CANCEL_FILES, mem_gb=4.0, cost=seconds, timeout=timeout,
                          label=False))
    return units


def _cancellation_run(ctx, files, engine, seconds):
    def run(r):
        seed = int(os.environ.get("SDB_CANCEL_SEED") or random.randrange(1 << 32))
        rng = random.Random(seed)
        slowdown = ctx.config.slowdown
        server = Server(ctx, r.dir / "server", profile_dir=r.profile_dir,
                        extra_env={"POSTGRES_PASSWORD": "postgres"}).start()
        r.stack_pids = lambda: (server.pid(),)
        end = min(time.monotonic() + seconds,
                  r.deadline - 2 * CANCEL_STOP_SECONDS * slowdown)
        rounds = 0
        try:
            with open(r.log, "a") as log:
                log.write(f"seed {seed} (SDB_CANCEL_SEED reruns these files and delays)\n")
            while time.monotonic() < end and not r.cancelled() and server.alive():
                picked = rng.sample(files, min(CANCEL_ROUND_FILES, len(files)))
                expected = sum(ctx.timing.expect(f"sqllogic:{engine}:{rel}")
                               or CANCEL_FILE_SECONDS * slowdown for rel in picked)
                delay = rng.uniform(0.05, max(0.1, expected / CANCEL_FILES))
                with open(r.log, "a") as log:
                    log.write(f"round {rounds}: SIGINT after {delay:.2f}s: {' '.join(picked)}\n")
                argv = [sqllogic.runner_binary(ctx), *picked, "--host", server.host,
                        "--port", str(server.port), "--engine", engine,
                        "--jobs", str(CANCEL_FILES), "--label", "serenedb",
                        "--shutdown-timeout", "60"]
                runner = proc.spawn(argv, env=r.env, cwd=sqllogic.sqllogic_dir(ctx), log_path=r.log)
                try:
                    runner.wait(timeout=delay)
                except subprocess.TimeoutExpired:
                    runner.send_signal(signal.SIGINT)
                try:
                    runner.wait(timeout=CANCEL_STOP_SECONDS * slowdown)
                except subprocess.TimeoutExpired:
                    stacks = proc.dump_all_stacks([p for p in (runner.pid, server.pid()) if p], r.dir)
                    return Outcome(STUCK, f"round {rounds}: the runner did not stop within "
                                   f"{CANCEL_STOP_SECONDS * slowdown:.0f}s of SIGINT; stacks saved",
                                   signature="cancellation: runner stuck after SIGINT",
                                   artifacts=[str(s) for s in stacks])
                finally:
                    proc.kill_session(runner.pid)
                    runner.wait()
                if server.alive() and not _responsive(server):
                    stacks = proc.dump_all_stacks([server.pid()], r.dir, prefix="server-stacks")
                    return Outcome(STUCK, f"serened stopped answering after round {rounds}; "
                                   "stacks saved", signature="serened unresponsive after cancellation",
                                   artifacts=[str(s) for s in stacks])
                rounds += 1
            drain_end = time.monotonic() + CANCEL_STOP_SECONDS * slowdown
            active = None
            while server.alive() and time.monotonic() < drain_end:
                active = _psql(server, ACTIVE_QUERIES)
                if active == "0":
                    break
                time.sleep(1.0)
            if server.alive() and active != "0":
                stacks = proc.dump_all_stacks([server.pid()], r.dir, prefix="server-stacks")
                return Outcome(FAILED, f"{active or 'unknown'} queries still running "
                               f"{CANCEL_STOP_SECONDS * slowdown:.0f}s after their sessions ended; "
                               "stacks saved", signature="cancellation: queries outlive their sessions",
                               artifacts=[str(s) for s in stacks])
        finally:
            stop = server.stop()
        sanitizer = reports.scan_sanitizer_dir(server.san_dir)
        if stop and stop[0] == "stuck":
            return Outcome(STUCK, "serened did not stop on SIGTERM after the cancellation rounds",
                           signature="serened stop stuck")
        if stop and stop[1] not in (0, None):
            sig = server.crash_signature()
            return Outcome(CRASHED, f"serened exited with {stop[1]} after {rounds} rounds: {sig}",
                           signature=sig)
        if sanitizer:
            return Outcome(FAILED, sanitizer[0].signature, signature=sanitizer[0].signature)
        return Outcome(PASSED, f"{rounds} rounds")

    return run


def _stress_seconds(ctx, profile):
    path = ctx.root / "tests" / "drivers" / "stress" / "config.py"
    spec = importlib.util.spec_from_file_location("sdb_stress_config", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    found = module.PROFILES.get(profile)
    return float(found.seconds) if found else 600.0


def stress_units(ctx, args, skips):
    uid = f"stress:{args.stress_profile}"
    if skipped(skips, uid):
        return []
    seconds = _stress_seconds(ctx, args.stress_profile)
    cost, timeout = plan(ctx, uid, seconds + 30.0)
    return [Unit(uid, "stress", _stress_run(ctx, args.stress_profile, args.biglake), cpus=8,
                 mem_gb=12.0, cost=cost, timeout=max(timeout, 3 * seconds + 600), label=False)]


def stress_harness_units(ctx, args, skips):
    uid = "stress:harness"
    if skipped(skips, uid):
        return []
    cost, timeout = plan(ctx, uid, 5.0)

    def run(r):
        res = r.exec(["python3", "-m", "pytest", "-q", "-p", "no:cacheprovider",
                      f"--junitxml={r.dir / 'junit' / 'harness.xml'}"],
                     cwd=ctx.root / "tests" / "drivers" / "stress")
        return r.outcome(res)

    return [Unit(uid, "stress", run, cpus=1, mem_gb=1.0, cost=cost, timeout=timeout)]


def _stress_run(ctx, profile, biglake):
    def run(r):
        res = r.exec([ctx.root / "tests" / "drivers" / "stress" / "run.sh", "--profile", profile,
                      "--outdir", str(r.dir / "stress"), "--junit", str(r.dir / "junit")],
                     env={"SERENED": str(ctx.build.bin("serened")),
                          "ICEBERG_BACKEND": "biglake" if biglake else "local"})
        out = r.outcome(res)
        summary = r.dir / "stress" / "summary.txt"
        if out.status == FAILED and summary.is_file():
            lines = [ln for ln in summary.read_text(errors="replace").splitlines() if ln.strip()]
            out.detail = " | ".join(lines[:3])[:300]
        return out

    return run


class CorpusFixture(Fixture):
    name = "iresearch-corpus"

    def start(self, ctx):
        res = subprocess.run(["bash", str(ctx.root / "scripts" / "ci" / "steps" / "iresearch-load-fetch-corpus.bash")],
                             capture_output=True, text=True, env=ctx.base_env, timeout=3600)
        if res.returncode != 0:
            raise RuntimeError(res.stderr.strip()[-500:] or "corpus fetch failed")
        self.path = res.stdout.strip().splitlines()[-1]

    def env(self):
        return {"CORPUS_PATH": self.path}


def iresearch_load_units(ctx, args, skips):
    uid = "iresearch-load:LoadTest"
    if not ctx.build.gtests or skipped(skips, uid):
        return []
    binary = gtest.required_binary(ctx.build.bin("iresearch-load-tests"), "iresearch-load-tests")
    cost, timeout = plan(ctx, uid, 600.0)

    def run(r):
        res = r.exec([binary, "--gtest_filter=LoadTest*",
                      f"--ires_resource_dir={ctx.root / 'resources' / 'tests' / 'iresearch'}",
                      f"--gtest_output=xml:{r.dir / 'report.xml'}"],
                     env={"MALLOC_ARENA_MAX": "1"}, cwd=ctx.build.dir / "bin")
        return r.outcome(res)

    return [Unit(uid, "iresearch-load", run, cpus=4, mem_gb=16.0, fixtures=("iresearch-corpus",),
                 cost=cost, timeout=timeout)]


def fixture_factories(ctx, args):
    return {"iresearch-corpus": lambda c: CorpusFixture()}
