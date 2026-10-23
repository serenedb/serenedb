import importlib.util
import subprocess

from serened import pg_startup_reachable

from .. import proc
from ..scheduler import CRASHED, FAILED, PASSED, STUCK, Fixture, Outcome, Unit
from ..servers import Server
from . import plan, skipped


def _responsive(server, timeout=60):
    if not server.alive() or not pg_startup_reachable("127.0.0.1", server.port, timeout=10):
        return False
    try:
        res = subprocess.run(["psql", f"host=127.0.0.1 port={server.port} user=postgres dbname=postgres "
                              "connect_timeout=10", "-tAc", "SELECT 1"], capture_output=True,
                             text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False
    return res.returncode == 0 and res.stdout.strip() == "1"


def sqlsmith_units(ctx, args, skips):
    uid = "sqlsmith"
    if skipped(skips, uid):
        return []
    seconds = args.fuzz_seconds
    _, _ = plan(ctx, uid, seconds)
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
    return [Unit(uid, "stress", _stress_run(ctx, args.stress_profile), cpus=8, mem_gb=12.0,
                 cost=cost, timeout=max(timeout, 3 * seconds + 600), label=False)]


def _stress_run(ctx, profile):
    def run(r):
        res = r.exec([ctx.root / "tests" / "drivers" / "stress" / "run.sh", "--profile", profile,
                      "--outdir", str(r.dir / "stress"), "--junit", str(r.dir / "junit")],
                     env={"SERENED": str(ctx.build.bin("serened"))})
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
    binary = ctx.build.bin("iresearch-load-tests")
    uid = "iresearch-load:LoadTest"
    if not binary.is_file() or skipped(skips, uid):
        return []
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
