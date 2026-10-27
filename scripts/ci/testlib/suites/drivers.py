import re

from ..scheduler import CRASHED, FAILED, PASSED, Outcome, Unit
from ..servers import Server, ServerPool
from . import plan, skipped

LANGS = ("python", "java", "js", "go", "rust", "php", "csharp", "c", "ruby", "r", "psql")
CPUS = {"java": 4, "csharp": 2, "js": 2, "python": 3}
OWN_SERVER = ("psql",)
HTTP_TOKEN = "drivers-ci-token"
TRUST_HBA = "host all all 0.0.0.0/0 trust\nhost all all ::0/0 trust\nlocal all all trust\n"


def driver_units(ctx, args, skips):
    units = []
    for lang in LANGS:
        uid = f"drivers:{lang}"
        skip = skipped(skips, uid)
        if skip:
            ctx.skips.append((uid, skip))
            continue
        cost, timeout = plan(ctx, uid, 120.0)
        own = lang in OWN_SERVER
        units.append(Unit(uid, "drivers", _driver_run(ctx, lang, own), cpus=CPUS.get(lang, 2),
                          mem_gb=2.0, fixtures=() if own else ("driver-servers",),
                          cost=cost, timeout=timeout))
    uid = "network:hba"
    if not skipped(skips, uid):
        cost, timeout = plan(ctx, uid, 30.0)
        units.append(Unit(uid, "network", _network_run(ctx), cpus=2, mem_gb=1.0, cost=cost,
                          timeout=timeout))
    return units


def _summary_line(text, lang):
    for line in text.splitlines():
        m = re.match(rf"^\s+{lang}\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)", line)
        if m:
            return f"{m.group(1)} tests, {m.group(2)} failed, {m.group(3)} errors"
    return ""


def _run_lang(ctx, r, lang, server):
    shared = ctx.out_dir / "drivers-shared"
    shared.mkdir(exist_ok=True)
    return r.exec([ctx.root / "tests" / "drivers" / "run.sh", "--lang", lang,
                   "--host", "127.0.0.1", "--port", str(server.port),
                   "--database", "postgres", "--user", "postgres",
                   "--junit", str(r.dir / "junit")],
                  env={"SDB_DRV_HTTP_PORT": str(server.http_port),
                       "SDB_DRV_HTTP_TOKEN": HTTP_TOKEN,
                       "SDB_DRV_SHARED_DIR": str(shared)})


def _driver_run(ctx, lang, own):
    def run(r):
        crash = None
        if own:
            server = Server(ctx, r.dir / "server", **SERVER).start()
            r.stack_pids = lambda: (server.pid(),)
            try:
                res = _run_lang(ctx, r, lang, server)
            finally:
                stop = server.stop()
            out = r.outcome(res, san_dirs=(server.san_dir,))
            if stop and stop[1] not in (0, None):
                crash = server.crash_signature()
        else:
            pool = r.fixture("driver-servers")
            i, generation = pool.acquire()
            try:
                res = _run_lang(ctx, r, lang, pool.watch(r, i))
            finally:
                pool.release(i)
            out = r.outcome(res)
            crash = pool.crashed_during(i, generation)
        if crash and out.status != PASSED:
            return Outcome(CRASHED, f"serened died during this unit: {crash}", signature=crash)
        summary = _summary_line(r.log.read_text(errors="replace") if r.log.exists() else "", lang)
        if out.status != FAILED:
            out.detail = summary
        elif not out.signature:
            out.detail = summary or out.detail
        return out

    return run


def _network_run(ctx):
    def run(r):
        res = r.exec([ctx.root / "tests" / "drivers" / "network" / "run.sh"],
                     env={"SERENED": str(ctx.build.bin("serened"))})
        return r.outcome(res)

    return run


SERVER = {"threads": 0, "listen_ssl": False, "http_apis": ("es", "mcp", "test", "otel"),
          "hba": TRUST_HBA, "extra_args": (f"--auth_bearer_token={HTTP_TOKEN}",)}


def fixture_factories(ctx, args):
    return {"driver-servers": lambda c: ServerPool(c, count=1, name="driver-servers", **SERVER)}
