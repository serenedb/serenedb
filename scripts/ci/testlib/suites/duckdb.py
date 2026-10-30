import hashlib
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import time
import urllib.request
from contextlib import closing
from pathlib import Path

import services
from serened import reserve_port

from .. import reports, store
from ..scheduler import CRASHED, FAILED, PASSED, SKIPPED, STUCK, Fixture, Outcome, Unit
from . import BELOW_RESOLUTION, Skip, plan, skipped

SUITES = ("core", "cpp", "avro", "azure", "httpfs", "iceberg", "inet", "markdown",
          "postgres_scanner", "spatial", "interop")
SUITE_DIRS = {
    "core": "third_party/duckdb", "cpp": "third_party/duckdb", "avro": "third_party/duckdb_avro",
    "azure": "third_party/duckdb_azure", "httpfs": "third_party/duckdb_httpfs",
    "iceberg": "third_party/duckdb_iceberg", "inet": "third_party/duckdb_inet",
    "markdown": "third_party/duckdb_markdown", "postgres_scanner": "third_party/duckdb_postgres",
    "spatial": "third_party/duckdb_spatial",
}
CORE_SERIAL = (
    "test/sql/storage/concurrent_checkpoint_append_before_snapshot.test_slow",
    "test/sql/storage/concurrent_checkpoint_index_in_memory.test_slow",
    "test/sql/storage/concurrent_checkpoint_writes_mid_write.test_slow",
)
ICEBERG_CATALOG = "iceberg_catalog.db"
ICEBERG_SETUP = "test/sql/local/catalog_test_config_setup/"
ICEBERG_CUSTOM = (
    "test/sql/local/catalog_custom_setup/",
    "test/sql/local/partitioning/foldable_expression_filter.test",
    "test/sql/local/partitioning/in_filter.test",
)
ENV_FIXTURES = (
    ("S3_", "duckdb:s3"), ("DUCKDB_S3_", "duckdb:s3"), ("AWS_", "duckdb:s3"),
    ("POSTGRES_TEST_DATABASE_AVAILABLE", "duckdb:postgres"),
    ("FIXTURE_SERVER_AVAILABLE", "duckdb:iceberg"),
    ("DUCKDB_ICEBERG_HAVE_GENERATED_DATA", "duckdb:iceberg"),
    ("VENDED_CREDENTIAL_REFRESH_PROXY", "duckdb:iceberg"),
    ("AZURE_STORAGE_CONNECTION_STRING", "duckdb:azurite"), ("AZ_", "duckdb:azurite"),
    ("HTTP_PROXY_", "duckdb:proxies"), ("PYTHON_HTTP_SERVER_", "duckdb:http"),
    ("TEST_PERSISTENT_SECRETS_AVAILABLE", "duckdb:secrets"),
)
BATCH_SECONDS = 30.0
BATCH_MAX = 200
JOBS = 2
THREADS = {"SLURM_CPUS_ON_NODE": str(min(os.cpu_count() or 16, 16))}
_REQUIRE_ENV = re.compile(r"^require-env\s+([A-Z0-9_]+)", re.M)
_EVENT = re.compile(r"\[TEST_EVENT\] (\{.*\})\s*$")
_ENDED = re.compile(r"^(.+) \(pid (\d+)\) (was terminated by signal|exited with status) (\d+) "
                    r"(before|after) reporting its result$")
_TOOK = re.compile(r"^\[\d+/\d+\] \(\d+%\): (.+?) took ([0-9.]+)s")


def unittest_binary(ctx):
    return ctx.build.dir / "third_party" / "duckdb" / "test" / "unittest"


def core_dir(ctx):
    return ctx.root / SUITE_DIRS["core"]


def config_files(ctx, suite):
    slow = ctx.build.sanitize or ctx.build.coverage or ctx.build.build_type == "Debug"
    names = ["slow_build", suite] if slow else [suite]
    paths = (ctx.root / "tests" / "duckdb" / "config" / f"{name}.json" for name in names)
    return [p for p in paths if p.is_file()]


def config_args(ctx, suite):
    return [arg for path in config_files(ctx, suite) for arg in ("--test-config", str(path))]


def suite_filter(ctx, suite):
    if suite == "core":
        return "test/*"
    if suite == "cpp":
        return '~"*.test" ~"*.test_slow" ~"*.test_coverage",[.] ~"*.test" ~"*.test_slow" ~"*.test_coverage"'
    return f"{ctx.root / SUITE_DIRS[suite]}/test/*"


def list_tests(ctx, suite, spec=None):
    binary = unittest_binary(ctx)
    res = subprocess.run([str(binary), "--test-dir", str(core_dir(ctx)),
                          "--list-test-names-only", spec or suite_filter(ctx, suite)],
                         capture_output=True, text=True, timeout=600, env=ctx.base_env)
    names = [ln.strip() for ln in res.stdout.splitlines() if ln.strip()]
    if res.returncode != min(255, len(names)):
        raise SystemExit(f"{binary} listed {len(names)} test(s) of {spec or suite} and exited with "
                         f"{res.returncode}: {res.stderr.strip()[-2000:]}")
    return sorted(names)


def required_fixtures(ctx, path):
    full = Path(path) if os.path.isabs(path) else core_dir(ctx) / path
    try:
        text = full.read_text(errors="replace")
    except OSError:
        return ()
    found = []
    for var in _REQUIRE_ENV.findall(text):
        for prefix, fixture in ENV_FIXTURES:
            if var.startswith(prefix) and fixture not in found:
                found.append(fixture)
    return tuple(sorted(found))


def _escape(name):
    return '"' + re.sub(r'([,\\"])', r"\\\1", name) + '"'


def config_skips(ctx, suite):
    skips = {}
    for path in config_files(ctx, suite):
        for group in json.loads(path.read_text()).get("skip_tests", []):
            reason = group.get("reason", "")
            first = reason.split(". ")[0][:200]
            for p in group.get("paths", []):
                skips[p] = first
    return skips


def _relative(ctx, suite, name):
    if suite in SUITE_DIRS and os.path.isabs(name):
        base = str(ctx.root / SUITE_DIRS[suite]) + "/"
        if name.startswith(base):
            return name[len(base):]
    return name


def duckdb_units(ctx, args, skips):
    if not ctx.build.duckdb_unittests:
        return []
    if not unittest_binary(ctx).is_file():
        raise SystemExit(f"{unittest_binary(ctx)} is missing, though the build has "
                         "SDB_BUILD_DUCKDB_UNITTESTS on: build the unittest target")
    suites = args.duckdb_suites or SUITES
    units = []
    for suite in suites:
        if suite == "interop":
            uid = "duckdb:interop"
            if not skipped(skips, uid):
                cost, timeout = plan(ctx, uid, 120.0)
                units.append(Unit(uid, "duckdb", _interop_run(ctx), cpus=4, mem_gb=4.0,
                                  cost=cost, timeout=timeout))
            continue
        names = list_tests(ctx, suite)
        if not names:
            raise SystemExit(f"{unittest_binary(ctx)} lists no test of the DuckDB {suite} suite")
        if suite == "cpp":
            may_fail = frozenset(list_tests(ctx, suite, "[!mayfail]"))
            units += _batches(ctx, skips, suite, [(n, n) for n in names], cpp=True, may_fail=may_fail)
            continue
        excluded = config_skips(ctx, suite)
        groups = {}
        special = []
        for name in names:
            key = _relative(ctx, suite, name)
            reason = excluded.get(key)
            if reason is not None:
                ctx.skips.append((f"duckdb:{suite}:{key}", Skip(key, "divergence", reason)))
                continue
            if suite == "iceberg" and key.startswith((ICEBERG_SETUP, *ICEBERG_CUSTOM)):
                special.append((name, key))
                continue
            fixtures = required_fixtures(ctx, name)
            serial = None
            if suite == "postgres_scanner":
                serial = "duckdb:postgres"
            elif name in CORE_SERIAL:
                serial = "duckdb:core-serial"
            elif suite == "httpfs" and "duckdb:s3" in fixtures:
                serial = "duckdb:s3"
            groups.setdefault((fixtures, serial), []).append((name, key))
        for (fixtures, serial), tests in sorted(groups.items(), key=lambda kv: str(kv[0])):
            units += _batches(ctx, skips, suite, tests, serial=serial, fixtures=fixtures)
        if suite == "iceberg" and special:
            units += _iceberg_special(ctx, skips, special)
    return units


def _batches(ctx, skips, suite, tests, cpp=False, serial=None, fixtures=(), may_fail=frozenset()):
    planned = []
    for name, key in tests:
        uid = f"duckdb:{suite}:{key}"
        skip = skipped(skips, uid)
        if skip:
            ctx.skips.append((uid, skip))
            continue
        cost, timeout = plan(ctx, uid, 1.5 if cpp else 30.0 if name.endswith(".test_slow") else 3.0)
        planned.append(((name, key), cost, timeout))
    target = BATCH_SECONDS * ctx.config.slowdown
    per_unit = 1 if serial else BATCH_MAX
    units = []
    batch, batch_cost = [], 0.0

    def flush():
        nonlocal batch, batch_cost
        if not batch:
            return
        members = tuple(test for test, _, _ in batch)
        uid = f"duckdb:{suite}:batch:{members[0][1]}+{len(members) - 1}"
        jobs = 1 if serial else JOBS
        expected = max(batch_cost / jobs, max(c for _, c, _ in batch))
        timeout = max(ctx.timeout_floor, 5 * expected + 60 * ctx.config.slowdown,
                      max(t for _, _, t in batch))
        units.append(Unit(uid, "duckdb", _batch_run(ctx, suite, members, cpp, jobs, may_fail),
                          cpus=jobs, mem_gb=3.0 * jobs, fixtures=fixtures, group=serial,
                          cost=expected, timeout=timeout,
                          members=tuple(f"duckdb:{suite}:{key}" for _, key in members)))
        batch, batch_cost = [], 0.0

    for test, cost, timeout in planned:
        if batch and (batch_cost + cost > target * JOBS or len(batch) >= per_unit):
            flush()
        batch.append((test, cost, timeout))
        batch_cost += cost
    flush()
    return units


def _ended(line):
    m = _ENDED.match(line)
    if not m:
        return None
    signal = m.group(3).startswith("was")
    status = FAILED if not signal and m.group(5) == "after" else CRASHED
    detail = f"{'signal' if signal else 'exit'} {m.group(4)} {m.group(5)} reporting its result"
    return m.group(1), m.group(2), status, detail


def _parse_events(text, members, label):
    keys = dict(members)
    results = {}
    pids = {}
    took = {}
    began = set()
    for line in text.splitlines():
        m = _TOOK.match(line)
        if m:
            took[m.group(1)] = max(float(m.group(2)), BELOW_RESOLUTION)
        m = _EVENT.search(line)
        if m:
            try:
                ev = json.loads(m.group(1))
            except json.JSONDecodeError:
                continue
            if ev.get("event") == "begin":
                began.add(ev.get("name", ""))
            elif ev.get("event") == "end":
                status = {"ok": PASSED, "error": FAILED}.get(ev.get("status"), SKIPPED)
                detail = (ev.get("data") or "").strip().splitlines()
                results[ev.get("name", "")] = [status, detail[0][:300] if detail else ""]
            continue
        ended = _ended(line)
        if ended:
            name, pid, status, detail = ended
            results[name] = [status, detail]
            pids[pid] = keys.get(name, name)
    out = {}
    for name, key in members:
        if name in results:
            status, detail = results[name]
            out[f"{label}:{key}"] = (status, took.get(name, 0.0), detail)
    return out, pids, began


def _attribute(san_dir, pids, member_results, label):
    unattributed = []
    for path, found in reports.scan_sanitizer_files(san_dir):
        if not found:
            continue
        key = f"{label}:{pids.get(path.name.rsplit('.', 1)[-1])}"
        if key not in member_results:
            unattributed += found
            continue
        status, seconds, detail = member_results[key][:3]
        member_results[key] = (status, seconds, f"{found[0].signature} ({detail})", found[0].signature)
    return unattributed


def _halt_on_ubsan(r):
    opts = r.env.get("UBSAN_OPTIONS")
    return {"UBSAN_OPTIONS": f"{opts}:halt_on_error=1"} if opts else {}


def _batch_run(ctx, suite, members, cpp, jobs, may_fail=frozenset()):
    label = f"duckdb:{suite}"

    def run(r):
        names = [name for name, _ in members]
        argv = [unittest_binary(ctx), "--test-dir", core_dir(ctx), *config_args(ctx, suite)]
        if cpp:
            argv += ["--jobs", str(max(2, jobs)), ",".join(_escape(n) for n in names)]
        else:
            listing = r.dir / "tests.txt"
            listing.write_text("\n".join(names) + "\n")
            argv += ["--emit-test-events", "--jobs", str(max(2, jobs)), "-f", str(listing)]
        res = r.exec(argv, env={**THREADS, **_halt_on_ubsan(r)}, cwd=core_dir(ctx))
        text = r.log.read_text(errors="replace") if r.log.exists() else ""
        began = set()
        if cpp:
            member_results, pids = _parse_catch(text, members, label, may_fail)
        else:
            member_results, pids, began = _parse_events(text, members, label)
        if res.stuck:
            unfinished = [m for m in members if f"{label}:{m[1]}" not in member_results]
            launched = sorted(unfinished, key=lambda m: not m[0].endswith(".test_slow"))
            head = next((m for m in unfinished if m[0] in began), launched[0] if launched else None)
            for m in unfinished:
                member_results[f"{label}:{m[1]}"] = (
                    (STUCK, res.duration, f"no result after {res.duration:.0f}s; stacks saved")
                    if m == head else (SKIPPED, 0.0, "no result: the batch timed out"))
            return Outcome(STUCK, "batch did not finish; stacks saved", signature="stuck",
                           members=member_results)
        out = r.outcome(res)
        unattributed = _attribute(r.san_dir, pids, member_results, label)
        missing = [key for _, key in members if f"{label}:{key}" not in member_results]
        if missing:
            reason = out.signature or out.detail or f"exit {res.returncode}"
            for key in missing:
                member_results[f"{label}:{key}"] = (FAILED, 0.0, f"no result reported ({reason})")
        bad = [k for k, v in member_results.items() if v[0] in (FAILED, CRASHED, STUCK)]
        if bad:
            status = CRASHED if any(v[0] == CRASHED for v in member_results.values()) else FAILED
            return Outcome(status, f"{len(bad)} test(s) failed: {', '.join(bad[:3])}",
                           signature=out.signature, members=member_results)
        if unattributed:
            return Outcome(FAILED, unattributed[0].signature, signature=unattributed[0].signature,
                           members=member_results)
        if out.status == FAILED:
            return Outcome(FAILED, f"unittest exited {res.returncode} with every test passing",
                           members=member_results)
        return Outcome(out.status, members=member_results)

    return run


def _parse_catch(text, members, label, may_fail=frozenset()):
    wanted = {name for name, _ in members}
    rule = "-" * 79
    lines = text.splitlines()
    took = {}
    failed = {}
    ended_with = {}
    pids = {}
    current = None
    for i, line in enumerate(lines):
        m = _TOOK.match(line)
        if m and m.group(1) in wanted:
            took[m.group(1)] = max(float(m.group(2)), BELOW_RESOLUTION)
            continue
        ended = _ended(line)
        if ended and ended[0] in wanted:
            name, pid, status, detail = ended
            ended_with[name] = (status, detail)
            pids[pid] = name
            continue
        if line == rule and i + 1 < len(lines) and lines[i + 1].strip() in wanted:
            current = lines[i + 1].strip()
            continue
        if current and ": FAILED:" in line and current not in failed and current not in may_fail:
            detail = next((ln.strip() for ln in lines[i + 1:i + 6] if ln.strip()), "")
            failed[current] = f"{line.strip()} {detail}"[:300]
    out = {}
    for name, key in members:
        uid = f"{label}:{key}"
        if name in ended_with:
            status, detail = ended_with[name]
            out[uid] = (status, took.get(name, 0.0), detail)
        elif name in failed:
            out[uid] = (FAILED, took.get(name, 0.0), failed[name])
        elif name in took:
            out[uid] = (PASSED, took[name], "")
    return out, pids


def _iceberg_special(ctx, skips, tests):
    ice = ctx.root / SUITE_DIRS["iceberg"]
    setup = tuple(t for t in tests if t[1].startswith(ICEBERG_SETUP))
    custom = tuple(t for t in tests if t[1].startswith(ICEBERG_CUSTOM))
    steps = tuple(s for s in (
        ("setup", setup, ["--test-config", str(ice / "test/configs/fixture.json")], {}),
        ("custom", custom, [], {"CATALOG_TEST_CONFIG_SETUP": "fixture"}),
    ) if s[1])
    uid = "duckdb:iceberg:catalog"
    if not steps or skipped(skips, uid):
        return []
    members = tuple(f"duckdb:iceberg:{key}" for _, step, _, _ in steps for _, key in step)
    planned = [plan(ctx, member, 0.5) for member in members]
    cost = sum(c for c, _ in planned)
    timeout = max(ctx.timeout_floor, 5 * cost + 60 * ctx.config.slowdown, max(t for _, t in planned))
    return [Unit(uid, "duckdb", _ordered_run(ctx, steps), cpus=2, mem_gb=4.0,
                 fixtures=("duckdb:iceberg", "duckdb:proxies"), cost=cost, timeout=timeout,
                 members=members)]


def _ordered_run(ctx, steps):
    label = "duckdb:iceberg"

    def run(r):
        member_results = {}
        failure = None
        for key, members, extra, env in steps:
            listing = r.dir / f"{key}.txt"
            listing.write_text("\n".join(name for name, _ in members) + "\n")
            local_env = dict(THREADS, **env)
            if env.get("CATALOG_TEST_CONFIG_SETUP") and r.env.get("ICEBERG_HTTP_PROXY"):
                local_env["HTTP_PROXY_PUBLIC"] = r.env["ICEBERG_HTTP_PROXY"]
            argv = [unittest_binary(ctx), "--test-dir", core_dir(ctx), *config_args(ctx, "iceberg"),
                    "--order", "lex", *extra, "--emit-test-events", "-f", str(listing)]
            res = r.exec(argv, env=local_env, cwd=core_dir(ctx))
            text = r.log.read_text(errors="replace") if r.log.exists() else ""
            results, _, _ = _parse_events(text, members, label)
            member_results.update(results)
            if res.stuck:
                return Outcome(STUCK, f"{key} did not finish; stacks saved", signature="stuck",
                               members=member_results)
            out = r.outcome(res)
            if out.status != PASSED and failure is None:
                failure = out
        bad = [k for k, v in member_results.items() if v[0] in (FAILED, CRASHED)]
        if bad or failure is not None:
            status = failure.status if failure is not None and failure.status != FAILED else FAILED
            return Outcome(status,
                           f"{len(bad)} test(s) failed: {', '.join(bad[:3])}" if bad else failure.detail,
                           signature=failure.signature if failure is not None else "",
                           members=member_results)
        return Outcome(PASSED, members=member_results)

    return run


def _interop_run(ctx):
    def run(r):
        build_rel = os.path.relpath(ctx.build.dir, ctx.root)
        res = r.exec([ctx.root / "tests" / "duckdb" / "interop" / "run.sh"],
                     env={"BUILD_DIR": build_rel})
        return r.outcome(res)

    return run


class _DockerFixture(Fixture):
    def __init__(self, ctx):
        self.ctx = ctx
        self.services = services.ServiceSet(log_dir=ctx.out_dir / "services" / self.name.replace(":", "-"))
        self._env = {}
        self.procs = []

    def env(self):
        return dict(self._env)

    def stop(self):
        for p in self.procs:
            try:
                p.terminate()
                p.wait(10)
            except Exception:
                p.kill()
        self.services.stop()

    def unittest_stdin(self, script, env=None):
        merged = dict(self.ctx.base_env)
        merged.update(THREADS)
        merged.update(self._env)
        merged.update(env or {})
        log = self.ctx.out_dir / "services" / f"{self.name.replace(':', '-')}-provision.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        with open(log, "ab") as fh:
            res = subprocess.run([str(unittest_binary(self.ctx)), "--test-dir", str(core_dir(self.ctx)),
                                  "--stdin"], input=script.encode(), stdout=fh, stderr=subprocess.STDOUT,
                                 env=merged, cwd=core_dir(self.ctx), timeout=1800)
        if res.returncode != 0:
            raise RuntimeError(f"{self.name}: provisioning through unittest failed, see {log}")


class PostgresFixture(_DockerFixture):
    name = "duckdb:postgres"
    stateful = True

    def start(self, ctx):
        image = f"serenedb/serenedb-test-postgres:{services.fixture_image_tag('postgres')}"
        root = str(ctx.root)
        extra = ["-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=postgres",
                 "-e", "POSTGRES_INITDB_ARGS=--locale=C.UTF-8", "-e", "LANG=C.UTF-8",
                 "-v", f"{root}:{root}:ro"]
        if self.services.netns:
            name = self.services.run("pgscan", image, fixed_ports=(("127.0.0.1", 5432),), extra=extra)
            host, port = "127.0.0.1", "5432"
        else:
            name = self.services.run("pgscan", image, ports=(5432,), extra=extra)
            host, port = self.services.address(name, 5432)
        services.wait_for("postgres", lambda: subprocess.run(
            ["docker", "exec", name, "pg_isready", "-U", "postgres"], capture_output=True).returncode == 0, 60)
        self._env = {"PGHOST": host, "PGPORT": port, "PGUSER": "postgres", "PGDATABASE": "postgres"}
        psql = ["psql", "-h", host, "-p", port, "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-q"]
        env = dict(ctx.base_env, PGPASSWORD="")
        for _ in range(30):
            if subprocess.run(psql + ["-d", "postgres", "-c", "SELECT 1"], env=env,
                              capture_output=True).returncode == 0:
                break
            time.sleep(1)
        subprocess.run(psql + ["-d", "postgres", "-c", "CREATE EXTENSION IF NOT EXISTS postgis"],
                       env=env, check=True, capture_output=True)
        subprocess.run(psql + ["-d", "postgres", "-c", "CREATE DATABASE postgresscanner"],
                       env=env, check=True, capture_output=True)
        for fixture in ("all_pg_types.sql", "decimals.sql", "other.sql"):
            subprocess.run(psql + ["-d", "postgresscanner", "-f",
                                   str(ctx.root / SUITE_DIRS["postgres_scanner"] / "test" / fixture)],
                           env=env, check=True, capture_output=True)
        self.unittest_stdin((ctx.root / "tests" / "duckdb" / "provision_tpch.test").read_text())
        self._env.update(POSTGRES_TEST_DATABASE_AVAILABLE="1", HAS_POSTGIS="1")
        if ctx.build.sanitize:
            self._env["SANITIZER_BUILD"] = "1"


S3_HOSTS = ("duckdb-minio.com", "test-bucket.duckdb-minio.com", "test-bucket-2.duckdb-minio.com",
            "test-bucket-public.duckdb-minio.com")
SEAWEEDFS_IMAGE = "chrislusf/seaweedfs:4.47"
AWS_CLI_IMAGE = "amazon/aws-cli:2.36.44"
AWS_ENV = ["-e", "AWS_ACCESS_KEY_ID=admin", "-e", "AWS_SECRET_ACCESS_KEY=password", "-e", "AWS_REGION=us-east-1"]
ICEBERG_IDENTITY = {"name": "iceberg", "credentials": [{"accessKey": "admin", "secretKey": "password"}],
                    "actions": ["Admin", "Read", "Write", "List", "Tagging"]}


class SeaweedFixture(_DockerFixture):
    name = "duckdb:seaweedfs"
    stateful = True

    def start(self, ctx):
        try:
            ips = {socket.gethostbyname(h) for h in S3_HOSTS}
        except OSError:
            ips = set()
        if len(ips) != 1 or not next(iter(ips)).startswith("127.") or "127.0.0.1" in ips:
            raise RuntimeError("duckdb-minio.com and its bucket names must resolve to one loopback "
                               "address other than 127.0.0.1 (see tests/duckdb/README.md)")
        ip = ips.pop()
        identities = json.loads((ctx.root / SUITE_DIRS["httpfs"] / "scripts" /
                                 "seaweedfs_s3_config.json").read_text())
        identities["identities"].append(ICEBERG_IDENTITY)
        cfg = ctx.out_dir / "services" / "seaweedfs-s3.json"
        cfg.parent.mkdir(parents=True, exist_ok=True)
        cfg.write_text(json.dumps(identities))
        args = ["server", "-dir=/data", "-ip=127.0.0.1", "-ip.bind=0.0.0.0",
                "-master.volumeSizeLimitMB=1024", "-s3", "-s3.port=9000", "-s3.port.iceberg=0",
                "-s3.domainName=duckdb-minio.com", "-s3.config=/etc/seaweedfs/s3_config.json"]
        extra = ["-v", f"{cfg}:/etc/seaweedfs/s3_config.json:ro"]
        if self.services.netns:
            self.services.run("seaweedfs", SEAWEEDFS_IMAGE, *args, fixed_ports=(("127.0.0.1", 9000),),
                              extra=extra)
        else:
            self.services.pod("store", ((ip, 9000), ("127.0.0.1", 9000), ("127.0.0.1", 8181)))
            self.services.run("seaweedfs", SEAWEEDFS_IMAGE, *args, peer="store", extra=extra)
        services.wait_for("SeaweedFS", lambda: _http_any("http://127.0.0.1:9000"), 60)

    def pod(self):
        return self.services.pods.get("store")


class S3Fixture(_DockerFixture):
    name = "duckdb:s3"
    stateful = True
    requires = ("duckdb:seaweedfs",)

    def start(self, ctx):
        services.wait_for("S3 test server", lambda: _http_ok("http://duckdb-minio.com:9000"), 60)
        data = ctx.unit_tmp()
        core = core_dir(ctx)
        script = "\n".join([
            "require tpch", "", "require parquet", "", "statement ok", "CALL dbgen(sf=1);", "",
            "statement ok", f"COPY lineitem TO '{data}/presigned-url-lineitem.parquet' (FORMAT parquet);", "",
            "statement ok", f"ATTACH '{data}/lineitem_sf1.db' AS sf1;", "",
            "statement ok", "COPY FROM DATABASE memory TO sf1;", "", "statement ok", "DETACH sf1;", "",
            "statement ok", f"ATTACH '{data}/attach.db' AS db (STORAGE_VERSION 'latest');", "",
            "statement ok", "USE db;", "", "statement ok",
            "\n".join(ln for ln in (core / "test/sql/storage_version/generate_storage_version.sql")
                      .read_text().splitlines() if ln.strip()), "",
        ])
        self.unittest_stdin(script)
        res = subprocess.run(["python3", str(ctx.root / "tests" / "duckdb" / "provision_s3.py"),
                              "http://duckdb-minio.com:9000", str(core / "data"), str(data)],
                             capture_output=True, text=True, timeout=1200, env=ctx.base_env)
        if res.returncode != 0:
            raise RuntimeError(f"S3 provisioning failed: {res.stderr[-2000:]}")
        for line in res.stdout.splitlines():
            if "=" in line:
                k, _, v = line.partition("=")
                self._env[k.strip()] = v.strip()
        self._env.update(
            S3_TEST_SERVER_AVAILABLE="1", AWS_DEFAULT_REGION="eu-west-1",
            AWS_ACCESS_KEY_ID="minio_duckdb_user", AWS_SECRET_ACCESS_KEY="minio_duckdb_user_password",
            DUCKDB_S3_ENDPOINT="duckdb-minio.com:9000", DUCKDB_S3_USE_SSL="false",
            S3_ATTACH_DB="s3://test-bucket/presigned/attach.db")


class HttpFixture(_DockerFixture):
    name = "duckdb:http"
    reserved = None

    def stop(self):
        super().stop()
        if self.reserved is not None:
            self.reserved.close()

    def start(self, ctx):
        root = ctx.unit_tmp()
        self.reserved = reserve_port()
        port = self.reserved.getsockname()[1]
        (ctx.out_dir / "services").mkdir(parents=True, exist_ok=True)
        log = open(ctx.out_dir / "services" / "http-server.log", "ab")
        self.procs.append(subprocess.Popen(
            ["python3", "-m", "http.server", "--bind", "127.0.0.1", "--directory", str(root), str(port)],
            stdout=log, stderr=subprocess.STDOUT))
        url = f"http://127.0.0.1:{port}"
        services.wait_for("python http server", lambda: _http_ok(url), 30, 0.2)
        self._env = {"PYTHON_HTTP_SERVER_DIR": str(root), "PYTHON_HTTP_SERVER_URL": url}


class AzuriteFixture(_DockerFixture):
    name = "duckdb:azurite"
    stateful = True
    conn = ("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey="
            + services.AZURITE_KEY + ";BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")

    def start(self, ctx):
        self.services.run("azurite", services.AZURITE_IMAGE, "azurite-blob", "--blobHost", "0.0.0.0",
                          "--skipApiVersionCheck", fixed_ports=(("127.0.0.1", 10000),))
        services.wait_for("Azurite", lambda: _http_any("http://127.0.0.1:10000"), 60)
        res = subprocess.run(["python3", str(ctx.root / "tests" / "duckdb" / "provision_azurite.py"),
                              self.conn, str(ctx.root / SUITE_DIRS["azure"] / "data")],
                             capture_output=True, text=True, timeout=600, env=ctx.base_env)
        if res.returncode != 0:
            raise RuntimeError(f"Azurite provisioning failed: {res.stderr[-2000:]}")
        self._env = {"AZURE_STORAGE_CONNECTION_STRING": self.conn,
                     "AZURE_STORAGE_ACCOUNT": "devstoreaccount1",
                     "AZ_STORAGE_ACCOUNT": "devstoreaccount1", "AZ_DATA_DIR": "testing-private",
                     "AZ_TEMP_DIR": f"writes/run-{os.getpid()}"}


class ProxiesFixture(_DockerFixture):
    name = "duckdb:proxies"

    def start(self, ctx):
        if not shutil.which("squid"):
            raise RuntimeError("squid is not installed (the build image has it)")
        script = ctx.root / SUITE_DIRS["azure"] / "scripts" / "run_squid.sh"
        for port, extra in ((3128, []), (3129, ["--auth"])):
            d = ctx.out_dir / "services" / f"squid-{port}"
            shutil.rmtree(d, ignore_errors=True)
            d.mkdir(parents=True)
            log = open(d / "run.log", "ab")
            self.procs.append(subprocess.Popen([str(script), "--port", str(port), "--log_dir", "logs", *extra],
                                               cwd=d, stdout=log, stderr=subprocess.STDOUT))
            services.wait_for(f"squid {port}", lambda p=port: services.tcp_open("127.0.0.1", p), 30, 0.5)
        self._env = {"HTTP_PROXY_RUNNING": "1", "HTTP_PROXY_PUBLIC": "localhost:3128",
                     "HTTP_PROXY_PRIVATE": "localhost:3129", "HTTP_PROXY_PRIVATE_USERNAME": "john",
                     "HTTP_PROXY_PRIVATE_PASSWORD": "doe"}


class IcebergFixture(_DockerFixture):
    name = "duckdb:iceberg"
    stateful = True
    requires = ("duckdb:seaweedfs",)
    proxy_log = None

    def stop(self):
        super().stop()
        if self.proxy_log is not None:
            self.proxy_log.unlink(missing_ok=True)

    def start(self, ctx):
        self.services.join("store", ctx.fixtures["duckdb:seaweedfs"].pod())
        self.services.oneshot("s3-init", AWS_CLI_IMAGE, ["-c",
                              "until aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://warehouse; "
                              "do sleep 1; done"],
                              extra=[*AWS_ENV, "--entrypoint", "/bin/sh"], peer="store")
        ice = ctx.root / SUITE_DIRS["iceberg"]
        work = ctx.out_dir / "services" / "duckdb-iceberg-data"
        shutil.rmtree(work, ignore_errors=True)
        (work / "generated").mkdir(parents=True)
        (work / "catalog").mkdir()
        (work / "catalog").chmod(0o777)
        (work / "catalog" / ICEBERG_CATALOG).touch()
        (work / "catalog" / ICEBERG_CATALOG).chmod(0o666)
        fixed = os.environ.get("SDB_ICEBERG_DATA")
        key = f"artifacts/duckdb-iceberg/{_iceberg_data_key(ctx, fixed)}.tar.zst"
        cached = bool(fixed) and store.reachable() and self._restore(key, work)
        self.services.run("rest", services.ICEBERG_REST_IMAGE,
                          extra=[*AWS_ENV, "-e", "CATALOG_WAREHOUSE=s3://warehouse/",
                                 "-e", "CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO",
                                 "-e", "CATALOG_S3_ENDPOINT=http://127.0.0.1:9000",
                                 "-e", "CATALOG_S3_PATH__STYLE__ACCESS=true",
                                 "-e", f"CATALOG_URI=jdbc:sqlite:/catalog/{ICEBERG_CATALOG}",
                                 "-v", f"{work / 'catalog'}:/catalog"], peer="store")
        services.wait_for("Iceberg REST", lambda: _http_ok("http://127.0.0.1:8181/v1/config"), 90)
        if cached:
            _load_catalog(work)
        env = dict(ctx.base_env)
        if fixed:
            for link, target in ((Path(fixed), work / "generated"), (ice / "data" / "generated", Path(fixed))):
                if link.is_symlink() or link.is_file():
                    link.unlink()
                shutil.rmtree(link, ignore_errors=True)
                link.parent.mkdir(parents=True, exist_ok=True)
                link.symlink_to(target)
            env["ICEBERG_GENERATED_DIR"] = fixed
        if not cached:
            log = ctx.out_dir / "services" / "iceberg-data.log"
            with open(log, "ab") as fh:
                res = subprocess.run([str(ctx.root / "tests" / "duckdb" / "generate_iceberg_data.sh"),
                                      "fixture", "local"], stdout=fh, stderr=subprocess.STDOUT,
                                     env=env, timeout=3600)
            if res.returncode != 0:
                raise RuntimeError(f"Iceberg data generation failed, see {log}")
            if fixed and store.reachable():
                self._save(key, work)
        self._env = {"FIXTURE_SERVER_AVAILABLE": "1", "DUCKDB_ICEBERG_HAVE_GENERATED_DATA": "1"}
        if shutil.which("mitmdump"):
            conf = ctx.out_dir / "services" / "mitmproxy"
            self.proxy_log = ice / "mitmproxy.log"
            for port, extra, log in (
                (8878, [], self.proxy_log),
                (19133, ["-s", str(ice / "scripts" / "vended_credentials_refresh_proxy.py")],
                 ctx.out_dir / "services" / "vended-credentials-refresh-proxy.log"),
            ):
                out = open(log, "ab")
                self.procs.append(subprocess.Popen(
                    ["mitmdump", "--mode", f"regular@{port}", "--flow-detail", "2",
                     "--set", f"confdir={conf}", *extra], stdout=out, stderr=subprocess.STDOUT))
                services.wait_for(f"mitmdump {port}", lambda p=port: services.tcp_open("127.0.0.1", p), 30, 0.5)
            self._env.update(ICEBERG_HTTP_PROXY="localhost:8878",
                             VENDED_CREDENTIAL_REFRESH_PROXY="http://127.0.0.1:19133")

    def _sync(self, work, source, dest):
        self.services.oneshot("s3-sync", AWS_CLI_IMAGE,
                              ["--endpoint-url", "http://127.0.0.1:9000", "s3", "sync", "--only-show-errors",
                               source, dest],
                              extra=[*AWS_ENV, "-e", "HOME=/tmp", "--user", f"{os.getuid()}:{os.getgid()}",
                                     "-v", f"{work}:/data"], peer="store", timeout=900)

    def _restore(self, key, work):
        archive = work / "data.tar.zst"
        saved = work / "saved"
        try:
            if not store.get(key, archive):
                return False
            saved.mkdir()
            subprocess.run(["tar", "--zstd", "-xf", str(archive), "-C", str(saved)], check=True,
                           capture_output=True)
        except (OSError, RuntimeError, subprocess.CalledProcessError):
            shutil.rmtree(saved, ignore_errors=True)
            return False
        finally:
            archive.unlink(missing_ok=True)
        (saved / "generated").rename(work / "generated")
        self._sync(work, "/data/saved/warehouse", "s3://warehouse")
        return True

    def _save(self, key, work):
        self._sync(work, "s3://warehouse", "/data/warehouse")
        archive = work / "data.tar.zst"
        subprocess.run(["tar", "--zstd", "-cf", str(archive), "-C", str(work), "generated", "catalog",
                        "warehouse"], check=True, capture_output=True)
        try:
            store.put(key, archive)
        finally:
            archive.unlink(missing_ok=True)


def _load_catalog(work):
    with closing(sqlite3.connect(work / "catalog" / ICEBERG_CATALOG, timeout=30)) as db:
        db.execute("ATTACH ? AS saved", (str(work / "saved" / "catalog" / ICEBERG_CATALOG),))
        with db:
            for table in ("iceberg_tables", "iceberg_namespace_properties"):
                db.execute(f"INSERT INTO {table} SELECT * FROM saved.{table}")
    shutil.rmtree(work / "saved")


def _iceberg_data_key(ctx, fixed):
    digest = hashlib.sha256(f"{fixed}\0{services.ICEBERG_REST_IMAGE}\0{SEAWEEDFS_IMAGE}\0".encode())
    generators = ctx.root / SUITE_DIRS["iceberg"] / "scripts" / "data_generators"
    inputs = sorted(p for p in generators.rglob("*") if p.is_file() and "__pycache__" not in p.parts)
    inputs += [ctx.root / "tests" / "duckdb" / "generate_iceberg_data.sh",
               ctx.root / "scripts" / "ci" / "build-ubuntu.Dockerfile",
               ctx.root / "scripts" / "ci" / "test-data-requirements.txt"]
    for path in inputs:
        digest.update(f"{path.relative_to(ctx.root)}\0".encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()[:32]


class SecretsFixture(Fixture):
    name = "duckdb:secrets"

    def start(self, ctx):
        for d in (ctx.root / SUITE_DIRS["httpfs"] / "data" / "secrets",
                  ctx.root / SUITE_DIRS["core"] / "data" / "secrets"):
            for dirpath, dirnames, filenames in os.walk(d):
                for n in dirnames + filenames:
                    p = Path(dirpath) / n
                    p.chmod(p.stat().st_mode & ~0o077)
                Path(dirpath).chmod(Path(dirpath).stat().st_mode & ~0o077)

    def env(self):
        return {"TEST_PERSISTENT_SECRETS_AVAILABLE": "true"}


def _http_ok(url):
    try:
        with urllib.request.urlopen(url, timeout=3):
            return True
    except Exception:
        return False


def _http_any(url):
    try:
        with urllib.request.urlopen(url, timeout=3):
            return True
    except urllib.error.HTTPError:
        return True
    except Exception:
        return False


def fixture_factories(ctx, args):
    return {
        "duckdb:seaweedfs": SeaweedFixture,
        "duckdb:postgres": PostgresFixture, "duckdb:s3": S3Fixture, "duckdb:http": HttpFixture,
        "duckdb:azurite": AzuriteFixture, "duckdb:proxies": ProxiesFixture,
        "duckdb:iceberg": IcebergFixture, "duckdb:secrets": lambda c: SecretsFixture(),
    }


def clean_scratch(ctx):
    for rel in set(SUITE_DIRS.values()):
        shutil.rmtree(ctx.root / rel / "duckdb_unittest_tempdir", ignore_errors=True)
