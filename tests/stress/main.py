import argparse
import os
import pathlib
import random
import shlex
import tempfile
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
for _p in (HERE, os.path.join(REPO, "tests", "harness", "python")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import psycopg

import shutil

import capture
import chaos as chaos_mod
import config
import coverage as coverage_mod
import faults as faults_mod
import journal as journal_mod
import junit
import oracle
import quarantine as quarantine_mod
import quiesce
import snapshot as snapshot_mod
from serened import Serened
from procutil import raise_open_files, reap_orphans
from watchdog import ALIVE, WEDGED, Watchdog
from worker import Worker

ORPHAN_PATTERNS = ("sdbstress-",)


def base36(n):
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = ""
    n = abs(int(n)) or 0
    while True:
        out = chars[n % 36] + out
        n //= 36
        if not n:
            break
    return out[-4:].rjust(4, "0")


def ready_via_sql(host, port):
    try:
        with psycopg.connect(
            f"host={host} port={port} user=postgres dbname=postgres connect_timeout=5"
        ) as c:
            with c.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False


def preflight(dsn):
    info = {}
    with psycopg.connect(dsn) as c:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("SELECT version()")
            info["server_version"] = cur.fetchone()[0][:120]
            try:
                cur.execute("RESET sdb_faults")
                info["fault_injection"] = True
            except Exception:
                info["fault_injection"] = False
            try:
                cur.execute("SELECT count(*) FROM sdb_catalog_sets()")
                info["catalog_sets_rows"] = cur.fetchone()[0]
            except Exception as exc:
                info["catalog_sets_rows"] = f"unavailable: {exc}"[:120]
    info["psycopg"] = psycopg.__version__
    info["libpq"] = psycopg.pq.version()
    return info


def main(argv=None):
    ap = argparse.ArgumentParser(prog="tests/stress")
    ap.add_argument("--profile", default="smoke")
    ap.add_argument("--scenario")
    ap.add_argument("--seconds", type=int)
    ap.add_argument("--workers", type=int)
    ap.add_argument("--seed", type=int)
    ap.add_argument("--binary", default=os.environ.get(
        "SERENED", os.path.join(REPO, "build", "bin", "serened")))
    ap.add_argument("--outdir", default=os.path.join(REPO, "out", "stress"))
    ap.add_argument("--junit")
    ap.add_argument("--restarts", type=int)
    ap.add_argument("--parks", type=int)
    ap.add_argument("--cancels", type=int)
    ap.add_argument("--compaction-windows", type=int, dest="compaction_windows")
    ap.add_argument("--data-domain-crashes", type=int, dest="data_domain_crashes")
    ap.add_argument("--slow-windows", type=int, dest="slow_windows")
    ap.add_argument("--graceful-restarts", type=int, dest="graceful_restarts")
    ap.add_argument("--keep-datadir", action="store_true")
    args = ap.parse_args(argv)

    profile = config.resolve(args.profile, scenario=args.scenario,
                             seconds=args.seconds, workers=args.workers,
                             restarts=args.restarts, parks=args.parks,
                             cancels=args.cancels,
                             compaction_windows=args.compaction_windows,
                             data_domain_crashes=args.data_domain_crashes,
                             slow_windows=args.slow_windows,
                             graceful_restarts=args.graceful_restarts)
    seed = args.seed if args.seed is not None else random.SystemRandom().randrange(1 << 30)
    run_tag = base36(seed)
    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if not os.access(args.binary, os.X_OK):
        print(f"[stress] serened not found at {args.binary}", file=sys.stderr)
        return 1

    fixtures = config.iceberg_fixtures(REPO)
    if profile.scenario in config.SCENARIOS_NEEDING_ICEBERG and not fixtures:
        print("[stress] scenario needs the iceberg fixture; generate it with "
              "scripts/ensure_iceberg_fixture.sh (needs docker) -- skipping",
              file=sys.stderr)
        return 0

    raise_open_files(16384)
    reaped = reap_orphans(ORPHAN_PATTERNS)

    repro = (f"python3 tests/stress/main.py --profile {profile.name} "
             f"--scenario {profile.scenario} --seconds {profile.seconds} "
             f"--workers {profile.workers} --restarts {profile.restarts} "
             f"--seed {seed} "
             f"--binary {shlex.quote(args.binary)}")

    server = Serened(
        args.binary,
        datadir_root=profile.datadir_root,
        prefix="sdbstress-",
        log_path=str(outdir / "serened.log"),
        ready=ready_via_sql,
    )
    print(f"[stress] profile={profile.name} scenario={profile.scenario} "
          f"workers={profile.workers} seconds={profile.seconds} seed={seed} "
          f"run_tag={run_tag}")
    if reaped:
        print(f"[stress] reaped {len(reaped)} orphan process(es)")

    t_start = time.time()
    try:
        server.start()
    except Exception as exc:
        print(f"[stress] server did not start: {exc}", file=sys.stderr)
        server.stop()
        return 1

    dsn = server.dsn()
    info = preflight(dsn)
    print(f"[stress] iceberg_fixtures={len(fixtures)}")
    print(f"[stress] port={server.port} fault_injection={info['fault_injection']} "
          f"psycopg={info['psycopg']} libpq={info['libpq']} "
          f"catalog_sets_rows={info['catalog_sets_rows']}")

    jrnl = journal_mod.Journal(outdir / "ops.jsonl")
    findings = []
    findings_lock = threading.Lock()
    stop_event = threading.Event()
    pause_event = threading.Event()
    planned_downtime = threading.Event()

    defined = faults_mod.source_defined_faults(REPO) if info["fault_injection"] else {}
    broker = faults_mod.FaultBroker(
        lambda: psycopg.connect(dsn), defined=defined,
        enabled=bool(info["fault_injection"]))
    if info["fault_injection"]:
        try:
            broker.reset_all()
        except Exception as exc:
            print(f"[stress] warning: could not reset fault points: {exc}")

    attach_root = tempfile.mkdtemp(prefix="sdbstress-att-",
                                   dir=profile.datadir_root)
    env = {"iceberg_fixtures": fixtures, "host": "127.0.0.1",
           "port": server.port, "attach_root": attach_root}
    workers = [
        Worker(i, dsn, profile, run_tag, seed, jrnl, broker, stop_event,
               pause_event, findings, findings_lock, planned_downtime, env)
        for i in range(profile.workers)
    ]
    dog = Watchdog(dsn, profile, workers, server, stop_event)
    oid_registry = oracle.OidRegistry()

    post_crash = []

    def after_recovery(fault_name):
        # Oracle first: it is what collapses each ambiguous key onto the state
        # reality settled on. Resyncing before that would throw away every key
        # the crash left ambiguous, even the ones that survived.
        found = run_oracle(workers, pause_event, dsn, run_tag, server.datadir,
                           oid_registry, f"after-crash:{fault_name}",
                           abort_if=lambda: dog.verdict != ALIVE,
                           scan_artifacts=True)
        resynced = [w.state.resync_from(w.model) for w in workers]
        print(f"[stress] resynced worker state after {fault_name}: "
              f"{resynced} live keys per worker")
        for f in found:
            f["after_injected_crash"] = fault_name
        post_crash.extend(found)
        with findings_lock:
            findings.extend(found)

    chaos = chaos_mod.Chaos(server, broker, dog, profile,
                            __import__("rng").derive(seed, 9999),
                            findings, findings_lock, on_recovered=after_recovery,
                            planned_downtime=planned_downtime)
    def committed_now():
        return sum(w.status.committed for w in workers)

    want_crashes = profile.restarts if chaos.available() else 0
    want_parks = profile.parks if chaos.available() else 0
    want_graceful = profile.graceful_restarts
    want_cancels = profile.cancels
    want_compaction = profile.compaction_windows if chaos.available() else 0
    want_data_crashes = profile.data_domain_crashes if chaos.available() else 0
    want_slow = profile.slow_windows if chaos.available() else 0
    if profile.restarts and not chaos.available():
        print("[stress] restarts requested but fault injection is unavailable; "
              "skipping chaos")

    for w in workers:
        w.start()
    dog.start()

    quiesces = 0
    windows = []
    last_committed = 0
    last_mark = time.monotonic()
    deadline = time.monotonic() + profile.seconds
    next_quiesce = time.monotonic() + profile.quiesce_every
    def schedule(n, offset=0.0):
        if not n:
            return []
        step = profile.seconds / (n + 1.0)
        return [time.monotonic() + step * (i + 1) + offset for i in range(n)]

    crash_at = schedule(want_crashes)
    park_at = schedule(want_parks, offset=profile.seconds / 8.0)
    graceful_at = schedule(want_graceful, offset=profile.seconds / 5.0)
    cancel_at = schedule(want_cancels, offset=profile.seconds / 12.0)
    compact_at = schedule(want_compaction, offset=profile.seconds / 6.0)
    data_crash_at = schedule(want_data_crashes, offset=profile.seconds / 7.0)
    slow_at = schedule(want_slow, offset=profile.seconds / 9.0)
    try:
        while time.monotonic() < deadline:
            if dog.verdict != ALIVE:
                break
            time.sleep(0.2)
            if slow_at and time.monotonic() >= slow_at[0]:
                slow_at.pop(0)
                print("[stress] chaos: slowing the background search tasks")
                chaos.slow_background()
                continue
            if data_crash_at and time.monotonic() >= data_crash_at[0]:
                data_crash_at.pop(0)
                print(f"[stress] chaos: crashing in the DATA domain "
                      f"({chaos.result.crashes_attempted + 1})")
                chaos.crash_and_restart(family=chaos_mod.DATA_DOMAIN_FAULTS)
                next_quiesce = time.monotonic() + profile.quiesce_every
                continue
            if compact_at and time.monotonic() >= compact_at[0]:
                compact_at.pop(0)
                print(f"[stress] chaos: forcing catalog-log compaction under load "
                      f"({chaos.result.compaction_windows + 1}/{want_compaction})")
                chaos.compaction_pressure()
                continue
            if cancel_at and time.monotonic() >= cancel_at[0]:
                cancel_at.pop(0)
                chaos.cancel_an_inflight_op(workers)
                continue
            if park_at and time.monotonic() >= park_at[0]:
                park_at.pop(0)
                chaos.result.parks += 1
                print(f"[stress] chaos: parking an index build "
                      f"({chaos.result.parks}/{want_parks})")
                chaos.park_and_probe(committed_now,
                                     unrelated_ddl=lambda: unrelated_ddl_probe(dsn))
                continue
            if graceful_at and time.monotonic() >= graceful_at[0]:
                graceful_at.pop(0)
                print("[stress] chaos: graceful SIGTERM restart")
                chaos.graceful_restart()
                next_quiesce = time.monotonic() + profile.quiesce_every
                continue
            if crash_at and time.monotonic() >= crash_at[0]:
                crash_at.pop(0)
                print(f"[stress] chaos: injecting a catalog-window crash "
                      f"({chaos.result.crashes_attempted + 1}/{want_crashes})")
                chaos.crash_and_restart()
                next_quiesce = time.monotonic() + profile.quiesce_every
                continue
            if time.monotonic() >= next_quiesce:
                quiesces += 1
                now_committed = sum(w.status.committed for w in workers)
                now = time.monotonic()
                windows.append((now_committed - last_committed, now - last_mark))
                last_committed = now_committed
                last_mark = now
                new = run_oracle(workers, pause_event, dsn, run_tag,
                                 server.datadir, oid_registry, f"quiesce{quiesces}",
                                 abort_if=lambda: dog.verdict != ALIVE)
                with findings_lock:
                    findings.extend(new)
                next_quiesce = time.monotonic() + profile.quiesce_every
                if new:
                    break
    finally:
        stop_event.set()
        for w in workers:
            w.join(timeout=15)

    verdict = dog.verdict
    if verdict == ALIVE:
        quiesces += 1
        try:
            final = run_oracle(workers, pause_event, dsn, run_tag, server.datadir,
                               oid_registry, "final",
                               abort_if=lambda: dog.verdict != ALIVE)
            with findings_lock:
                findings.extend(final)
        except Exception as exc:
            with findings_lock:
                findings.append({
                    "kind": "oracle_failed", "key": None,
                    "detail": f"{type(exc).__name__}: {exc}"[:300],
                    "candidates": None, "observed": None})

    total_committed = sum(w.status.committed for w in workers)
    windows.append((max(total_committed - last_committed, 0),
                    max(time.monotonic() - last_mark, 0.0)))
    attempted = set()
    for w in workers:
        attempted |= set(w.op_kinds)
    labels_now = {}
    for w in workers:
        for k, v in w.labels.items():
            labels_now[k] = labels_now.get(k, 0) + v
    cov_data, cov_findings = coverage_mod.report(
        profile.scenario, attempted, windows, defined,
        chaos.result.faults_used, total_committed, quiesces,
        labels=labels_now,
        conflict_ceiling=config.conflict_ceiling_for(profile, profile.scenario),
        env=env,
        chaos_active=bool(want_crashes or want_parks or want_graceful
                          or want_cancels or want_compaction
                          or want_data_crashes or want_slow))

    with findings_lock:
        findings.extend(cov_findings)
        findings.extend(capture.scan_server_log(server))
        san_findings, san_totals = capture.scan_sanitizer_logs()
        findings.extend(san_findings)
        if verdict == WEDGED:
            findings.append({
                "kind": "server_wedged", "key": None,
                "detail": dog.detail or "server stopped answering fresh connections",
                "candidates": None, "observed": dog.wedged_at})
        elif verdict == "dead":
            findings.append({
                "kind": "server_exited", "key": None, "detail": dog.detail,
                "candidates": None, "observed": dog.wedged_at})

    try:
        entries = quarantine_mod.load()
    except quarantine_mod.QuarantineError as exc:
        entries = []
        with findings_lock:
            findings.append({"kind": "quarantine_unreadable", "key": None,
                             "detail": str(exc)[:300], "candidates": None,
                             "observed": None})
    with findings_lock:
        kept, quarantined = quarantine_mod.apply(
            list(findings), entries, profile.scenario, profile.workers)
        findings[:] = kept

    elapsed = time.time() - t_start
    committed = sum(w.status.committed for w in workers)
    retries = sum(w.status.retries for w in workers)
    labels = {}
    for w in workers:
        for k, v in w.labels.items():
            labels[k] = labels.get(k, 0) + v

    jrnl.close()
    tail = read_tail(outdir / "ops.jsonl", 12)

    meta = {
        "profile": profile.name, "scenario": profile.scenario, "seed": seed,
        "run_tag": run_tag, "workers": profile.workers, "seconds": profile.seconds,
        "binary": args.binary, "port": server.port, "datadir": server.datadir,
        "server_log": server.log.path if server.log else None,
        "journal": str(outdir / "ops.jsonl"), "repro_cmd": repro,
        "committed_ops": committed, "retries": retries, "quiesces": quiesces,
        "labels": labels, "elapsed_s": round(elapsed, 1),
        "build_config": info.get("server_version"),
        "fault_injection": info.get("fault_injection"),
        "chaos": (chaos.result.as_dict()
                  if (want_crashes or want_parks or want_graceful or want_cancels
                      or want_compaction or want_data_crashes or want_slow)
                  else None),
        "coverage": cov_data,
        "quarantined": quarantined,
        "quarantine_entries": [e.as_dict() for e in entries],
        "server_generations": server.generation,
        "findings_after_injected_crash": len(post_crash),
    }

    print(f"[stress] committed={committed} retries={retries} quiesces={quiesces} "
          f"verdict={verdict} findings={len(findings)} elapsed={elapsed:.1f}s")
    print(f"[stress] labels={labels}")
    if san_totals:
        print(f"[stress] sanitizer: {san_totals} "
              f"(from {len(capture.sanitizer_log_paths())} log_path file(s))")
    print(coverage_mod.render(cov_data))
    if quarantined:
        print(f"[stress] {len(quarantined)} finding(s) quarantined: "
              f"{sorted({q['quarantined_by'] for q in quarantined})}")
    if (want_crashes or want_parks or want_graceful or want_cancels
            or want_compaction or want_data_crashes or want_slow):
        cr = chaos.result
        print(f"[stress] chaos: crashes {cr.crashes_observed}/{cr.crashes_attempted} "
              f"observed, restarts_ok={cr.restarts_ok}, "
              f"generations={server.generation}, parks={cr.parks}, "
              f"cancels={cr.cancels}, "
              f"findings_after_crash={len(post_crash)}, faults={sorted(set(cr.faults_used))}")

    with findings_lock:
        final_findings = list(findings)
    capture.write_artifacts(outdir, meta, final_findings, dog, server, tail,
                            sanitizer_totals=san_totals)

    if final_findings or verdict != ALIVE:
        text = journal_mod.render_repro(outdir / "summary.txt", meta,
                                        final_findings, tail)
        print(text)
        if args.keep_datadir or verdict != ALIVE:
            try:
                kept = server.preserve_datadir(str(outdir / "datadirs"))
                print(f"[stress] datadir preserved at {kept}")
            except Exception:
                pass

    if args.junit:
        os.makedirs(args.junit, exist_ok=True)
        junit.write(os.path.join(args.junit, "tests-stress-junit.xml"),
                    f"{profile.name}_{profile.scenario}_w{profile.workers}",
                    elapsed, final_findings, verdict, repro)

    try:
        broker.close()
    except Exception:
        pass
    server.stop(keep_datadir=True)
    shutil.rmtree(attach_root, ignore_errors=True)

    if final_findings or verdict != ALIVE:
        print(f"[stress] FAIL verdict={verdict} findings={len(final_findings)}")
        return 1
    print("[stress] PASS")
    return 0


def unrelated_ddl_probe(dsn, timeout=20.0):
    name = f"park_probe_{int(time.monotonic() * 1000) % 1000000}"
    try:
        with psycopg.connect(dsn, connect_timeout=10) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"CREATE TABLE {name}(a INT)")
                cur.execute(f"DROP TABLE {name}")
        return True
    except Exception:
        return False


def run_oracle(workers, pause_event, dsn, run_tag, datadir, oid_registry, label,
               abort_if=None, scan_artifacts=False):
    drained, stuck = quiesce.drain(workers, pause_event, timeout=120.0,
                                   abort_if=abort_if)
    try:
        if not drained:
            if abort_if is not None and abort_if():
                return [{
                    "kind": "quiesce_abandoned_server_unhealthy", "key": None,
                    "detail": f"{label}: server unhealthy while draining; "
                              f"in flight: {stuck}",
                    "candidates": None, "observed": None}]
            return [{
                "kind": "quiesce_never_converged", "key": None,
                "detail": f"{label}: workers still in flight after 120s: {stuck}",
                "candidates": None, "observed": None}]
        models = [w.model for w in workers]
        row_keys = set()
        for m in models:
            row_keys |= set(m.row_bearing_keys())
        with psycopg.connect(dsn) as conn:
            conn.autocommit = True
            snap = snapshot_mod.take(conn, run_tag, datadir=datadir,
                                     row_keys=row_keys,
                                     scan_artifacts=scan_artifacts)
            return oracle.run_all(models, snap, conn, oid_registry)
    finally:
        quiesce.resume(pause_event)


def read_tail(path, n):
    try:
        import json
        lines = open(path).read().splitlines()[-n:]
        return [json.loads(x) for x in lines]
    except Exception:
        return []


if __name__ == "__main__":
    sys.exit(main())
