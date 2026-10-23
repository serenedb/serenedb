import itertools
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path

from . import attribution, proc, reports

PASSED, FAILED, STUCK, CRASHED, SKIPPED, FLAKY = (
    "passed", "failed", "stuck", "crashed", "skipped", "flaky")
BAD = (FAILED, STUCK, CRASHED, FLAKY)
RERUN = (FAILED, STUCK, CRASHED)


@dataclass
class Outcome:
    status: str
    detail: str = ""
    signature: str = ""
    members: dict = field(default_factory=dict)
    artifacts: list = field(default_factory=list)


@dataclass
class Unit:
    id: str
    suite: str
    run: object
    cpus: int = 1
    mem_gb: float = 0.5
    fixtures: tuple = ()
    group: str | None = None
    cost: float = 1.0
    timeout: float = 600.0
    members: tuple = ()
    label: bool = True

    def safe_name(self):
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", self.id)[-180:]


@dataclass
class Result:
    unit: str
    suite: str
    status: str
    duration: float
    attempt: int
    started: float
    detail: str = ""
    signature: str = ""
    member_of: str = ""
    dir: str = ""
    wrapper: bool = False


class Fixture:
    name = ""
    stateful = False
    requires = ()

    def start(self, ctx):
        pass

    def stop(self):
        pass

    def env(self):
        return {}

    def health(self):
        return None


class Context:
    def __init__(self, *, root, build, config, out_dir, timing, jobs, mem_gb, verbose,
                 base_env, attribute=False):
        self.root = root
        self.build = build
        self.config = config
        self.out_dir = out_dir
        self.timing = timing
        self.jobs = jobs
        self.mem_gb = mem_gb
        self.verbose = verbose
        self.base_env = base_env
        self.attribute = attribute
        self.fixtures = {}
        self.findings = []
        self.skips = []
        self.timeout_floor = 180.0 if config.slowdown <= 2 else 900.0
        self.tmp_root = Path(tempfile.mkdtemp(prefix="sdbt-", dir="/tmp"))
        self._lock = threading.Lock()
        self._tmp_ids = itertools.count()

    def unit_tmp(self):
        with self._lock:
            n = next(self._tmp_ids)
        path = self.tmp_root / str(n)
        path.mkdir()
        return path

    def finding(self, result):
        with self._lock:
            self.findings.append(result)


class Run:
    def __init__(self, ctx, unit, attempt, cancelled):
        self.ctx = ctx
        self.unit = unit
        self.attempt = attempt
        name = unit.safe_name() + (f".{attempt}" if attempt else "")
        self.dir = ctx.out_dir / "units" / name
        if self.dir.exists():
            shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        self.tmp = ctx.unit_tmp()
        self.san_dir = self.dir / "san"
        self.san_dir.mkdir()
        self.log = self.dir / "log.txt"
        self.started = time.monotonic()
        self.deadline = self.started + unit.timeout
        self.cancelled = cancelled
        self.stack_pids = lambda: ()
        self.profile_dir = self.dir / "profiles" if ctx.attribute else ctx.out_dir / "profiles"
        env = dict(ctx.base_env)
        env["TMPDIR"] = str(self.tmp)
        env.update(ctx.build.sanitizer_env(self.san_dir))
        env.update(ctx.build.coverage_env(self.profile_dir))
        for name in unit.fixtures:
            env.update(ctx.fixtures[name].env())
        self.env = env

    def fixture(self, name):
        return self.ctx.fixtures[name]

    def remaining(self):
        return max(1.0, self.deadline - time.monotonic())

    def exec(self, argv, *, cwd=None, env=None, log=None, timeout=None):
        merged = dict(self.env)
        if env:
            merged.update(env)
        return proc.run(
            argv, env=merged, cwd=cwd or self.ctx.root, log_path=log or self.log,
            timeout=min(timeout or self.remaining(), self.remaining()),
            stack_dir=self.dir, extra_stack_pids=self.stack_pids,
            cancelled=self.cancelled,
        )

    def outcome(self, res, detail="", san_dirs=()):
        if res.stuck:
            return Outcome(STUCK, detail or f"no result after {res.duration:.0f}s; stacks saved",
                           signature="stuck", artifacts=[str(p) for p in res.stacks])
        sanitizer = reports.scan_sanitizer_dir(self.san_dir)
        for d in san_dirs:
            sanitizer += reports.scan_sanitizer_dir(d)
        if res.signal:
            crash = reports.scan_crash(reports.tail(self.log, 20000), self.log)
            sig = sanitizer[0].signature if sanitizer else (
                crash.signature if crash else f"signal {res.signal}")
            return Outcome(CRASHED, detail or sig, signature=sig)
        if sanitizer:
            return Outcome(FAILED, sanitizer[0].signature, signature=sanitizer[0].signature)
        if res.returncode != 0:
            crash = reports.scan_crash(reports.tail(self.log, 20000), self.log)
            if crash:
                return Outcome(CRASHED, crash.signature, signature=crash.signature)
            return Outcome(FAILED, detail or f"exit {res.returncode}")
        return Outcome(PASSED, detail)


class Scheduler:
    def __init__(self, ctx, units, fixture_factories):
        self.ctx = ctx
        self.units = sorted(units, key=lambda u: (-u.cost, u.id))
        self.factories = fixture_factories
        self.results = []
        self._cv = threading.Condition()
        self._free_cpus = ctx.jobs
        self._free_mem = ctx.mem_gb
        self._groups = set()
        self._running = {}
        self._fixture_locks = {}
        self._fixture_errors = {}
        self._stopping = False
        self._done = 0
        self._last_progress = 0.0
        self._blocked_since = None

    def _fixture(self, name):
        with self._cv:
            lock = self._fixture_locks.setdefault(name, threading.Lock())
        with lock:
            if name in self._fixture_errors:
                raise RuntimeError(f"fixture {name} failed to start: {self._fixture_errors[name]}")
            if name not in self.ctx.fixtures:
                fixture = self.factories[name](self.ctx)
                for required in fixture.requires:
                    self._fixture(required)
                t0 = time.monotonic()
                try:
                    fixture.start(self.ctx)
                except Exception as e:
                    self._fixture_errors[name] = str(e)
                    try:
                        fixture.stop()
                    except Exception:
                        pass
                    raise RuntimeError(f"fixture {name} failed to start: {e}") from e
                self.ctx.fixtures[name] = fixture
                self._log(f"fixture {name} up in {time.monotonic() - t0:.1f}s")
            return self.ctx.fixtures[name]

    def _log(self, msg):
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    def _resources_fit(self, unit):
        cpus = min(unit.cpus, self.ctx.jobs)
        mem = min(unit.mem_gb, self.ctx.mem_gb)
        return cpus <= self._free_cpus and mem <= self._free_mem

    def _fits(self, unit):
        if not self._resources_fit(unit):
            return False
        return unit.group is None or unit.group not in self._groups

    def _take(self, unit):
        self._free_cpus -= min(unit.cpus, self.ctx.jobs)
        self._free_mem -= min(unit.mem_gb, self.ctx.mem_gb)
        if unit.group:
            self._groups.add(unit.group)

    def _give(self, unit):
        self._free_cpus += min(unit.cpus, self.ctx.jobs)
        self._free_mem += min(unit.mem_gb, self.ctx.mem_gb)
        if unit.group:
            self._groups.discard(unit.group)

    def _execute(self, unit, attempt):
        started_wall = time.time()
        t0 = time.monotonic()
        run = None
        try:
            for name in unit.fixtures:
                self._fixture(name)
            run = Run(self.ctx, unit, attempt, lambda: self._stopping)
            out = unit.run(run)
            if self.ctx.attribute and attempt == 0:
                attribution.collect(self.ctx, unit, run, out.status)
        except Exception as e:
            out = Outcome(FAILED, f"harness error: {e}",
                          signature=f"harness: {type(e).__name__}")
            if run is not None:
                with open(run.log, "a") as fh:
                    fh.write(traceback.format_exc())
        duration = time.monotonic() - t0
        unit_dir = str(run.dir) if run else ""
        res = Result(unit.id, unit.suite, out.status, duration, attempt, started_wall,
                     out.detail, out.signature, dir=unit_dir, wrapper=bool(out.members))
        member_results = []
        for member, (status, seconds, detail, *signature) in out.members.items():
            member_results.append(Result(member, unit.suite, status, seconds, attempt,
                                         started_wall, detail, signature[0] if signature else "",
                                         member_of=unit.id, dir=unit_dir))
        if run is not None:
            shutil.rmtree(run.tmp, ignore_errors=True)
            if out.status == PASSED and not self.ctx.verbose:
                shutil.rmtree(run.dir, ignore_errors=True)
        return res, member_results

    def _progress(self, total, force=False):
        now = time.monotonic()
        if not force and now - self._last_progress < 30:
            return
        self._last_progress = now
        bad = sum(1 for r in self.results if r.status in BAD and not r.member_of)
        running = ", ".join(sorted(self._running)[:4])
        self._log(f"{self._done}/{total} done, {bad} bad, {len(self._running)} running"
                  + (f" ({running}{', ...' if len(self._running) > 4 else ''})"
                     if running else ""))

    def _report(self, res):
        if res.status in BAD:
            self._log(f"{res.status.upper()} {res.unit} ({res.duration:.1f}s): {res.detail}")
        elif self.ctx.verbose:
            self._log(f"{res.status} {res.unit} ({res.duration:.1f}s)")

    def run_pass(self, units, attempt=0, deadline=None):
        pending = list(units)
        total = len(pending)
        self._done = 0

        def worker(unit):
            res, members = self._execute(unit, attempt)
            with self._cv:
                self.results.append(res)
                self.results.extend(members)
                self._give(unit)
                del self._running[unit.id]
                self._done += 1
                self._report(res)
                self._cv.notify_all()

        with self._cv:
            while pending or self._running:
                if deadline is not None and time.monotonic() > deadline:
                    pending.clear()
                launched = True
                while launched and pending and not self._stopping:
                    launched = False
                    head_fits = self._resources_fit(pending[0])
                    if head_fits:
                        self._blocked_since = None
                    elif self._blocked_since is None:
                        self._blocked_since = time.monotonic()
                    reserve = (not head_fits and self._blocked_since is not None
                               and time.monotonic() - self._blocked_since > 60)
                    for i, unit in enumerate(pending):
                        if reserve and i > 0:
                            break
                        if self._fits(unit):
                            self._take(unit)
                            pending.pop(i)
                            self._running[unit.id] = unit
                            threading.Thread(target=worker, args=(unit,), daemon=True).start()
                            launched = True
                            break
                if self._stopping:
                    pending.clear()
                self._cv.wait(timeout=5)
                self._progress(total)
            self._progress(total, force=True)

    def relabel(self, budget_s, attempts=2, max_units=20):
        failed = [r for r in self.results if r.status in RERUN and r.attempt == 0]
        by_id = {u.id: u for u in self.units}
        wanted = []
        for r in failed:
            uid = r.member_of or r.unit
            if uid in by_id and by_id[uid].label and by_id[uid] not in wanted:
                wanted.append(by_id[uid])
        if not wanted or budget_s <= 0:
            return
        wanted = sorted(wanted, key=lambda u: u.cost)[:max_units]
        self._log(f"rerunning {len(wanted)} failed unit(s) on their own to label them")
        deadline = time.monotonic() + budget_s
        for attempt in range(1, attempts + 1):
            if time.monotonic() > deadline:
                break
            self._reset_stateful(wanted)
            self.run_pass(wanted, attempt=attempt, deadline=deadline)
        for r in failed:
            later = [x for x in self.results if x.unit == r.unit and x.attempt > 0]
            if any(x.status == PASSED for x in later):
                r.status = FLAKY
                r.detail += " (passed on a rerun: flaky)"
            elif later:
                r.detail += f" (failed again on {len(later)} rerun(s): deterministic)"

    def repeat(self, units, rounds, budget_s):
        first = {r.unit: r for r in self.results if r.attempt == 0 and r.status == PASSED}
        candidates = [u for u in units if u.id in first]
        if not candidates:
            return
        self._log(f"repeating {len(candidates)} changed unit(s) up to {rounds} time(s)")
        deadline = time.monotonic() + budget_s
        for k in range(rounds):
            if time.monotonic() > deadline:
                break
            self._reset_stateful(candidates)
            self.run_pass(candidates, attempt=100 + k, deadline=deadline)
        for r in list(first.values()):
            reps = [x for x in self.results if x.unit == r.unit and x.attempt >= 100]
            bad = [x for x in reps if x.status in RERUN]
            if bad:
                r.status = FLAKY
                r.signature = bad[0].signature
                r.detail = (f"passed, then failed {len(bad)} of {len(reps)} repeat(s): "
                            f"{bad[0].detail}")

    def invert(self):
        for r in self.results:
            if r.attempt != 0 or r.status == SKIPPED:
                continue
            if r.status == PASSED:
                r.status = FAILED
                r.detail = "passes without the fix: the test does not reproduce the bug"
            else:
                r.detail = f"fails without the fix, as it should: {r.detail}"
                r.status = PASSED

    def _stop_fixture(self, name):
        fixture = self.ctx.fixtures.pop(name)
        try:
            findings = fixture.stop()
        except Exception as e:
            findings = [Result(f"fixture:{name}", "fixture", FAILED, 0, 0, time.time(),
                               f"stop failed: {e}", f"fixture: {type(e).__name__}")]
        for f in findings or ():
            self.results.append(f)
            self._report(f)

    def _reset_stateful(self, units):
        started = self.ctx.fixtures
        reset = set()
        todo = [n for u in units for n in u.fixtures]
        while todo:
            name = todo.pop()
            if name in reset or name not in started or not started[name].stateful:
                continue
            reset.add(name)
            todo.extend(started[name].requires)
        while dependents := {n for n, f in started.items()
                             if n not in reset and reset.intersection(f.requires)}:
            reset |= dependents
        for name in reversed([n for n in started if n in reset]):
            self._stop_fixture(name)

    def stop_fixtures(self):
        shutil.rmtree(self.ctx.tmp_root, ignore_errors=True)
        for name in reversed(list(self.ctx.fixtures)):
            self._stop_fixture(name)

    def interrupt(self):
        with self._cv:
            self._stopping = True
            self._cv.notify_all()


def install_signal_handlers(scheduler):
    import signal

    def handler(signum, _frame):
        print(f"signal {signum}: stopping", file=sys.stderr, flush=True)
        scheduler.interrupt()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handler)


def env_without(keys):
    return {k: v for k, v in os.environ.items() if k not in keys}
