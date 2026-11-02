import re
import subprocess
import tempfile

from ..scheduler import CRASHED, FAILED, PASSED, SKIPPED, STUCK, Outcome, Unit
from . import BELOW_RESOLUTION, plan, skipped

_RESULT = re.compile(r"^\[\s*(OK|FAILED|SKIPPED)\s*\] (\S+?)(?:, where .*)? \((\d+) ms\)\s*$")
_RUN = re.compile(r"^\[ RUN\s*\] (\S+)\s*$")
BATCH_SECONDS = 20.0
BATCH_MAX = 400


def list_tests(binary, extra_args, env):
    res = subprocess.run([str(binary), "--gtest_list_tests", *extra_args], capture_output=True,
                         text=True, timeout=300, env=env)
    if res.returncode != 0:
        raise SystemExit(f"{binary} --gtest_list_tests exited with {res.returncode}: "
                         f"{res.stderr.strip()[-2000:]}")
    tests = []
    suite = None
    for line in res.stdout.splitlines():
        if not line.strip() or line.startswith("Running main()"):
            continue
        if not line.startswith(" "):
            suite = line.split("#")[0].strip()
            if not suite.endswith("."):
                suite = None
            continue
        if suite:
            name = suite + line.split("#")[0].strip()
            if "DISABLED_" not in name:
                tests.append(name)
    return tests


def parse_log(text):
    started = []
    results = {}
    for line in text.splitlines():
        m = _RUN.match(line)
        if m:
            started.append(m.group(1))
            continue
        m = _RESULT.match(line)
        if m:
            status = {"OK": PASSED, "FAILED": FAILED, "SKIPPED": SKIPPED}[m.group(1)]
            results[m.group(2)] = (status, max(int(m.group(3)) / 1000.0, BELOW_RESOLUTION))
    unfinished = [t for t in started if t not in results]
    return results, unfinished


def _halt_on_reports(r):
    return {key: f"{r.env[key]}:halt_on_error=1" for key in ("TSAN_OPTIONS", "UBSAN_OPTIONS")
            if r.env.get(key)}


def _failure_text(text, test):
    lines = text.splitlines()
    out = []
    capture = False
    for line in lines:
        if line.startswith("[ RUN      ] " + test):
            capture = True
            continue
        if capture:
            if line.startswith("[  FAILED  ] " + test) or line.startswith("[       OK ]"):
                break
            if line.strip():
                out.append(line.strip())
    return " | ".join(out[:3])[:300]


class GtestBinary:
    def __init__(self, ctx, name, extra_args=(), env=None, label=None):
        self.ctx = ctx
        self.name = name
        self.path = ctx.build.bin(name)
        self.extra_args = list(extra_args)
        self.env = dict(env or {})
        self.label = label or name

    def units(self, skips, test_filter=None):
        env = dict(self.ctx.base_env)
        env.update(self.env)
        with tempfile.TemporaryDirectory() as tmp:
            tests = list_tests(self.path, [*self.extra_args, *self.list_args(tmp)], env)
        if test_filter:
            tests = [t for t in tests if test_filter(t)]
        planned = []
        for t in tests:
            uid = f"gtest:{self.label}:{t}"
            skip = skipped(skips, uid)
            if skip:
                self.ctx.skips.append((uid, skip))
                continue
            cost, timeout = plan(self.ctx, uid, 0.4)
            planned.append((t, cost, timeout, self.ctx.timing.expect(uid) is not None))
        units = []
        batch, batch_cost = [], 0.0
        target = BATCH_SECONDS * self.ctx.config.slowdown

        def flush():
            nonlocal batch, batch_cost
            if not batch:
                return
            members = tuple(t for t, _, _, _ in batch)
            uid = f"gtest:{self.label}:batch:{members[0]}+{len(members) - 1}"
            timeout = max(self.ctx.timeout_floor, 5 * batch_cost + 30 * self.ctx.config.slowdown,
                          batch_cost + max(t for _, _, t, _ in batch))
            units.append(Unit(uid, "gtest", self._run(members), cpus=1, mem_gb=1.0,
                              cost=batch_cost, timeout=timeout,
                              members=tuple(f"gtest:{self.label}:{m}" for m in members)))
            batch, batch_cost = [], 0.0

        for t, cost, timeout, timed in planned:
            if batch and (not timed or batch_cost + cost > target or len(batch) >= BATCH_MAX):
                flush()
            batch.append((t, cost, timeout, timed))
            batch_cost += cost
            if not timed:
                flush()
        flush()
        return units

    def list_args(self, tmp):
        return []

    def run_args(self, r):
        return []

    def _run(self, members):
        def run(r):
            remaining = list(members)
            outcome_members = {}
            failures = []
            crash = None
            exit_failure = None
            round_no = 0
            while remaining:
                round_no += 1
                log = r.dir / f"log.{round_no}.txt"
                argv = [self.path, "--gtest_filter=" + ":".join(remaining), *self.extra_args,
                        *self.run_args(r)]
                res = r.exec(argv, env={**self.env, **_halt_on_reports(r)}, log=log)
                text = log.read_text(errors="replace") if log.exists() else ""
                results, unfinished = parse_log(text)
                for t, (status, seconds) in results.items():
                    detail = _failure_text(text, t) if status == FAILED else ""
                    outcome_members[f"gtest:{self.label}:{t}"] = (status, seconds, detail)
                    if status == FAILED:
                        failures.append(t)
                if res.stuck:
                    stuck = unfinished[-1] if unfinished else "?"
                    ran = max(0.0, res.duration - sum(seconds for _, seconds in results.values()))
                    outcome_members[f"gtest:{self.label}:{stuck}"] = (STUCK, ran,
                                                                     "no result; stacks saved")
                    for t in remaining:
                        key = f"gtest:{self.label}:{t}"
                        outcome_members.setdefault(key, (SKIPPED, 0.0, "not run: batch timed out"))
                    return Outcome(STUCK, f"{stuck} did not finish; stacks saved",
                                   signature="stuck", members=outcome_members)
                out = r.outcome(res, log=log)
                done = set(results)
                if out.status == PASSED or (out.status == FAILED and not unfinished
                                            and not out.signature):
                    if out.status == FAILED and results and all(
                            status != FAILED for status, _ in results.values()):
                        exit_failure = f"exited {res.returncode} with every test passing"
                    remaining = [t for t in remaining if t not in done]
                    if not remaining:
                        break
                    if not results:
                        crash = crash or f"exit {res.returncode} before running any test"
                        for t in remaining:
                            outcome_members[f"gtest:{self.label}:{t}"] = (FAILED, 0.0, crash)
                        break
                    continue
                victim = unfinished[-1] if unfinished else None
                crash = out.signature or out.detail
                if victim:
                    outcome_members[f"gtest:{self.label}:{victim}"] = (CRASHED, 0.0, crash)
                    done.add(victim)
                remaining = [t for t in remaining if t not in done]
                if not victim:
                    for t in remaining:
                        outcome_members[f"gtest:{self.label}:{t}"] = (CRASHED, 0.0, crash)
                    break
                for p in list(r.san_dir.iterdir()):
                    p.rename(r.dir / f"{p.name}.round{round_no}")
            bad = [k for k, v in outcome_members.items() if v[0] in (FAILED, CRASHED, STUCK)]
            if crash:
                return Outcome(CRASHED, crash, signature=crash, members=outcome_members)
            if bad:
                return Outcome(FAILED, f"{len(bad)} test(s) failed: {', '.join(bad[:3])}",
                               members=outcome_members)
            if exit_failure:
                return Outcome(FAILED, exit_failure, members=outcome_members)
            return Outcome(PASSED, members=outcome_members)

        return run


def required_binary(path, target):
    if not path.is_file():
        raise SystemExit(f"{path} is missing, though the build has SDB_GTEST on: build the {target} "
                         "target")
    return path


def gtest_units(ctx, args, skips):
    if not ctx.build.gtests:
        return []
    required_binary(ctx.build.bin("serenedb-tests"), "serenedb-tests")
    return GtestBinary(ctx, "serenedb-tests", env={"LANG": "en_US"}).units(skips)


def re2_units(ctx, args, skips):
    if not ctx.build.gtests:
        return []
    required_binary(ctx.build.bin("re2-tests"), "re2-tests")
    return GtestBinary(ctx, "re2-tests").units(skips)


def croaring_units(ctx, args, skips):
    if not ctx.build.croaring_tests:
        return []
    tests = ctx.build.bin("croaring-tests")
    if not tests.is_dir():
        raise SystemExit(f"{tests} is missing, though the build has ENABLE_ROARING_TESTS on: build "
                         "the croaring-tests target")
    units = []
    for binary in sorted(tests.iterdir()):
        uid = f"croaring:{binary.name}"
        skip = skipped(skips, uid)
        if skip:
            ctx.skips.append((uid, skip))
            continue
        cost, timeout = plan(ctx, uid, 5.0)
        units.append(Unit(uid, "croaring", _croaring_run(ctx, binary), cost=cost, timeout=timeout))
    return units


def _croaring_run(ctx, binary):
    def run(r):
        return r.outcome(r.exec([str(binary)], cwd=ctx.root / "third_party" / "croaring"))

    return run


def iresearch_units(ctx, args, skips):
    if not ctx.build.gtests:
        return []
    required_binary(ctx.build.bin("iresearch-tests"), "iresearch-tests")
    resources = ctx.root / "resources" / "tests" / "iresearch"
    return IresearchBinary(ctx, "iresearch-tests", [f"--ires_resource_dir={resources}"],
                           env={"MALLOC_ARENA_MAX": "1"}).units(skips)


class IresearchBinary(GtestBinary):
    def list_args(self, tmp):
        return [f"--ires_output_path={tmp}"]

    def run_args(self, r):
        return [f"--ires_output_path={r.tmp}"]
