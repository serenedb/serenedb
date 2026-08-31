import threading
import time

import faults as faults_mod

CATALOG_WINDOW_FAULTS = (
    faults_mod.CRASH_BEFORE_CATALOG_COMMIT_FAULT,
    faults_mod.CRASH_AFTER_CATALOG_BEFORE_DATA_FAULT,
    faults_mod.CRASH_ON_DROP_FAULT,
)

ABORT_FAULTS = (
    faults_mod.CATALOG_APPEND_FAILS_FAULT,
)


class ChaosResult:
    def __init__(self):
        self.crashes_attempted = 0
        self.crashes_observed = 0
        self.restarts_ok = 0
        self.restart_failures = []
        self.aborts_injected = 0
        self.restarts_attempted = 0
        self.parks = 0
        self.cancels = 0
        self.faults_used = []
        self.timeline = []

    def as_dict(self):
        return {
            "crashes_attempted": self.crashes_attempted,
            "crashes_observed": self.crashes_observed,
            "restarts_ok": self.restarts_ok,
            "restart_failures": self.restart_failures,
            "aborts_injected": self.aborts_injected,
            "restarts_attempted": self.restarts_attempted,
            "parks": self.parks,
            "cancels": self.cancels,
            "faults_used": self.faults_used,
            "timeline": self.timeline[-40:],
        }


class Chaos:
    def __init__(self, server, broker, watchdog, profile, rng, findings, findings_lock,
                 on_recovered=None, planned_downtime=None):
        self.server = server
        self.broker = broker
        self.watchdog = watchdog
        self.profile = profile
        self.rng = rng
        self.findings = findings
        self.findings_lock = findings_lock
        self.on_recovered = on_recovered
        self.planned_downtime = planned_downtime or threading.Event()
        self.result = ChaosResult()
        self._lock = threading.Lock()

    def _finding(self, kind, detail):
        with self.findings_lock:
            self.findings.append({"kind": kind, "key": None, "detail": detail,
                                  "candidates": None, "observed": None})

    def available(self):
        return self.broker is not None and self.broker.available()

    def inject_abort(self, seconds=1.0):
        name = self.rng.choice(ABORT_FAULTS)
        try:
            self.broker.arm(name)
        except Exception as exc:
            self._finding("chaos_arm_failed", f"{name}: {exc}")
            return
        self.result.aborts_injected += 1
        self.result.faults_used.append(name)
        time.sleep(seconds)
        try:
            self.broker.disarm(name)
        except Exception:
            pass

    def cancel_an_inflight_op(self, workers):
        inflight = [w for w in workers
                    if w.status.started_at is not None and not w.status.finished]
        # Prefer something actually slow enough to still be running: at a few
        # thousand ops a second an ordinary statement finishes before the cancel
        # request reaches the server, and the axis goes untested.
        slow = [w for w in inflight if (w.status.op_kind or "").startswith("slow_")]
        candidates = slow or inflight
        if not candidates:
            self.result.timeline.append({"fault": None, "outcome": "nothing_in_flight"})
            return False
        target = self.rng.choice(candidates)
        kind = target.status.op_kind
        ok = target.cancel_current()
        self.result.cancels += 1
        self.result.timeline.append({
            "fault": None, "outcome": "cancelled" if ok else "cancel_failed",
            "op_kind": kind, "worker": target.worker_id})
        if not ok:
            self._finding(
                "cancel_request_failed",
                f"cancel_safe() failed against worker {target.worker_id} "
                f"running {kind}")
        return ok

    def park_and_probe(self, progress_of, seconds=8.0):
        name = faults_mod.PAUSE_CREATE_INDEX_MID_BUILD_FAULT
        before = progress_of()
        try:
            self.broker.arm(name)
        except Exception as exc:
            self._finding("chaos_arm_failed", f"{name}: {exc}")
            return False
        self.result.faults_used.append(name)
        alive_during = True
        try:
            time.sleep(seconds / 2.0)
            alive_during = self.watchdog.probe_once()
            time.sleep(seconds / 2.0)
            during = progress_of()
        finally:
            try:
                self.broker.disarm(name)
            except Exception:
                pass
        gained = during - before
        self.result.timeline.append({
            "fault": name, "outcome": "parked",
            "committed_while_parked": gained, "alive_while_parked": alive_during})
        if not alive_during:
            self._finding(
                "no_liveness_while_a_build_was_parked",
                f"a fresh connection could not run SELECT 1 while {name} was armed; "
                "a parked index build must not take the server with it")
            return False
        if gained == 0:
            self._finding(
                "no_progress_while_a_build_was_parked",
                f"no worker committed anything during {seconds:.0f}s with {name} "
                "armed; a parked build must not block unrelated work")
            return False
        return True

    def graceful_restart(self, timeout=120.0):
        with self._lock:
            t0 = time.monotonic()
            self.watchdog.expect_death.set()
            self.planned_downtime.set()
            self.result.restarts_attempted = getattr(
                self.result, "restarts_attempted", 0) + 1
            rc = self.server.graceful_stop(timeout=timeout)
            if rc is None:
                self.watchdog.expect_death.clear()
                self._finding(
                    "graceful_shutdown_did_not_finish",
                    f"SIGTERM did not bring serened down within {timeout:.0f}s; the "
                    "shutdown checkpoint path is stuck")
                return False
            self.broker.forget_all()
            try:
                self.server.start()
                self.result.restarts_ok += 1
                self.result.timeline.append({
                    "fault": None, "outcome": "graceful_restart",
                    "exit_code": rc, "generation": self.server.generation,
                    "after_s": round(time.monotonic() - t0, 1)})
                ok = True
            except Exception as exc:
                self.result.restart_failures.append(f"graceful: {exc}"[:300])
                self._finding(
                    "restart_after_graceful_shutdown_failed",
                    f"serened did not come back after a clean SIGTERM: {exc}"[:300])
                ok = False
            finally:
                self.watchdog.expect_death.clear()
            if ok and self.on_recovered is not None:
                self.on_recovered("graceful_restart")
            self.planned_downtime.clear()
            return ok

    def crash_and_restart(self, timeout=90.0):
        with self._lock:
            name = self.rng.choice(CATALOG_WINDOW_FAULTS)
            t0 = time.monotonic()
            self.watchdog.expect_death.set()
            self.planned_downtime.set()
            self.result.crashes_attempted += 1
            self.result.faults_used.append(name)
            try:
                self.broker.arm(name)
            except Exception as exc:
                self.watchdog.expect_death.clear()
                self._finding("chaos_arm_failed", f"{name}: {exc}")
                return False

            died = False
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if not self.server.running():
                    died = True
                    break
                time.sleep(0.2)

            if not died:
                try:
                    self.broker.disarm(name)
                except Exception:
                    pass
                self.watchdog.expect_death.clear()
                self.planned_downtime.clear()
                # A crash fault that never fires means the commit path was never
                # reached. The ordinary reason is that the server is already
                # wedged, and expect_death has been masking the watchdog's own
                # detection for the whole wait -- so confirm it here rather than
                # letting the run report itself healthy.
                wedged = self.watchdog.confirm_wedge_now()
                outcome = "wedged_before_the_fault_could_fire" if wedged \
                    else "no_crash_within_timeout"
                self.result.timeline.append({"fault": name, "outcome": outcome})
                if wedged:
                    self._finding(
                        "server_wedged_during_chaos_window",
                        f"armed {name} for {timeout:.0f}s and the commit path was "
                        f"never reached; the server is wedged: "
                        f"{self.watchdog.detail}")
                else:
                    self._finding(
                        "crash_fault_never_fired",
                        f"{name} was armed for {timeout:.0f}s while the server "
                        f"stayed responsive; the scenario never reached its site")
                return False

            self.result.crashes_observed += 1
            self.broker.forget_all()

            try:
                self.server.start()
                self.result.restarts_ok += 1
                self.result.timeline.append({
                    "fault": name, "outcome": "crashed_and_restarted",
                    "generation": self.server.generation,
                    "after_s": round(time.monotonic() - t0, 1)})
                ok = True
            except Exception as exc:
                self.result.restart_failures.append(f"{name}: {exc}"[:300])
                self._finding(
                    "restart_after_injected_crash_failed",
                    f"fault {name}: serened did not come back: {exc}"[:300])
                ok = False
            finally:
                self.watchdog.expect_death.clear()

            if ok and self.on_recovered is not None:
                self.on_recovered(name)
            self.planned_downtime.clear()
            return ok
