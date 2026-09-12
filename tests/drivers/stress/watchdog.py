import threading
import time

import psycopg

from procutil import samples_are_frozen, thread_sample

ALIVE = "alive"
STALLED = "stalled"
WEDGED = "wedged"
DEAD = "dead"


class Watchdog(threading.Thread):
    def __init__(self, dsn, profile, workers, server, stop_event):
        super().__init__(daemon=True, name="stress-watchdog")
        self.dsn = dsn
        self.profile = profile
        self.workers = workers
        self.server = server
        self.stop_event = stop_event
        self.expect_death = threading.Event()
        self.verdict = ALIVE
        self.probe_failures = 0
        self.probes = 0
        self.consecutive_failures = 0
        self.stalled_workers = []
        self.frozen = None
        self.samples = None
        self.detail = ""
        self.wedged_at = None
        self._t0 = time.monotonic()

    def probe_once(self):
        self.probes += 1
        try:
            with psycopg.connect(self.dsn, connect_timeout=self.profile.probe_timeout_s) as c:
                with c.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            return True
        except Exception:
            return False

    def _stalls(self):
        now = time.monotonic()
        out = []
        for w in self.workers:
            started = w.status.started_at
            if started is not None and (now - started) > self.profile.op_deadline_s:
                out.append((w.worker_id, w.status.op_kind, round(now - started, 1)))
        return out

    def run(self):
        while not self.stop_event.is_set():
            if self.expect_death.is_set():
                self.consecutive_failures = 0
                self.stop_event.wait(self.profile.probe_interval_s)
                continue
            if not self.server.running():
                self.verdict = DEAD
                self.detail = f"serened exited (rc={self.server.last_exit})"
                self.wedged_at = round(time.monotonic() - self._t0, 1)
                return
            ok = self.probe_once()
            if ok:
                self.consecutive_failures = 0
            else:
                self.probe_failures += 1
                self.consecutive_failures += 1
                if self.consecutive_failures >= self.profile.wedge_confirmations:
                    self._confirm_wedge()
                    return
            stalls = self._stalls()
            if stalls:
                self.stalled_workers = stalls
                for w in self.workers:
                    if any(s[0] == w.worker_id for s in stalls):
                        w.status.stalled = True
                        w.cancel_current()
            self.stop_event.wait(self.profile.probe_interval_s)

    def confirm_wedge_now(self):
        if not self.server.running():
            return False
        for _ in range(self.profile.wedge_confirmations):
            if self.probe_once():
                return False
        self._confirm_wedge()
        return True

    def _confirm_wedge(self):
        self.verdict = WEDGED
        self.wedged_at = round(time.monotonic() - self._t0, 1)
        pid = self.server.pid()
        self.stalled_workers = self._stalls()
        if pid:
            s1 = thread_sample(pid)
            time.sleep(self.profile.freeze_sample_gap_s)
            s2 = thread_sample(pid)
            self.frozen = samples_are_frozen(s1, s2)
            self.samples = (s1, s2)
            wch = {}
            for e in s1.values():
                key = e.get("wchan", "")
                wch[key] = wch.get(key, 0) + 1
            top = sorted(wch.items(), key=lambda kv: -kv[1])[:4]
            self.detail = (
                f"{len(s1)} threads, frozen={self.frozen}, "
                f"wchan={dict(top)}, "
                f"{self.consecutive_failures} consecutive fresh-connection probe "
                f"failures"
            )
        else:
            self.detail = "no pid available for /proc sampling"

    def as_dict(self):
        return {
            "verdict": self.verdict,
            "wedged_at_s": self.wedged_at,
            "probes": self.probes,
            "probe_failures": self.probe_failures,
            "stalled_workers": self.stalled_workers,
            "frozen": self.frozen,
            "detail": self.detail,
        }
