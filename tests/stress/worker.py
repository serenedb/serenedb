import threading
import time

import psycopg

import classify
import ops as ops_mod
import scenarios
from model import Model, Outcome
from ops import NameGen
from rng import derive


class WorkerStatus:
    __slots__ = ("worker_id", "op_kind", "started_at", "committed", "retries",
                 "stalled", "finished", "cancel_sent")

    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.op_kind = None
        self.started_at = None
        self.committed = 0
        self.retries = 0
        self.stalled = False
        self.finished = False
        self.cancel_sent = 0


class Worker(threading.Thread):
    def __init__(self, worker_id, dsn, profile, run_tag, seed, journal, broker,
                 stop_event, pause_event, findings, findings_lock):
        super().__init__(daemon=True, name=f"stress-w{worker_id}")
        self.worker_id = worker_id
        self.dsn = dsn
        self.profile = profile
        self.journal = journal
        self.broker = broker
        self.stop_event = stop_event
        self.pause_event = pause_event
        self.findings = findings
        self.findings_lock = findings_lock
        self.model = Model()
        self.names = NameGen(run_tag, worker_id)
        self.state = scenarios.WorkerState(self.names)
        self.pick = scenarios.resolve(profile.scenario)
        self.rng = derive(seed, worker_id)
        self.status = WorkerStatus(worker_id)
        self.labels = {}
        self.op_kinds = {}
        self.conn = None
        self._conn_lock = threading.Lock()

    def _record_finding(self, kind, detail, extra=None):
        entry = {"kind": kind, "key": None, "detail": detail,
                 "candidates": None, "observed": None, "worker": self.worker_id}
        if extra:
            entry.update(extra)
        with self.findings_lock:
            self.findings.append(entry)

    def _bump(self, label):
        self.labels[label] = self.labels.get(label, 0) + 1

    def cancel_current(self):
        with self._conn_lock:
            conn = self.conn
        if conn is None:
            return False
        try:
            conn.cancel_safe(timeout=5)
            self.status.cancel_sent += 1
            return True
        except Exception:
            return False

    def _connect(self):
        conn = psycopg.connect(self.dsn)
        conn.autocommit = False
        with self._conn_lock:
            self.conn = conn

    def _declare(self, op):
        for key, _token in op.creates:
            if not self.model.is_owned(key):
                self.model.declare_owned(key)

    def _scope_for(self, key):
        if key is None:
            return "private"
        cands = self.model.candidates(key)
        if cands is not None and len(cands) > 1:
            return "shared"
        return "private"

    def _apply(self, op, outcome):
        for key, token in op.creates:
            self.model.apply_create(key, token, outcome, rows=op.rows_added)
        for key in list(op.drops) + list(op.cascade):
            if self.model.is_owned(key):
                self.model.apply_drop(key, outcome)
        if not op.creates and op.key is not None \
                and (op.rows_added or op.rows_removed):
            self.model.apply_rows(op.key, added=op.rows_added,
                                  removed=op.rows_removed, outcome=outcome)

    def _execute(self, op):
        self.status.op_kind = op.kind
        self.status.started_at = time.monotonic()
        try:
            for sql in op.statements:
                with self.conn.cursor() as cur:
                    cur.execute(sql)
            self.conn.commit()
            return Outcome.COMMITTED, None, ""
        except psycopg.Error as exc:
            state = exc.sqlstate
            msg = str(exc).replace("\n", " ")[:200]
            if state is None or getattr(self.conn, "closed", False):
                return Outcome.UNKNOWN_CRASH, state, msg
            try:
                self.conn.rollback()
            except Exception:
                return Outcome.UNKNOWN_CRASH, state, msg
            if state == classify.QUERY_CANCELED:
                return Outcome.UNKNOWN_CANCEL, state, msg
            return Outcome.REFUSED_CONFLICT, state, msg
        finally:
            self.status.started_at = None
            self.status.op_kind = None

    def run(self):
        try:
            self._connect()
        except Exception as exc:
            self._record_finding("worker_connect_failed", str(exc)[:200])
            self.status.finished = True
            return
        try:
            self._loop()
        except Exception as exc:
            self._record_finding("worker_crashed", f"{type(exc).__name__}: {exc}"[:300])
        finally:
            self.status.finished = True
            try:
                with self._conn_lock:
                    if self.conn is not None:
                        self.conn.close()
            except Exception:
                pass

    def _loop(self):
        while not self.stop_event.is_set():
            while self.pause_event.is_set() and not self.stop_event.is_set():
                time.sleep(0.05)
            if self.stop_event.is_set():
                break
            op = self.pick(self.rng, self.state)
            self.op_kinds[op.kind] = self.op_kinds.get(op.kind, 0) + 1
            self._declare(op)
            attempts = 0
            while True:
                attempts += 1
                outcome, sqlstate, msg = self._execute(op)
                dead = outcome is Outcome.UNKNOWN_CRASH
                scope = self._scope_for(op.key)
                cls = classify.classify(
                    sqlstate if outcome is not Outcome.COMMITTED else None,
                    msg, op.kind, scope,
                    self.broker.armed() if self.broker else (),
                    dead_connection=dead,
                    cancel_requested=self.status.cancel_sent > 0,
                )
                self._bump(cls.label)
                if cls.is_finding:
                    self._record_finding(
                        f"unexpected_error_{cls.label}",
                        f"{op.kind}: sqlstate={sqlstate} {msg}",
                        {"key": list(op.key) if op.key else None},
                    )
                if outcome is Outcome.REFUSED_CONFLICT and cls.label == \
                        "permanent_40001_reported_retryable":
                    outcome = Outcome.REFUSED_PERMANENT
                self.journal.write({
                    "w": self.worker_id, "op": op.as_record(),
                    "outcome": outcome.value, "sqlstate": sqlstate,
                    "label": cls.label, "attempt": attempts,
                    "t": round(time.monotonic(), 4),
                })
                if cls.retryable and attempts < self.profile.max_retries \
                        and not self.stop_event.is_set():
                    self.status.retries += 1
                    if attempts >= self.profile.livelock_retry_limit:
                        self._record_finding(
                            "retry_livelock",
                            f"{op.kind} still retryable after {attempts} attempts: {msg}")
                        break
                    time.sleep(min(0.005 * attempts, 0.05))
                    continue
                break

            self._apply(op, outcome)
            if outcome is Outcome.COMMITTED:
                self.state.note_created(op)
                self.state.note_rows(op)
                self.state.note_dropped(op)
                self.status.committed += 1
            if outcome is Outcome.UNKNOWN_CRASH:
                if not self._reconnect():
                    break

    def _reconnect(self):
        for _ in range(40):
            if self.stop_event.is_set():
                return False
            try:
                self._connect()
                return True
            except Exception:
                time.sleep(0.5)
        return False
