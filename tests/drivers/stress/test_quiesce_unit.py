import threading

import pytest

import config
import quiesce
from model import Outcome
from worker import Worker


class Journal:
    def write(self, record):
        pass


class HeldWorker(Worker):
    def __init__(self, hold, pause, stop):
        super().__init__(0, "", config.resolve("smoke"), "ab", 7, Journal(), None,
                         stop, pause, [], threading.Lock())
        self.hold = hold
        self.held = threading.Event()
        self.release = threading.Event()
        pick = self.pick

        def held_pick(rng, state):
            self._hold_at("pick")
            return pick(rng, state)

        self.pick = held_pick

    def _hold_at(self, where):
        if self.hold == where and not self.release.is_set():
            self.held.set()
            self.release.wait(10)

    def _connect(self):
        pass

    def _execute(self, op):
        self.status.op_kind = op.kind
        self.status.started_at = 0.0
        try:
            return Outcome.COMMITTED, None, ""
        finally:
            self.status.started_at = None
            self.status.op_kind = None

    def _apply(self, op, outcome):
        self._hold_at("apply")
        super()._apply(op, outcome)


@pytest.mark.parametrize("hold", ["pick", "apply"])
def test_drain_waits_until_every_acknowledged_op_is_in_its_model(hold):
    pause, stop = threading.Event(), threading.Event()
    worker = HeldWorker(hold, pause, stop)
    worker.start()
    try:
        assert worker.held.wait(10)
        drained = []
        drainer = threading.Thread(
            target=lambda: drained.append(quiesce.drain([worker], pause, timeout=10)))
        drainer.start()
        drainer.join(0.5)
        assert drainer.is_alive(), (
            f"drain returned while the worker was held at {hold}, before its op "
            "reached its model")
        worker.release.set()
        drainer.join(10)
        assert drained == [(True, [])]
        assert worker.status.committed == 1
    finally:
        worker.release.set()
        stop.set()
        quiesce.resume(pause)
        worker.join(10)
