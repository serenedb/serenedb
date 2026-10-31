import threading

import config
import ops
from model import Outcome
from worker import Worker


class Journal:
    def write(self, record):
        pass


class ScriptedWorker(Worker):
    def __init__(self, steps):
        self.downtime = threading.Event()
        super().__init__(0, "", config.resolve("smoke"), "ab", 7, Journal(), None,
                         threading.Event(), threading.Event(), [], threading.Lock(),
                         planned_downtime=self.downtime)
        self.steps = list(steps)
        self.outcome = None
        self.pick = self._next_step

    def _next_step(self, rng, state):
        build, outcome, downtime = self.steps.pop(0)
        if downtime:
            self.downtime.set()
        else:
            self.downtime.clear()
        if not self.steps:
            self.stop_event.set()
        self.outcome = outcome
        return build()

    def _connect(self):
        pass

    def _reconnect(self):
        return True

    def _execute(self, op):
        return self.outcome


def test_an_op_on_a_table_whose_drop_a_restart_cut_off_may_find_it_gone():
    names = ops.NameGen("ab", 0)
    table = ops.create_table(names)
    worker = ScriptedWorker([
        (lambda: table, (Outcome.COMMITTED, None, ""), False),
        (lambda: ops.drop_table(table.key),
         (Outcome.UNKNOWN_CRASH, None, "server closed the connection unexpectedly"), True),
        (lambda: ops.create_index(names, table.key),
         (Outcome.REFUSED_CONFLICT, "42P01", f"Table with name {table.key[1]} does not exist!"),
         False),
    ])
    worker.run()
    assert worker.findings == []
    assert worker.labels.get("shared_key_vanished") == 1
