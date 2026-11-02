import time


def busy(workers):
    return [(w.worker_id, w.status.op_kind) for w in workers
            if not w.status.parked and not w.status.finished]


def drain(workers, pause_event, timeout=120.0, abort_if=None):
    pause_event.set()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if abort_if is not None and abort_if():
            return False, busy(workers)
        if not busy(workers):
            return True, []
        time.sleep(0.1)
    return False, busy(workers)


def resume(pause_event):
    pause_event.clear()
