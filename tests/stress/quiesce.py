import time


def drain(workers, pause_event, timeout=120.0, abort_if=None):
    pause_event.set()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if abort_if is not None and abort_if():
            return False, [(w.worker_id, w.status.op_kind) for w in workers
                           if w.status.started_at is not None and not w.status.finished]
        in_flight = [w for w in workers
                     if w.status.started_at is not None and not w.status.finished]
        if not in_flight:
            return True, []
        time.sleep(0.1)
    return False, [(w.worker_id, w.status.op_kind) for w in workers
                   if w.status.started_at is not None and not w.status.finished]


def resume(pause_event):
    pause_event.clear()
