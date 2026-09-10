import os
import resource
import signal
import subprocess
import time


def _pgid(proc):
    try:
        return os.getpgid(proc.pid)
    except (ProcessLookupError, PermissionError):
        return None


def kill_tree(proc, grace=5.0):
    if proc is None or proc.poll() is not None:
        return proc.poll() if proc else None
    gid = _pgid(proc)
    if gid is not None and gid != os.getpgid(0):
        try:
            os.killpg(gid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    else:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
    try:
        return proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
            return proc.wait(timeout=grace)
        except Exception:
            return None


def reap_orphans(patterns, exclude_pid=None):
    if not patterns:
        return []
    try:
        out = subprocess.run(
            ["ps", "-eo", "pid,args", "--no-headers"],
            capture_output=True, text=True, timeout=20,
        ).stdout
    except Exception:
        return []
    killed = []
    me = os.getpid()
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        pid_s, _, args = line.partition(" ")
        try:
            pid = int(pid_s)
        except ValueError:
            continue
        if pid in (me, exclude_pid):
            continue
        if any(p in args for p in patterns):
            try:
                os.kill(pid, signal.SIGKILL)
                killed.append((pid, args[:120]))
            except (ProcessLookupError, PermissionError):
                pass
    return killed


def thread_sample(pid):
    base = f"/proc/{pid}/task"
    sample = {}
    try:
        tids = os.listdir(base)
    except OSError:
        return sample
    for tid in tids:
        entry = {}
        try:
            with open(f"{base}/{tid}/stat", "rb") as fh:
                fields = fh.read().decode("utf-8", "replace").rsplit(") ", 1)[-1].split()
            entry["state"] = fields[0]
            entry["utime"] = int(fields[11])
            entry["stime"] = int(fields[12])
        except (OSError, IndexError, ValueError):
            continue
        for name in ("wchan", "syscall"):
            try:
                with open(f"{base}/{tid}/{name}", "rb") as fh:
                    entry[name] = fh.read().decode("utf-8", "replace").strip()[:80]
            except OSError:
                entry[name] = ""
        sample[tid] = entry
    return sample


def samples_are_frozen(a, b):
    if not a or not b:
        return False
    if set(a) != set(b):
        return False
    for tid, ea in a.items():
        eb = b[tid]
        if ea["utime"] != eb["utime"] or ea["stime"] != eb["stime"]:
            return False
    return True


def frozen_thread_report(a, b, limit=40):
    lines = []
    for tid in sorted(a, key=lambda t: int(t))[:limit]:
        ea = a[tid]
        eb = b.get(tid, {})
        lines.append(
            f"tid={tid} state={ea['state']}->{eb.get('state', '?')} "
            f"utime={ea['utime']}/{eb.get('utime', '?')} "
            f"wchan={ea.get('wchan', '')} syscall={ea.get('syscall', '')}"
        )
    return "\n".join(lines)


def raise_open_files(target=16384):
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (ValueError, OSError):
        return None
    want = min(target, hard) if hard != resource.RLIM_INFINITY else target
    if soft >= want:
        return soft
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
        return want
    except (ValueError, OSError):
        return soft


def enable_core_dumps():
    try:
        _, hard = resource.getrlimit(resource.RLIMIT_CORE)
        resource.setrlimit(resource.RLIMIT_CORE, (hard, hard))
        return hard
    except (ValueError, OSError):
        return None


def wait_until(predicate, timeout, interval=0.2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False
