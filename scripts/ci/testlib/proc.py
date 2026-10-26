import contextlib
import ctypes
import os
import shutil
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

_PR_SET_PTRACER = 0x59616D61
_PR_SET_PTRACER_ANY = ctypes.c_ulong(0xFFFFFFFFFFFFFFFF)

try:
    _libc = ctypes.CDLL(None, use_errno=True)
except OSError:
    _libc = None


def allow_tracing():
    if _libc is not None:
        _libc.prctl(_PR_SET_PTRACER, _PR_SET_PTRACER_ANY, 0, 0, 0)


def _tracing_needs_exception():
    try:
        scope = int(Path("/proc/sys/kernel/yama/ptrace_scope").read_text())
    except (OSError, ValueError):
        return False
    if scope == 0:
        return False
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("CapEff:"):
                return not (int(line.split()[1], 16) >> 19) & 1
    except (OSError, ValueError):
        pass
    return True


_PREEXEC = allow_tracing if _tracing_needs_exception() else None


def lldb_binary():
    for name in (os.environ.get("SDB_LLDB", ""), "lldb-23", "lldb"):
        if name and shutil.which(name):
            return name
    return None


def session_pids(sid):
    if not os.path.isdir("/proc"):
        out = subprocess.run(["ps", "-A", "-o", "pid=,pgid="], capture_output=True,
                             text=True).stdout
        return sorted(int(pid) for pid, pgid in (ln.split() for ln in out.splitlines())
                      if int(pgid) == sid)
    pids = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat", "rb") as fh:
                fields = fh.read().decode("utf-8", "replace").rsplit(") ", 1)[-1].split()
        except OSError:
            continue
        if len(fields) > 3 and int(fields[3]) == sid:
            pids.append(int(entry))
    return sorted(pids)


def process_name(pid):
    try:
        return Path(f"/proc/{pid}/comm").read_text().strip()
    except OSError:
        pass
    name = subprocess.run(["ps", "-o", "comm=", "-p", str(pid)], capture_output=True,
                          text=True).stdout.strip()
    return Path(name).name or "?"


def dump_stacks(pid, out_path, timeout=120):
    lldb = lldb_binary()
    header = f"pid {pid} ({process_name(pid)})\n"
    if lldb is None:
        Path(out_path).write_text(header + "no lldb on PATH\n")
        return out_path
    try:
        res = subprocess.run(
            [lldb, "--batch", "--no-lldbinit", "-p", str(pid),
             "-o", "thread backtrace all", "-o", "process detach"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            timeout=timeout, start_new_session=True,
        )
        text = res.stdout
    except subprocess.TimeoutExpired:
        text = "lldb timed out\n"
    Path(out_path).write_text(header + text)
    return out_path


def dump_all_stacks(pids, out_dir, prefix="stacks"):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    threads = []
    for pid in pids:
        path = out_dir / f"{prefix}-{process_name(pid)}-{pid}.txt"
        paths.append(path)
        t = threading.Thread(target=dump_stacks, args=(pid, path), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join(180)
    return paths


def kill_session(sid, grace=0.0):
    try:
        os.killpg(sid, signal.SIGTERM if grace else signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        return
    if grace:
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline and session_pids(sid):
            time.sleep(0.1)
        try:
            os.killpg(sid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


@dataclass
class ProcResult:
    returncode: int | None
    duration: float
    stuck: bool = False
    stacks: list = field(default_factory=list)

    @property
    def signal(self):
        if self.returncode is not None and self.returncode < 0:
            return -self.returncode
        return None

    @property
    def ok(self):
        return not self.stuck and self.returncode == 0


def spawn(argv, *, env, cwd, log_path):
    log = open(log_path, "ab", buffering=0)
    try:
        return subprocess.Popen(
            [str(a) for a in argv], stdin=subprocess.DEVNULL, stdout=log,
            stderr=subprocess.STDOUT, cwd=str(cwd), env=env,
            start_new_session=True, preexec_fn=_PREEXEC,
        )
    finally:
        log.close()


def run(argv, *, env, cwd, log_path, timeout, stack_dir, extra_stack_pids=lambda: (),
        cancelled=None, stalled=lambda: 0.0, dumping=contextlib.nullcontext):
    start = time.monotonic()
    proc = spawn(argv, env=env, cwd=cwd, log_path=log_path)
    deadline = start + timeout
    while True:
        remaining = deadline + stalled() - time.monotonic()
        try:
            rc = proc.wait(timeout=max(0.0, min(remaining, 1.0)))
            kill_session(proc.pid)
            return ProcResult(rc, time.monotonic() - start)
        except subprocess.TimeoutExpired:
            pass
        if cancelled is not None and cancelled():
            kill_session(proc.pid)
            proc.wait()
            return ProcResult(None, time.monotonic() - start)
        if time.monotonic() >= deadline + stalled():
            break
    with dumping():
        pids = session_pids(proc.pid) + [p for p in extra_stack_pids() if p]
        stacks = dump_all_stacks(pids, stack_dir)
    kill_session(proc.pid)
    proc.wait()
    return ProcResult(None, time.monotonic() - start, stuck=True, stacks=stacks)
