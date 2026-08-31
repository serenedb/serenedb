import os
import re
import shutil
import signal
import socket
import struct
import subprocess
import tempfile
import time

from procutil import kill_tree

SANITIZER_RE = re.compile(
    r"(AddressSanitizer|UndefinedBehaviorSanitizer|ThreadSanitizer|LeakSanitizer"
    r"|MemorySanitizer|Assertion .* failed|FATAL|SIGSEGV|SIGABRT|SIGBUS"
    r"|lock-order-inversion|data race)"
)

PROTO_V3 = 196608
DEFAULT_START_TIMEOUT = 120.0


def free_port():
    s = socket.socket()
    try:
        s.bind(("", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def _startup_packet(user, database):
    body = struct.pack("!I", PROTO_V3)
    for k, v in (("user", user), ("database", database)):
        body += k.encode() + b"\x00" + v.encode() + b"\x00"
    body += b"\x00"
    return struct.pack("!I", len(body) + 4) + body


def pg_startup_reachable(host, port, user="postgres", database="postgres", timeout=5.0):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.sendall(_startup_packet(user, database))
        return bool(s.recv(1))
    except (OSError, socket.timeout):
        return False
    finally:
        s.close()


class ServerLog:
    def __init__(self, path):
        self.path = str(path)
        self._offset = 0

    def mark(self):
        try:
            self._offset = os.path.getsize(self.path)
        except OSError:
            self._offset = 0
        return self._offset

    def tail(self, since=None, limit_bytes=4 << 20):
        start = self._offset if since is None else since
        try:
            with open(self.path, "rb") as fh:
                fh.seek(start)
                return fh.read(limit_bytes).decode("utf-8", "replace")
        except OSError:
            return ""

    def scan(self, since=None):
        text = self.tail(since)
        return [ln for ln in text.splitlines() if SANITIZER_RE.search(ln)]


class Serened:
    def __init__(self, binary, port=None, datadir=None, datadir_root=None,
                 extra_args=(), log_path=None, ready=None, env=None,
                 prefix="serened-", auth_timeout="600s"):
        self.binary = str(binary)
        self.port = port or free_port()
        self.owns_datadir = datadir is None
        self.datadir = datadir or tempfile.mkdtemp(prefix=prefix, dir=datadir_root)
        self.extra_args = list(extra_args)
        self.log_path = str(log_path) if log_path else None
        self.log = ServerLog(self.log_path) if self.log_path else None
        self.ready = ready or (lambda h, p: pg_startup_reachable(h, p))
        self.env = env
        self.auth_timeout = auth_timeout
        self.proc = None
        self.generation = 0
        self.last_exit = None
        self._log_fh = None

    def dsn(self, database="postgres", user="postgres", connect_timeout=10):
        return (f"host=127.0.0.1 port={self.port} user={user} dbname={database} "
                f"connect_timeout={connect_timeout}")

    def argv(self):
        return [self.binary, self.datadir,
                f"--listen=postgres://0.0.0.0:{self.port}",
                f"--auth_timeout={self.auth_timeout}"] + self.extra_args

    def start(self, timeout=DEFAULT_START_TIMEOUT):
        if self.proc is not None and self.proc.poll() is None:
            raise RuntimeError("serened already running")
        self.generation += 1
        if self.log_path:
            path = self.log_path if self.generation == 1 else f"{self.log_path}.{self.generation}"
            self.log = ServerLog(path)
            self._log_fh = open(path, "ab", buffering=0)
            out = self._log_fh
        else:
            out = subprocess.DEVNULL
        self.log and self.log.mark()
        self.proc = subprocess.Popen(
            self.argv(), stdout=out, stderr=subprocess.STDOUT,
            start_new_session=True, env=self.env,
        )
        deadline = time.time() + timeout
        while time.time() < deadline:
            rc = self.proc.poll()
            if rc is not None:
                self.last_exit = rc
                raise RuntimeError(
                    f"serened exited {rc} during startup; log tail:\n"
                    + (self.log.tail(0)[-2000:] if self.log else "(no log)")
                )
            if self.ready("127.0.0.1", self.port):
                return self
            time.sleep(0.3)
        raise RuntimeError(f"serened did not come up in {timeout}s on port {self.port}")

    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def pid(self):
        return self.proc.pid if self.proc else None

    def wait_exit(self, timeout=30):
        if not self.proc:
            return None
        try:
            self.last_exit = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None
        return self.last_exit

    def signal(self, sig):
        if self.running():
            os.kill(self.proc.pid, sig)

    def abort(self):
        self.signal(signal.SIGABRT)
        return self.wait_exit(timeout=60)

    def kill(self):
        if self.proc:
            kill_tree(self.proc)
            self.last_exit = self.proc.poll()
        self.proc = None

    def restart(self, timeout=DEFAULT_START_TIMEOUT):
        self.kill()
        return self.start(timeout=timeout)

    def stop(self, keep_datadir=False):
        self.kill()
        if self._log_fh:
            try:
                self._log_fh.close()
            except OSError:
                pass
            self._log_fh = None
        if self.owns_datadir and not keep_datadir:
            shutil.rmtree(self.datadir, ignore_errors=True)

    def preserve_datadir(self, dest_root):
        os.makedirs(dest_root, exist_ok=True)
        dest = os.path.join(dest_root, os.path.basename(self.datadir.rstrip("/")))
        shutil.move(self.datadir, dest)
        self.owns_datadir = False
        return dest

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()
        return False
