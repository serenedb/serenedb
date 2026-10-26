import contextlib
import shutil
import signal
import subprocess
import threading
import time
from pathlib import Path

from serened import listens_on, pg_startup_reachable, reserve_port

from . import proc, reports
from .scheduler import CRASHED, FAILED, STUCK, Fixture, Result

START_TIMEOUT = 300


class Server:
    def __init__(self, ctx, home, *, threads=0, listen_ssl=False, http_apis=(), hba=None,
                 extra_args=(), extra_env=None, profile_dir=None):
        self.ctx = ctx
        self.home = Path(home)
        self.profile_dir = profile_dir or ctx.out_dir / "profiles"
        self.threads = threads
        self.listen_ssl = listen_ssl
        self.http_apis = tuple(http_apis)
        self.hba = hba
        self.extra_args = list(extra_args)
        self.extra_env = dict(extra_env or {})
        self.host = "127.0.0.1"
        self.port = None
        self.ssl_port = None
        self.http_port = None
        self.reserved = []
        self.proc = None
        self.generation = 0
        self.datadir = None
        self.log = None
        self.san_dir = self.home / "san"
        self.exits = []

    def env(self):
        env = dict(self.ctx.base_env)
        env.update(self.ctx.build.sanitizer_env(self.san_dir))
        env.update(self.ctx.build.coverage_env(self.profile_dir))
        env.update(self.extra_env)
        return env

    def argv(self):
        listen = f"postgres://127.0.0.1:{self.port}"
        if self.listen_ssl:
            ca = self.ctx.root / "resources" / "ca.pem"
            listen += f",postgres://127.0.0.1:{self.ssl_port}?sslmode=require&cert={ca}&key={ca}"
        if self.http_apis:
            listen += f",http://127.0.0.1:{self.http_port}?" + "&".join(f"api={a}" for a in self.http_apis)
        args = [str(self.ctx.build.bin("serened")), str(self.datadir), f"--listen={listen}",
                "--auth_timeout=600s"]
        if self.threads:
            args.append(f"--cpu_threads={self.threads}")
        return args + self.extra_args

    def start(self, fresh=True):
        self.generation += 1
        self.home.mkdir(parents=True, exist_ok=True)
        self.san_dir.mkdir(exist_ok=True)
        if fresh or self.datadir is None:
            if self.datadir is not None:
                shutil.rmtree(self.datadir, ignore_errors=True)
            self.datadir = self.home / f"data.{self.generation}"
            self.datadir.mkdir()
            if self.hba is not None:
                (self.datadir / "pg_hba.conf").write_text(self.hba)
        self._release_ports()
        self.port = self._reserve_port()
        self.ssl_port = self._reserve_port() if self.listen_ssl else None
        self.http_port = self._reserve_port() if self.http_apis else None
        self.log = self.home / f"log.{self.generation}.txt"
        self.proc = proc.spawn(self.argv(), env=self.env(), cwd=self.ctx.root, log_path=self.log)
        if self._wait_ready():
            return self
        raise RuntimeError(f"serened did not start (exit {self.proc.poll()}); log tail:\n"
                           + reports.tail(self.log, 3000))

    def _reserve_port(self):
        sock = reserve_port()
        self.reserved.append(sock)
        return sock.getsockname()[1]

    def _release_ports(self):
        for sock in self.reserved:
            sock.close()
        self.reserved = []

    def _wait_ready(self):
        deadline = time.monotonic() + START_TIMEOUT
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                return False
            ports = [p for p in (self.port, self.ssl_port, self.http_port) if p]
            if (pg_startup_reachable("127.0.0.1", self.port, timeout=2.0)
                    and listens_on(self.proc.pid, ports)):
                return True
            time.sleep(0.2)
        return False

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def pid(self):
        return self.proc.pid if self.alive() else None

    def stop(self, timeout=180):
        if self.proc is None:
            return None
        if self.proc.poll() is None:
            try:
                self.proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                stacks = proc.dump_all_stacks([self.proc.pid], self.home, prefix="stop-stacks")
                proc.kill_session(self.proc.pid)
                self.proc.wait()
                self._release_ports()
                return ("stuck", stacks)
        proc.kill_session(self.proc.pid)
        self._release_ports()
        return ("exit", self.proc.returncode)

    def crash_signature(self):
        san = reports.scan_sanitizer_dir(self.san_dir)
        if san:
            return san[0].signature
        crash = reports.scan_crash(reports.tail(self.log, 30000), self.log)
        return crash.signature if crash else f"exit {self.proc.returncode if self.proc else '?'}"


class ServerPool(Fixture):
    stateful = True

    def __init__(self, ctx, count, threads, name="servers", **server_args):
        self.ctx = ctx
        self.name = name
        server_args.setdefault("listen_ssl", True)
        home = ctx.out_dir / name
        instance = 1
        while home.exists():
            instance += 1
            home = ctx.out_dir / f"{name}.{instance}"
        self.servers = [Server(ctx, home / str(i), threads=threads, **server_args)
                        for i in range(count)]
        self.load = [0] * count
        self.stalls = [[] for _ in range(count)]
        self.lock = threading.Lock()
        self.findings = []
        self.crash_info = {}
        self._stop = threading.Event()
        self._monitor = None

    def start(self, ctx):
        errors = []

        def start_one(s):
            try:
                s.start()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=start_one, args=(s,), daemon=True) for s in self.servers]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if errors:
            raise errors[0]
        self._monitor = threading.Thread(target=self._watch, daemon=True)
        self._monitor.start()

    def _watch(self):
        while not self._stop.wait(1.0):
            for i, s in enumerate(self.servers):
                with self.lock:
                    if s.proc is not None and s.proc.poll() is not None and not self._stop.is_set():
                        self._restart(i, s)

    def _restart(self, i, s):
        sig = s.crash_signature()
        rc = s.proc.returncode
        status = CRASHED
        self.crash_info[(i, s.generation)] = sig
        keep = s.home / f"crashed.{s.generation}"
        if s.log and s.log.exists():
            shutil.copy(s.log, keep.with_suffix(".log"))
        self.findings.append(Result(f"server:{self.name}:{i}", "server", status, 0, 0, time.time(),
                                    f"shared serened exited with {rc}: {sig}", sig,
                                    dir=str(s.home)))
        print(f"shared server {i} died ({rc}): {sig}; restarting", flush=True)
        try:
            s.start(fresh=True)
        except Exception as e:
            print(f"shared server {i} restart failed: {e}", flush=True)

    def acquire(self):
        with self.lock:
            candidates = [i for i, s in enumerate(self.servers) if s.alive()]
            if not candidates:
                raise RuntimeError("no shared serened is running")
            i = min(candidates, key=lambda k: self.load[k])
            self.load[i] += 1
            s = self.servers[i]
            return i, s.generation

    def release(self, i):
        with self.lock:
            self.load[i] -= 1

    def server(self, i):
        return self.servers[i]

    @contextlib.contextmanager
    def stalling(self, i):
        stall = [time.monotonic(), None]
        with self.lock:
            self.stalls[i].append(stall)
        try:
            yield
        finally:
            stall[1] = time.monotonic()

    def stalled(self, i, since):
        now = time.monotonic()
        with self.lock:
            return sum(max(0.0, (end or now) - max(start, since)) for start, end in self.stalls[i])

    def watch(self, r, i):
        since = time.monotonic()
        server = self.servers[i]
        r.stack_pids = lambda: (server.pid(),)
        r.stalled = lambda: self.stalled(i, since)
        r.dumping = lambda: self.stalling(i)
        return server

    def crashed_during(self, i, generation):
        with self.lock:
            s = self.servers[i]
            if s.generation != generation:
                return self.crash_info.get((i, generation), "shared serened restarted")
            if s.proc is not None and s.proc.poll() is not None:
                return s.crash_signature()
        return None

    def stop(self):
        self._stop.set()
        if self._monitor:
            self._monitor.join(5)
        findings = list(self.findings)
        for i, s in enumerate(self.servers):
            outcome = s.stop()
            unit = f"server:{self.name}:{i}"
            if outcome and outcome[0] == "stuck":
                findings.append(Result(unit, "server", STUCK, 0, 0, time.time(),
                                       "shared serened did not stop on SIGTERM; stacks saved",
                                       "server stop stuck", dir=str(s.home)))
            elif outcome and outcome[1] not in (0, None):
                findings.append(Result(unit, "server", FAILED, 0, 0, time.time(),
                                       f"shared serened exited with {outcome[1]} on shutdown: "
                                       f"{s.crash_signature()}", s.crash_signature(),
                                       dir=str(s.home)))
            for r in reports.scan_sanitizer_dir(s.san_dir):
                findings.append(Result(unit, "server", FAILED, 0, 0, time.time(),
                                       r.signature, r.signature, dir=str(s.home)))
            if s.datadir is not None:
                shutil.rmtree(s.datadir, ignore_errors=True)
        return findings


class RestartingServer:
    def __init__(self, ctx, home, threads=0, extra_args=(), profile_dir=None):
        self.server = Server(ctx, home, threads=threads, extra_args=extra_args,
                             profile_dir=profile_dir)
        self.exits = []
        self._stopping = threading.Event()
        self._thread = None
        self._ready = threading.Event()

    def start(self):
        self.server.start(fresh=True)
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def _loop(self):
        s = self.server
        while True:
            rc = s.proc.wait()
            self.exits.append(rc)
            if self._stopping.is_set():
                return
            s.generation += 1
            s.log = s.home / f"log.{s.generation}.txt"
            for _ in range(600):
                if self._stopping.is_set():
                    return
                s.proc = proc.spawn(s.argv(), env=s.env(), cwd=s.ctx.root, log_path=s.log)
                if s._wait_ready() or s.proc.poll() is None:
                    break
                self.exits.append(s.proc.returncode)
                time.sleep(0.5)

    @property
    def port(self):
        return self.server.port

    @property
    def datadir(self):
        return self.server.datadir

    def pid(self):
        return self.server.pid()

    def stop(self):
        self._stopping.set()
        outcome = self.server.stop()
        if self._thread:
            self._thread.join(30)
        return outcome
