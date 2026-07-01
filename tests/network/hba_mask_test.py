#!/usr/bin/env python3
"""hba_mask_test -- end-to-end HBA CIDR/mask matching over real sockets.

Exercises the full accept -> remote_endpoint -> HBA Decide path by connecting to
serened FROM different loopback source addresses (127.0.0.0/8 all route to the
local host, so no docker/routing is needed) and asserting the auth outcome the
HBA ruleset dictates for each source IP.

libpq/psql cannot bind a client source address, so this drives a minimal
pg-wire startup directly over a source-bound socket and classifies the server's
first auth reply as TRUST / AUTH_REQUESTED / REJECTED.

Runnable standalone:
    python3 tests/network/hba_mask_test.py --serened ./build/bin/serened
It starts its own serened on a free port with --listen=0.0.0.0, sets the HBA
ruleset over a superuser (loopback) connection, then runs the source-IP matrix.
"""

import argparse
import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time

# --- minimal pg-wire client -------------------------------------------------

PROTO_V3 = 196608  # 0x00030000


def _startup(user: str, database: str) -> bytes:
    body = struct.pack("!I", PROTO_V3)
    for k, v in (("user", user), ("database", database)):
        body += k.encode() + b"\x00" + v.encode() + b"\x00"
    body += b"\x00"
    return struct.pack("!I", len(body) + 4) + body


# What the server's first post-startup message tells us.
TRUST = "trust"                 # AuthenticationOk immediately -> HBA said trust
AUTH_REQUESTED = "auth"         # server asked for a password/SASL -> a method rule
REJECTED = "rejected"           # ErrorResponse (FATAL) -> HBA reject / no match
BROKEN = "broken"               # connection dropped with no readable reply


def probe(server_host: str, port: int, source_ip: str,
          user: str = "postgres", database: str = "postgres",
          timeout: float = 5.0) -> str:
    """Connect from `source_ip` and classify the first auth reply."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.bind((source_ip, 0))
    except OSError as e:
        raise RuntimeError(f"cannot bind source {source_ip}: {e}") from e
    try:
        s.connect((server_host, port))
        s.sendall(_startup(user, database))
        tag = s.recv(1)
        if not tag:
            return BROKEN
        if tag == b"E":            # ErrorResponse
            return REJECTED
        if tag == b"R":            # Authentication*
            length = struct.unpack("!I", _recvn(s, 4))[0]
            sub = struct.unpack("!I", _recvn(s, 4))[0]
            # 0 = AuthenticationOk (trust); anything else = a method challenge.
            return TRUST if sub == 0 else AUTH_REQUESTED
        return BROKEN
    except (socket.timeout, ConnectionError):
        return BROKEN
    finally:
        s.close()


def _recvn(s: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("short read")
        buf += chunk
    return buf


# --- server lifecycle -------------------------------------------------------

def free_port() -> int:
    s = socket.socket()
    s.bind(("", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class Serened:
    def __init__(self, binary: str, port: int):
        self.binary = binary
        self.port = port
        self.datadir = tempfile.mkdtemp(prefix="hba_mask_")
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen(
            [self.binary, self.datadir,
             f"--listen=postgres://0.0.0.0:{self.port}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 30
        while time.time() < deadline:
            if probe("127.0.0.1", self.port, "127.0.0.1") != BROKEN:
                return
            if self.proc.poll() is not None:
                raise RuntimeError("serened exited during startup")
            time.sleep(0.3)
        raise RuntimeError("serened did not come up in 30s")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        shutil.rmtree(self.datadir, ignore_errors=True)


# --- the mask matrix --------------------------------------------------------

# Each case: an HBA ruleset (whole-string, as the GUC takes it) + the expected
# outcome per source IP. Rules use trust vs reject so the auth reply is
# unambiguous (TRUST vs REJECTED) without needing a password.
MASK_CASES = [
    {
        "name": "host-32-exact",
        "hba": ("host all all 127.0.0.1/32 trust\n"
                "host all all 0.0.0.0/0   reject\n"),
        "expect": {"127.0.0.1": TRUST, "127.0.0.2": REJECTED,
                   "127.5.5.5": REJECTED},
    },
    {
        "name": "host-8-broad",
        "hba": ("host all all 127.0.0.0/8 trust\n"
                "host all all 0.0.0.0/0   reject\n"),
        "expect": {"127.0.0.1": TRUST, "127.0.0.2": TRUST,
                   "127.255.255.254": TRUST},
    },
    {
        "name": "host-16-boundary",
        "hba": ("host all all 127.0.0.0/16 trust\n"
                "host all all 0.0.0.0/0    reject\n"),
        "expect": {"127.0.0.1": TRUST, "127.0.255.254": TRUST,
                   "127.1.0.1": REJECTED},
    },
    {
        "name": "first-match-wins",
        "hba": ("host all all 127.0.0.5/32 reject\n"
                "host all all 127.0.0.0/8  trust\n"),
        "expect": {"127.0.0.5": REJECTED, "127.0.0.6": TRUST},
    },
    {
        "name": "dotted-netmask-form",  # bare IP + separate mask column (PG form)
        "hba": ("host all all 127.0.0.0 255.255.255.0 trust\n"
                "host all all 0.0.0.0/0                reject\n"),
        "expect": {"127.0.0.9": TRUST, "127.0.1.9": REJECTED},
    },
    {
        # 127.5.5.5 is not covered by the force-prepended safety rule
        # (host 127.0.0.1/32 trust), so with no matching user rule it falls
        # through to implicit reject. (127.0.0.1 itself can never be rejected --
        # that is the anti-lockout guarantee.)
        "name": "implicit-reject-no-match",
        "hba": "host all all 10.0.0.0/8 trust\n",
        "expect": {"127.5.5.5": REJECTED},
    },
]


def set_hba(port: int, ruleset: str):
    """Set the HBA ruleset over a superuser loopback connection via `SET hba`.

    Uses psql from 127.0.0.1, which the force-prepended safety rule always
    admits, so the control connection never locks itself out while testing a
    restrictive ruleset. Raises on a non-zero psql exit (e.g. a parse error).
    """
    dollar = f"$${ruleset}$$"
    proc = subprocess.run(
        ["psql", "-h", "127.0.0.1", "-p", str(port), "-U", "postgres",
         "-d", "postgres", "-v", "ON_ERROR_STOP=1",
         "-c", f"SET hba = {dollar}"],
        capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"SET hba failed: {proc.stderr.strip()}")


def run(binary: str) -> int:
    port = free_port()
    srv = Serened(binary, port)
    failures = []
    try:
        srv.start()
        for case in MASK_CASES:
            try:
                set_hba(port, case["hba"])
            except NotImplementedError as e:
                print(f"SKIP {case['name']}: {e}")
                failures.append((case["name"], "not-wired"))
                continue
            for src, want in case["expect"].items():
                got = probe("127.0.0.1", port, src)
                ok = got == want
                print(f"[{'OK' if ok else 'FAIL'}] {case['name']}: "
                      f"from {src} -> {got} (want {want})")
                if not ok:
                    failures.append((case["name"], f"{src}: {got}!={want}"))
    finally:
        srv.stop()

    if failures:
        print(f"\nFAILED: {len(failures)}")
        for name, why in failures:
            print(f"  {name}: {why}")
        return 1
    print("\nAll HBA mask cases passed.")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--serened", default="./build/bin/serened",
                    help="path to the serened binary")
    args = ap.parse_args()
    if not os.path.exists(args.serened):
        sys.exit(f"serened not found: {args.serened}")
    sys.exit(run(args.serened))
