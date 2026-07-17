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
             f"--listen=postgres://0.0.0.0:{self.port}",
             "--auth_timeout=600s"],
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
        self.proc = None
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
        # A rule authored in v4-mapped-v6 form (::ffff:x) must still match a v4
        # client -- PG promotes the client to ::ffff:x and retries. Without the
        # promotion the reject is skipped and the client falls open through the
        # 0.0.0.0/0 allow. 127.0.0.9/.10 dodge the loopback safety rule (only
        # 127.0.0.1/32), so the authored rules decide.
        "name": "v4-mapped-rule-matches-v4-client",
        "hba": ("host all all ::ffff:127.0.0.9/128 reject\n"
                "host all all 0.0.0.0/0             trust\n"),
        "expect": {"127.0.0.9": REJECTED, "127.0.0.10": TRUST},
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


# --- axis matrix: user / database / method / +role / reject-at-connect -------
#
# These exercise paths the in-process gtest can only fake: that the startup
# packet's user/database fields reach ClientInfo over the wire, that a matched
# password method actually makes the server send an auth challenge
# (AUTH_REQUESTED), that +role membership is resolved against the live catalog,
# and that an unsupported method rejects at connect. Each probe carries a
# (source_ip, user, database) and an expected outcome. `setup` runs first as a
# superuser (loopback) to create any roles/grants the case needs.
MATCH_CASES = [
    {
        "name": "method-selection-scram",
        # A scram rule must make the server send an auth challenge, not trust.
        # (Distinct from trust/reject -- the whole point of HBA method choice.)
        "setup": "CREATE ROLE m_alice LOGIN PASSWORD 'pw';",
        "hba": ("host all all 127.0.0.2/32 scram-sha-256\n"
                "host all all 0.0.0.0/0    reject\n"),
        "probes": [
            ("127.0.0.2", "m_alice", "postgres", AUTH_REQUESTED),
            ("127.5.5.5", "m_alice", "postgres", REJECTED),
        ],
    },
    {
        "name": "method-selection-md5",
        # An md5 rule against a role whose stored secret is an md5 verifier must
        # drive an md5 challenge (the server can authenticate md5 storage --
        # unlike a scram line, which cannot). The probe only checks that the
        # challenge is issued, so any well-formed md5<hex> verifier works here;
        # the verify math itself is covered by the NetworkPgMd5 gtests.
        "setup": ("CREATE ROLE m_md5 LOGIN PASSWORD "
                  "'md52c1e2d5c8b0f8f3a1f8c6b7e9d0a4c3b';"),
        "hba": ("host all all 127.0.0.2/32 md5\n"
                "host all all 0.0.0.0/0    reject\n"),
        "probes": [
            ("127.0.0.2", "m_md5", "postgres", AUTH_REQUESTED),
            ("127.5.5.5", "m_md5", "postgres", REJECTED),
        ],
    },
    {
        "name": "user-axis",
        # Match on the role field: only u_alice is trusted from this net.
        "setup": "CREATE ROLE u_alice LOGIN; CREATE ROLE u_bob LOGIN;",
        "hba": ("host all u_alice 127.0.0.0/8 trust\n"
                "host all all     0.0.0.0/0   reject\n"),
        "probes": [
            ("127.0.0.9", "u_alice", "postgres", TRUST),
            ("127.0.0.9", "u_bob", "postgres", REJECTED),
        ],
    },
    {
        "name": "database-axis",
        # Match on the database field.
        "setup": "CREATE DATABASE reports; CREATE ROLE d_svc LOGIN;",
        "hba": ("host reports all 127.0.0.0/8 trust\n"
                "host all     all 0.0.0.0/0   reject\n"),
        "probes": [
            ("127.0.0.9", "d_svc", "reports", TRUST),
            ("127.0.0.9", "d_svc", "postgres", REJECTED),
        ],
    },
    {
        "name": "role-membership",
        # +analysts matches any (in)direct member; resolved live from the catalog.
        "setup": ("CREATE ROLE analysts; "
                  "CREATE ROLE r_in LOGIN IN ROLE analysts; "
                  "CREATE ROLE r_out LOGIN;"),
        "hba": ("host all +analysts 127.0.0.0/8 trust\n"
                "host all all       0.0.0.0/0   reject\n"),
        "probes": [
            ("127.0.0.9", "r_in", "postgres", TRUST),
            ("127.0.0.9", "r_out", "postgres", REJECTED),
        ],
    },
    {
        "name": "sameuser",
        # sameuser matches when the target database name equals the role name.
        "setup": "CREATE ROLE s_dev LOGIN; CREATE DATABASE s_dev;",
        "hba": ("host sameuser all 127.0.0.0/8 trust\n"
                "host all      all 0.0.0.0/0   reject\n"),
        "probes": [
            ("127.0.0.9", "s_dev", "s_dev", TRUST),      # db == role
            ("127.0.0.9", "s_dev", "postgres", REJECTED),  # db != role
        ],
    },
    {
        "name": "reject-at-connect-ldap",
        # An unsupported method parses fine but a matched connection is refused
        # (not trusted, not an auth challenge) -- surfaces as REJECTED here.
        "setup": "CREATE ROLE l_user LOGIN;",
        "hba": "host all all 127.0.0.0/8 ldap ldapserver=nope\n",
        "probes": [
            ("127.0.0.9", "l_user", "postgres", REJECTED),
        ],
    },
]


def psql_exec(port: int, sql: str, user: str = "postgres"):
    """Run a control statement as a superuser over the loopback (always admitted
    by the safety rule). Raises on non-zero exit."""
    proc = subprocess.run(
        ["psql", "-h", "127.0.0.1", "-p", str(port), "-U", user,
         "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"psql failed ({sql!r}): {proc.stderr.strip()}")


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

        # Address/CIDR axis: vary the source IP, all-users/all-dbs, trust/reject.
        for case in MASK_CASES:
            set_hba(port, case["hba"])
            for src, want in case["expect"].items():
                got = probe("127.0.0.1", port, src)
                ok = got == want
                print(f"[{'OK' if ok else 'FAIL'}] {case['name']}: "
                      f"from {src} -> {got} (want {want})")
                if not ok:
                    failures.append((case["name"], f"{src}: {got}!={want}"))

        # user / database / method / +role / reject-at-connect axes.
        for case in MATCH_CASES:
            if setup := case.get("setup"):
                psql_exec(port, setup)
            set_hba(port, case["hba"])
            for src, user, db, want in case["probes"]:
                got = probe("127.0.0.1", port, src, user=user, database=db)
                ok = got == want
                print(f"[{'OK' if ok else 'FAIL'}] {case['name']}: "
                      f"{user}@{src}/{db} -> {got} (want {want})")
                if not ok:
                    failures.append(
                        (case["name"], f"{user}@{src}/{db}: {got}!={want}"))
        # Persistence across restart is covered by the recovery suite
        # (tests/sqllogic/recovery/hba_persist.test), which crashes serened and
        # reads the ruleset back from pg_hba_file_rules -- the purpose-built
        # harness for crash-durability.
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
