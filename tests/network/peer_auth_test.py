#!/usr/bin/env python3
"""peer_auth_test -- end-to-end HBA `peer` authentication over a unix socket.

peer compares the client process's OS user (SO_PEERCRED) with the requested
role name, so it can only be exercised with a real unix-socket connection --
the sqllogic harness is TCP-only. Starts serened with both a TCP listener (the
superuser control connection; the force-prepended safety rule always admits it)
and a unix listener, sets a `local all all peer` ruleset, and asserts:

  - a role named after the OS user authenticates over the unix socket,
  - any other role gets PG's `peer authentication failed for user "x"`,
  - the bootstrap superuser still gets in over unix (safety rule, trust),
  - `local all all trust` admits everyone (unix + trust sanity).

Runnable standalone:
    python3 tests/network/peer_auth_test.py --serened ./build/bin/serened
"""

import argparse
import getpass
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Serened:
    def __init__(self, binary: str, tcp_port: int, sock_dir: str,
                 unix_port: int):
        self.binary = binary
        self.tcp_port = tcp_port
        self.sock_dir = sock_dir
        self.unix_port = unix_port
        self.datadir = tempfile.mkdtemp(prefix="peer-auth-data-")
        self.proc = None

    def start(self):
        listen = (f"postgres://127.0.0.1:{self.tcp_port},"
                  f"postgres:///{self.sock_dir}?port={self.unix_port}")
        self.proc = subprocess.Popen(
            [self.binary, self.datadir, f"--listen={listen}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 30
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError("serened exited during startup")
            r = subprocess.run(
                ["psql", "-h", "127.0.0.1", "-p", str(self.tcp_port),
                 "-U", "postgres", "-d", "postgres", "-c", "SELECT 1"],
                capture_output=True, text=True)
            if r.returncode == 0:
                return
            time.sleep(0.3)
        raise RuntimeError("serened did not come up in 30s")

    def stop(self):
        if self.proc:
            self.proc.kill()
            self.proc.wait()
        shutil.rmtree(self.datadir, ignore_errors=True)


def psql(host: str, port: int, user: str, sql: str):
    return subprocess.run(
        ["psql", "-h", host, "-p", str(port), "-U", user, "-d", "postgres",
         "-v", "ON_ERROR_STOP=1", "-qtA", "-c", sql],
        capture_output=True, text=True)


def control(port: int, sql: str):
    r = psql("127.0.0.1", port, "postgres", sql)
    if r.returncode != 0:
        raise RuntimeError(f"control psql failed ({sql!r}): {r.stderr.strip()}")
    return r.stdout.strip()


def set_hba(port: int, ruleset: str):
    control(port, f"SET hba = $${ruleset}$$")


def run(binary: str) -> int:
    os_user = getpass.getuser()
    if os_user == "postgres":
        # The safety rule trusts the bootstrap superuser over unix, which would
        # mask the peer path for a same-named role.
        print("[SKIP] OS user is 'postgres'; peer path is shadowed by the "
              "superuser safety rule")
        return 0

    tcp_port = free_port()
    unix_port = free_port()
    sock_dir = tempfile.mkdtemp(prefix="peer-auth-sock-")
    srv = Serened(binary, tcp_port, sock_dir, unix_port)
    failures = []

    def check(name: str, ok: bool, detail: str = ""):
        print(f"[{'OK' if ok else 'FAIL'}] {name}" + (f": {detail}" if detail and not ok else ""))
        if not ok:
            failures.append(name)

    try:
        srv.start()
        control(tcp_port, f'CREATE ROLE "{os_user}" LOGIN')
        control(tcp_port, "CREATE ROLE peer_wrong LOGIN")
        set_hba(tcp_port, "local all all peer\n"
                          "host all all 0.0.0.0/0 trust\n"
                          "host all all ::0/0 trust")

        # Matching OS user authenticates and is the current_user.
        r = psql(sock_dir, unix_port, os_user, "SELECT current_user")
        check("peer match admits the same-named role",
              r.returncode == 0 and r.stdout.strip() == os_user,
              r.stderr.strip())

        # Any other role fails with PG's exact message.
        r = psql(sock_dir, unix_port, "peer_wrong", "SELECT 1")
        check("peer mismatch is rejected",
              r.returncode != 0 and
              'peer authentication failed for user "peer_wrong"' in r.stderr,
              r.stderr.strip())

        # The bootstrap superuser rides the force-prepended safety rule.
        r = psql(sock_dir, unix_port, "postgres", "SELECT 1")
        check("superuser safety rule still admits over unix",
              r.returncode == 0, r.stderr.strip())

        # Sanity: local trust admits everyone over the unix socket.
        set_hba(tcp_port, "local all all trust\n"
                          "host all all 0.0.0.0/0 trust\n"
                          "host all all ::0/0 trust")
        r = psql(sock_dir, unix_port, "peer_wrong", "SELECT 1")
        check("local trust admits a non-matching role", r.returncode == 0,
              r.stderr.strip())
    finally:
        srv.stop()
        shutil.rmtree(sock_dir, ignore_errors=True)

    if failures:
        print(f"[peer-auth] {len(failures)} failure(s): {failures}")
        return 1
    print("[peer-auth] all checks passed")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--serened", default="./build/bin/serened",
                    help="path to the serened binary")
    args = ap.parse_args()
    sys.exit(run(os.path.abspath(args.serened)))


if __name__ == "__main__":
    main()
