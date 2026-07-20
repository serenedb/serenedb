#!/usr/bin/env python3
"""rbac_temp_test -- database TEMP privilege enforcement over a real connection.

`CREATE TEMP TABLE` requires the TEMP privilege on the connected database, like
PostgreSQL. This lives here rather than in sqllogictest because the sqllogictest
runner routes CREATE/DDL statements to its default *superuser* connection,
ignoring the `connection ... user=` directive, so it cannot drive a temp create
as a non-superuser role (the same limitation that keeps COPY-from-file out of
the sqllogic suite). psql connects as the actual role, so it can.

Runnable standalone:
    python3 tests/network/rbac_temp_test.py --serened ./build/bin/serened
"""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time


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
        self.datadir = tempfile.mkdtemp(prefix="rbac_temp_")
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen(
            [self.binary, self.datadir,
             f"--listen=postgres://127.0.0.1:{self.port}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 30
        while time.time() < deadline:
            r = subprocess.run(
                ["psql", "-h", "127.0.0.1", "-p", str(self.port),
                 "-U", "postgres", "-d", "postgres", "-c", "SELECT 1"],
                capture_output=True, text=True)
            if r.returncode == 0:
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


def su_exec(port: int, sql: str):
    """Run a control statement as the superuser over loopback (trust)."""
    r = subprocess.run(
        ["psql", "-h", "127.0.0.1", "-p", str(port), "-U", "postgres",
         "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"psql failed ({sql!r}): {r.stderr.strip()}")


def role_create_temp(port: int, role: str, password: str, table: str):
    """CREATE TEMP TABLE as `role`. Returns (ok, stderr)."""
    env = dict(os.environ, PGPASSWORD=password)
    r = subprocess.run(
        ["psql", "-h", "127.0.0.1", "-p", str(port), "-U", role,
         "-d", "postgres", "-v", "ON_ERROR_STOP=1",
         "-c", f"CREATE TEMP TABLE {table} (a int)"],
        capture_output=True, text=True, env=env)
    return r.returncode == 0, r.stderr.strip()


EXPECTED_DENIAL = (
    'permission denied to create temporary tables in database "postgres"')


def run(binary: str) -> int:
    port = free_port()
    srv = Serened(binary, port)
    failures = []
    try:
        srv.start()

        su_exec(port, "CREATE ROLE t_haver LOGIN PASSWORD 'pw'")
        su_exec(port, "CREATE ROLE t_denied LOGIN PASSWORD 'pw'")
        # Remove PUBLIC's implicit TEMP; grant it explicitly only to t_haver.
        su_exec(port, "REVOKE TEMP ON DATABASE postgres FROM PUBLIC")
        su_exec(port, "GRANT TEMP ON DATABASE postgres TO t_haver")

        # t_haver has an explicit grant -> allowed.
        ok, err = role_create_temp(port, "t_haver", "pw", "t_ok")
        if ok:
            print("[OK] t_haver (has TEMP) can CREATE TEMP TABLE")
        else:
            print(f"[FAIL] t_haver denied unexpectedly: {err}")
            failures.append(("t_haver", err))

        # t_denied has no TEMP (PUBLIC's was revoked) -> denied with PG's message.
        ok, err = role_create_temp(port, "t_denied", "pw", "t_no")
        if not ok and EXPECTED_DENIAL in err:
            print("[OK] t_denied (no TEMP) refused with the expected message")
        elif ok:
            print("[FAIL] t_denied was ALLOWED to CREATE TEMP TABLE")
            failures.append(("t_denied", "allowed, expected denial"))
        else:
            print(f"[FAIL] t_denied wrong error: {err}")
            failures.append(("t_denied", f"wrong message: {err}"))
    finally:
        srv.stop()

    if failures:
        print(f"\nFAILED: {len(failures)}")
        for name, why in failures:
            print(f"  {name}: {why}")
        return 1
    print("\nAll RBAC temp cases passed.")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--serened", default="./build/bin/serened",
                    help="path to the serened binary")
    args = ap.parse_args()
    if not os.path.exists(args.serened):
        sys.exit(f"serened not found: {args.serened}")
    sys.exit(run(args.serened))
