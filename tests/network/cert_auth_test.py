#!/usr/bin/env python3
"""cert_auth_test -- end-to-end HBA `cert` authentication over TLS.

cert compares the verified client certificate's CN with the requested role, so
it needs a real TLS handshake with a client certificate -- the sqllogic harness
can't do that. This test builds a throwaway CA, a server cert, and two client
certs (CN matching a role, CN mismatching), starts serened with a
verify-ca TLS listener, sets `hostssl all all 0.0.0.0/0 cert`, and asserts:

  - a client cert whose CN equals the role authenticates,
  - a client cert whose CN differs gets PG's `certificate authentication
    failed for user "x"`,
  - connecting with NO client cert is refused,
  - the superuser still gets in over loopback (safety rule, trust).

Requires the `openssl` CLI. Runnable standalone:
    python3 tests/network/cert_auth_test.py --serened ./build/bin/serened
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
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def openssl(*args, cwd):
    r = subprocess.run(["openssl", *args], cwd=cwd, capture_output=True,
                       text=True)
    if r.returncode != 0:
        raise RuntimeError(f"openssl {' '.join(args)} failed: {r.stderr}")


def make_pki(d: str):
    """CA + server cert (CN=localhost) + client certs (CN=cert_ok / cert_bad)."""
    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout",
            "ca.key", "-out", "ca.crt", "-days", "1", "-subj", "/CN=Test CA",
            cwd=d)
    for name, cn in (("server", "localhost"), ("cert_ok", "cert_ok"),
                     ("cert_bad", "cert_bad")):
        openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout",
                f"{name}.key", "-out", f"{name}.csr", "-subj", f"/CN={cn}",
                cwd=d)
        openssl("x509", "-req", "-in", f"{name}.csr", "-CA", "ca.crt",
                "-CAkey", "ca.key", "-CAcreateserial", "-out", f"{name}.crt",
                "-days", "1", cwd=d)


class Serened:
    def __init__(self, binary, port, pki):
        self.binary, self.port, self.pki = binary, port, pki
        self.datadir = tempfile.mkdtemp(prefix="cert-auth-data-")
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen(
            [self.binary, self.datadir,
             f"--listen=postgres://127.0.0.1:{self.port}?sslmode=verify-ca",
             f"--tls_cert={self.pki}/server.crt",
             f"--tls_key={self.pki}/server.key",
             f"--tls_ca={self.pki}/ca.crt"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 30
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError("serened exited during startup")
            # Control connection: loopback + client cert (verify-ca requires one
            # for every TLS client), admitted by the superuser safety rule.
            if self._ctl("SELECT 1").returncode == 0:
                return
            time.sleep(0.3)
        raise RuntimeError("serened did not come up in 30s")

    def _ctl(self, sql, user="postgres", cn="cert_ok"):
        env = dict(os.environ,
                   PGSSLMODE="verify-ca",
                   PGSSLROOTCERT=f"{self.pki}/ca.crt",
                   PGSSLCERT=f"{self.pki}/{cn}.crt",
                   PGSSLKEY=f"{self.pki}/{cn}.key")
        return subprocess.run(
            ["psql", "-h", "127.0.0.1", "-p", str(self.port), "-U", user,
             "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-qtA", "-c", sql],
            capture_output=True, text=True, env=env)

    def stop(self):
        if self.proc:
            self.proc.kill()
            self.proc.wait()
        shutil.rmtree(self.datadir, ignore_errors=True)


def psql_cert(pki, port, user, cn, sql, present_cert=True):
    """psql with a client cert (or none), verifying the server against the CA."""
    env = dict(os.environ, PGSSLMODE="verify-ca",
               PGSSLROOTCERT=f"{pki}/ca.crt")
    if present_cert:
        env["PGSSLCERT"] = f"{pki}/{cn}.crt"
        env["PGSSLKEY"] = f"{pki}/{cn}.key"
    else:
        # Point at nonexistent files so libpq sends no client cert.
        env["PGSSLCERT"] = f"{pki}/none.crt"
        env["PGSSLKEY"] = f"{pki}/none.key"
    return subprocess.run(
        ["psql", "-h", "127.0.0.1", "-p", str(port), "-U", user,
         "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-qtA", "-c", sql],
        capture_output=True, text=True, env=env)


def run(binary: str) -> int:
    if shutil.which("openssl") is None:
        print("[SKIP] openssl CLI not found")
        return 0

    pki = tempfile.mkdtemp(prefix="cert-auth-pki-")
    make_pki(pki)
    port = free_port()
    srv = Serened(binary, port, pki)
    failures = []

    def check(name, ok, detail=""):
        print(f"[{'OK' if ok else 'FAIL'}] {name}"
              + (f": {detail}" if detail and not ok else ""))
        if not ok:
            failures.append(name)

    try:
        srv.start()
        srv._ctl("CREATE ROLE cert_ok LOGIN")
        srv._ctl("CREATE ROLE cert_bad LOGIN")
        # Superuser stays on the loopback safety rule (trust); everyone else
        # must present a matching client certificate.
        srv._ctl("SET hba = $$"
                 "host all postgres 127.0.0.1/32 trust\n"
                 "hostssl all all 0.0.0.0/0 cert\n"
                 "host all all 0.0.0.0/0 reject$$")

        r = psql_cert(pki, port, "cert_ok", "cert_ok", "SELECT current_user")
        check("matching client-cert CN admits the role",
              r.returncode == 0 and r.stdout.strip() == "cert_ok",
              r.stderr.strip())

        r = psql_cert(pki, port, "cert_bad", "cert_ok", "SELECT 1")
        check("CN != requested role is rejected",
              r.returncode != 0 and
              'certificate authentication failed for user "cert_bad"'
              in r.stderr, r.stderr.strip())

        r = psql_cert(pki, port, "cert_ok", "cert_ok", "SELECT 1",
                      present_cert=False)
        check("no client certificate is refused", r.returncode != 0,
              r.stderr.strip())

        r = srv._ctl("SELECT current_user")  # superuser over loopback
        check("superuser safety rule still admits",
              r.returncode == 0 and r.stdout.strip() == "postgres",
              r.stderr.strip())
    finally:
        srv.stop()
        shutil.rmtree(pki, ignore_errors=True)

    if failures:
        print(f"[cert-auth] {len(failures)} failure(s): {failures}")
        return 1
    print("[cert-auth] all checks passed")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--serened", default="./build/bin/serened")
    args = ap.parse_args()
    sys.exit(run(os.path.abspath(args.serened)))


if __name__ == "__main__":
    main()
