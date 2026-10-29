#!/usr/bin/env python3
"""proxy_source_test -- HBA decides on the PROXY-protocol source address.

A listener with proxy_protocol=require sits behind a proxy on the same host, so
every TCP peer is 127.0.0.1. The client's address is the source in the PROXY
header: a remote source must not get the loopback trust rules, and a header
without a source must not match any host rule.

Runnable standalone:
    python3 tests/drivers/network/proxy_source_test.py --serened ./build/bin/serened
"""

import argparse
import os
import socket
import struct
import sys

from hba_mask_test import AUTH_REQUESTED, BROKEN, REJECTED, TRUST, _recvn, _startup

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "harness"))

from serened import Serened  # noqa: E402


def probe(port: int, header: bytes, timeout: float = 5.0) -> str:
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    except OSError:
        return BROKEN
    try:
        s.sendall(header + _startup("postgres", "postgres"))
        tag = s.recv(1)
        if not tag:
            return BROKEN
        if tag == b"E":
            return REJECTED
        if tag == b"R":
            _recvn(s, 4)
            sub = struct.unpack("!I", _recvn(s, 4))[0]
            return TRUST if sub == 0 else AUTH_REQUESTED
        return BROKEN
    except (socket.timeout, ConnectionError):
        return BROKEN
    finally:
        s.close()


def v1(source: str, port: int) -> bytes:
    return f"PROXY TCP4 {source} 127.0.0.1 40000 {port}\r\n".encode()


def run(binary: str) -> int:
    srv = Serened(binary, prefix="proxy_source_",
                  ready=lambda host, p: probe(p, v1("127.0.0.1", p)) != BROKEN)
    srv.extra_args.append(
        f"--listen=postgres://127.0.0.1:{srv.port}?proxy_protocol=require")
    srv.start()
    failures = []
    try:
        cases = [
            ("loopback source", v1("127.0.0.1", srv.port), {TRUST}),
            ("remote source", v1("203.0.113.7", srv.port),
             {AUTH_REQUESTED, REJECTED}),
            ("unknown source", b"PROXY UNKNOWN\r\n", {AUTH_REQUESTED, REJECTED}),
        ]
        for name, header, want in cases:
            got = probe(srv.port, header)
            ok = got in want
            print(f"[{'OK' if ok else 'FAIL'}] {name} -> {got} "
                  f"(want {sorted(want)})")
            if not ok:
                failures.append(name)
    finally:
        srv.stop()
    if failures:
        print(f"\nFAILED: {', '.join(failures)}")
        return 1
    print("\nAll PROXY source cases passed.")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--serened", default="./build/bin/serened",
                    help="path to the serened binary")
    args = ap.parse_args()
    if not os.path.exists(args.serened):
        sys.exit(f"serened not found: {args.serened}")
    sys.exit(run(args.serened))
