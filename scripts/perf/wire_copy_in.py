#!/usr/bin/env python3
# Raw pg-wire COPY FROM STDIN ingest-throughput probe, any type x {binary,text}.
# Capture-replay: capture the engine's OWN "COPY (SELECT * FROM t_<typ>) TO STDOUT
# (FORMAT ...)" bytes, then stream them straight back into "COPY t_<typ>_in FROM
# STDIN (FORMAT ...)" and time the load to CommandComplete. Capturing the engine's
# own output keeps the payload valid for every engine and every type without any
# hand-encoding. COPY-in is single-threaded by nature, so there is no ordered/
# threads variant here. The CopyData frame size is the wire-feeder lever.
#
# usage: wire_copy_in.py PORT SRC_TABLE FMT REPS FRAME_BYTES LABEL
# env: RUN_ONE_USER/RUN_ONE_DB/PGPASSWORD (wire_auth handles trust/md5/SCRAM).
# Output (stdout, one line): LABEL <median_s> <MB> <MB/s> <status>
# SRC_TABLE is a (small) pre-built source table -- the bench sizes it at
# COPYIN_ROWS via a parallel CTAS, NOT a LIMIT (LIMIT forces a single-threaded
# scan).
import os
import socket
import statistics
import struct
import sys
import time

import wire_auth  # same dir; sys.path[0] is the script's directory

PORT = int(sys.argv[1])
SRC = sys.argv[2]
FMT = sys.argv[3]  # "binary" | "text" | "csv"
REPS = int(sys.argv[4])
FRAME = int(sys.argv[5])
LABEL = sys.argv[6]
USER, DB, PASSWORD = wire_auth.env_creds()
DST = f"{SRC}_copyin"
TIMEOUT = float(os.environ.get("PERF_SER_TIMEOUT", "120"))


def recv_exact(s, n):
    b = b""
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise RuntimeError("eof")
        b += c
    return b


def read_msg(s):
    t = recv_exact(s, 1)
    (ln,) = struct.unpack("!I", recv_exact(s, 4))
    return t, recv_exact(s, ln - 4)


def err_msg(p):
    off = 0
    while off < len(p) and p[off : off + 1] != b"\x00":
        code = p[off : off + 1]
        end = p.index(b"\x00", off + 1)
        if code == b"M":
            return p[off + 1 : end].decode("ascii", "replace")
        off = end + 1
    return "error"


def wait_for(s, types):
    while True:
        t, p = read_msg(s)
        if t == b"E":
            raise RuntimeError(err_msg(p))
        if t in types:
            return t, p


def simple(s, sql):
    s.sendall(b"Q" + struct.pack("!i", 5 + len(sql)) + sql.encode() + b"\x00")
    wait_for(s, (b"Z",))


def capture(s):
    sql = f"COPY (SELECT * FROM {SRC}) TO STDOUT (FORMAT {FMT})"
    s.sendall(b"Q" + struct.pack("!i", 5 + len(sql)) + sql.encode() + b"\x00")
    wait_for(s, (b"H",))  # CopyOutResponse
    out = []
    while True:
        t, p = read_msg(s)
        if t == b"d":
            out.append(p)
        elif t == b"E":
            raise RuntimeError(err_msg(p))
        elif t == b"Z":  # after CopyDone + CommandComplete
            break
    return b"".join(out)


def load_once(s, blob):
    simple(s, f"truncate {DST}")
    sql = f"COPY {DST} FROM STDIN (FORMAT {FMT})"
    s.sendall(b"Q" + struct.pack("!i", 5 + len(sql)) + sql.encode() + b"\x00")
    wait_for(s, (b"G",))  # CopyInResponse
    start = time.perf_counter()
    for off in range(0, len(blob), FRAME):
        chunk = blob[off : off + FRAME]
        s.sendall(b"d" + struct.pack("!i", 4 + len(chunk)) + chunk)
    s.sendall(b"c" + struct.pack("!i", 4))  # CopyDone
    wait_for(s, (b"C",))  # CommandComplete
    wait_for(s, (b"Z",))
    return time.perf_counter() - start


timings = []
mb = 0.0
err = None
try:
    s = socket.create_connection(("127.0.0.1", PORT), timeout=TIMEOUT)
    s.settimeout(TIMEOUT)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    wire_auth.startup(s, USER, DB, PASSWORD)
    simple(s, f"drop table if exists {DST}")
    simple(s, f"create table {DST} as select * from {SRC} where 1=0")
    blob = capture(s)
    mb = len(blob) / 1e6
    if not blob:
        raise RuntimeError("empty capture")
    for _ in range(REPS):
        timings.append(load_once(s, blob))
    s.close()
except Exception as exc:
    err = f"{type(exc).__name__}: {exc}"

if err or not timings:
    msg = str(err or "no timings").replace("\t", " ").replace("\n", " ").replace("\r", " ")
    print(f"{LABEL}\t0\t{mb:.1f}\t0\tERROR: {msg}")
else:
    med = statistics.median(timings)
    print(f"{LABEL}\t{med:.3f}\t{mb:.1f}\t{mb / med:.0f}\tok")
