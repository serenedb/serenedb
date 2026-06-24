#!/usr/bin/env python3
# Raw pg-wire COPY FROM STDIN (FORMAT BINARY) ingest throughput probe. Streams a
# generated PGCOPY blob to the server in CopyData frames of a chosen size and
# times the load to CommandComplete. The CopyData frame size is the lever: large
# frames stress the wire feeder (whole-frame assembly/linearization vs streaming
# straight to the CopyInBridge).
#
# usage: wire_copy_ingest.py PORT ROWS FRAME_BYTES REPS LABEL
# env: RUN_ONE_USER / RUN_ONE_DB (default postgres/postgres), trust only.
# Output (stdout): LABEL <median_s> <MB> <MB/s> <Mrows/s> <status>
import os
import socket
import statistics
import struct
import sys
import time

PORT = int(sys.argv[1])
ROWS = int(sys.argv[2])
FRAME = int(sys.argv[3])
REPS = int(sys.argv[4])
LABEL = sys.argv[5]
USER = os.environ.get("RUN_ONE_USER") or "postgres"
DB = os.environ.get("RUN_ONE_DB") or "postgres"

PGCOPY = b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0)  # signature, flags, ext
TRAILER = b"\xff\xff"


def make_blob(rows):
    # One bigint column: int16 field-count=1, int32 len=8, int64 value.
    row = b"".join(
        struct.pack("!hiq", 1, 8, i) for i in range(rows)
    )
    return PGCOPY + row + TRAILER


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


def wait_for(s, types):
    while True:
        t, p = read_msg(s)
        if t == b"E":
            raise RuntimeError("server error: " + p.decode("ascii", "replace"))
        if t in types:
            return t, p


def connect():
    s = socket.create_connection(("127.0.0.1", PORT), timeout=120)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    params = b"user\x00" + USER.encode() + b"\x00database\x00" + DB.encode() + b"\x00\x00"
    s.sendall(struct.pack("!ii", 8 + len(params), 196608) + params)
    wait_for(s, (b"Z",))
    return s


def simple(s, sql):
    s.sendall(b"Q" + struct.pack("!i", 5 + len(sql)) + sql.encode() + b"\x00")
    wait_for(s, (b"Z",))


def copy_once(s, blob):
    simple(s, "truncate bench_copyin")
    s.sendall(b"Q" + struct.pack("!i", 5 + len("copy bench_copyin from stdin (format binary)"))
              + b"copy bench_copyin from stdin (format binary)\x00")
    wait_for(s, (b"G",))  # CopyInResponse
    start = time.perf_counter()
    for off in range(0, len(blob), FRAME):
        chunk = blob[off : off + FRAME]
        s.sendall(b"d" + struct.pack("!i", 4 + len(chunk)) + chunk)
    s.sendall(b"c" + struct.pack("!i", 4))  # CopyDone
    wait_for(s, (b"C",))  # CommandComplete
    wait_for(s, (b"Z",))
    return time.perf_counter() - start


blob = make_blob(ROWS)
mb = len(blob) / 1e6
timings, err = [], None
try:
    s = connect()
    simple(s, "drop table if exists bench_copyin")
    simple(s, "create table bench_copyin(a bigint)")
    for _ in range(REPS):
        timings.append(copy_once(s, blob))
    s.close()
except Exception as exc:
    err = f"{type(exc).__name__}: {exc}"

if err or not timings:
    msg = str(err or "no timings").replace("\t", " ").replace("\n", " ")
    print(f"{LABEL}\t0\t{mb:.1f}\t0\t0\tERROR: {msg}")
else:
    med = statistics.median(timings)
    print(f"{LABEL}\t{med:.3f}\t{mb:.1f}\t{mb / med:.0f}\t{ROWS / med / 1e6:.2f}\tok")
