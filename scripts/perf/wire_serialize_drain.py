#!/usr/bin/env python3
# Raw pg-wire drain client for serialization throughput. Speaks the extended
# protocol so the result format is selectable (Bind result-format code 0/1) --
# psql/pgbench/psycopg2 are text-only AND parse rows client-side, which caps
# them near 400 MB/s and masks server-side serialization differences. This
# client just counts bytes (multi-GB/s), so the server stays the bottleneck.
#
# usage: wire_serialize_drain.py PORT MODE REPS SETUP QUERY LABEL
# MODE = text|binary       -- drain the SELECT result (Bind result-format 0/1).
#      | copy_text|copy_binary -- drain COPY (QUERY) TO STDOUT (FORMAT text|binary)
#        as a simple query, so SELECT-serialize vs COPY-serialize are measured the
#        same way (raw bytes to RFQ, server-bound -- neither decodes rows).
# SETUP ("" = none) runs as a simple-protocol Query first (e.g. SET ...).
# Output (stdout, one line): LABEL <median_s> <MB> <MB/s> <status>
import socket
import statistics
import struct
import sys
import time

PORT = int(sys.argv[1])
MODE = sys.argv[2]
COPY = MODE.startswith("copy_")
FMT = 1 if MODE.endswith("binary") else 0
REPS = int(sys.argv[3])
SETUP = sys.argv[4]
QUERY = sys.argv[5]
LABEL = sys.argv[6]

RFQ_TAIL = b"\x00\x00\x00\x05"


def error_message(payload):
    # ErrorResponse payload: (<field-code byte><cstring>)* terminated by \0.
    off = 0
    while off < len(payload) and payload[off : off + 1] != b"\x00":
        code = payload[off : off + 1]
        end = payload.index(b"\x00", off + 1)
        if code == b"M":
            return payload[off + 1 : end].decode("ascii", "replace")
        off = end + 1
    return "unknown error"


def wait_rfq(sock, buf=b""):
    while True:
        while len(buf) >= 5:
            typ = buf[0:1]
            (ln,) = struct.unpack("!i", buf[1:5])
            if len(buf) < 1 + ln:
                break
            payload = buf[5 : 1 + ln]
            buf = buf[1 + ln :]
            if typ == b"E":
                raise RuntimeError(error_message(payload))
            if typ == b"Z":
                return buf
        data = sock.recv(65536)
        if not data:
            raise RuntimeError("eof")
        buf += data


def connect():
    sock = socket.create_connection(("127.0.0.1", PORT))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    params = b"user\x00postgres\x00database\x00postgres\x00\x00"
    sock.sendall(struct.pack("!ii", 8 + len(params), 196608) + params)
    wait_rfq(sock)
    if SETUP:
        q = SETUP.encode()
        sock.sendall(b"Q" + struct.pack("!i", 5 + len(q)) + q + b"\x00")
        wait_rfq(sock)
    return sock


def extended_query(sock):
    q = QUERY.encode()
    parse = b"\x00" + q + b"\x00" + struct.pack("!h", 0)
    bind = (b"\x00\x00" + struct.pack("!h", 0) + struct.pack("!h", 0)
            + struct.pack("!hh", 1, FMT))
    execute = b"\x00" + struct.pack("!i", 0)
    msg = (b"P" + struct.pack("!i", 4 + len(parse)) + parse
           + b"B" + struct.pack("!i", 4 + len(bind)) + bind
           + b"E" + struct.pack("!i", 4 + len(execute)) + execute
           + b"S" + struct.pack("!i", 4))
    sock.sendall(msg)


def simple_copy_query(sock):
    fmt = "binary" if FMT else "text"
    q = f"COPY ({QUERY}) TO STDOUT (FORMAT {fmt})".encode()
    sock.sendall(b"Q" + struct.pack("!i", 5 + len(q)) + q + b"\x00")


def scan_error(chunk):
    # The response head is small control frames (ParseComplete/BindComplete/
    # CopyOutResponse/ErrorResponse) before the DataRow/CopyData flood; walking
    # frames in the first chunk is enough to catch a serialization error.
    off = 0
    while off + 5 <= len(chunk):
        typ = chunk[off : off + 1]
        (ln,) = struct.unpack("!i", chunk[off + 1 : off + 5])
        if typ in (b"D", b"d", b"H"):  # DataRow / CopyData / CopyOutResponse
            return None
        if typ == b"E":
            end = min(off + 1 + ln, len(chunk))
            return error_message(chunk[off + 5 : end])
        off += 1 + ln
    return None


def drain_once(sock):
    if COPY:
        simple_copy_query(sock)
    else:
        extended_query(sock)
    total = 0
    tail = b""
    error = None
    first = True
    start = time.perf_counter()
    while True:
        data = sock.recv(1 << 20)
        if not data:
            raise RuntimeError("eof during drain")
        if first:
            error = scan_error(data)
            first = False
        total += len(data)
        tail = (tail + data)[-6:]
        if tail[0:1] == b"Z" and tail[1:5] == RFQ_TAIL:
            break
    return total, time.perf_counter() - start, error


timings = []
total = 0
err = None
for rep in range(REPS):
    s = connect()
    try:
        total, secs, err = drain_once(s)
    finally:
        s.close()
    if err:
        break
    timings.append(secs)

if err:
    print(f"{LABEL}\t0\t0\t0\tERROR: {err}")
else:
    med = statistics.median(timings)
    print(f"{LABEL}\t{med:.3f}\t{total / 1e6:.1f}\t{total / 1e6 / med:.0f}\tok")
