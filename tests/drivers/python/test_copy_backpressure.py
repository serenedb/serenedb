"""A client that stops reading a large COPY ... TO STDOUT holds the COPY back,
over HTTP as over the pg wire: the server keeps about its send high-water mark
of the answer unsent instead of the rest of it, and the COPY finishes once the
client reads again."""

from __future__ import annotations

import base64
import os
import socket
import struct
import time
from pathlib import Path

import psycopg
import pytest
from serened import Serened  # type: ignore[import-untyped]

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)

ROWS = 4_000_000
WIDTH = 100
ANSWER = ROWS * WIDTH
READ_FIRST = 1 << 20
UNSENT = 16 << 20
GROWTH = 128 << 20
STALL = 2.0
RCVBUF = 64 << 10


def copy(marker: str, fmt: str, order: str = "") -> str:
    return (f"COPY (SELECT lpad(i::TEXT, {WIDTH - 1}, '0') AS {marker} "
            f"FROM range({ROWS}) t(i){order}) TO STDOUT (FORMAT {fmt})")


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    root = tmp_path_factory.mktemp("copy_backpressure")
    serened = Serened(SERENED_BIN, datadir_root=str(root),
                      log_path=str(root / "serened.log"), http="api=test",
                      extra_args=("--cpu_threads=4",))
    serened.start()
    try:
        yield serened
    finally:
        serened.stop()


@pytest.fixture()
def observer(server):
    with psycopg.connect(server.dsn(), autocommit=True) as conn:
        yield conn


def connect(port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RCVBUF)
    sock.settimeout(60)
    sock.connect(("127.0.0.1", port))
    return sock


class Stream:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buffer = bytearray()
        self.offset = 0
        self.received = 0

    def fill(self, need: int) -> None:
        while len(self.buffer) - self.offset < need:
            if self.offset >= 1 << 20:
                del self.buffer[:self.offset]
                self.offset = 0
            chunk = self.sock.recv(1 << 20)
            if not chunk:
                raise EOFError("the server closed the connection")
            self.received += len(chunk)
            self.buffer += chunk

    def take(self, size: int) -> bytes:
        self.fill(size)
        data = bytes(self.buffer[self.offset:self.offset + size])
        self.offset += size
        return data

    def line(self) -> bytes:
        while True:
            end = self.buffer.find(b"\r\n", self.offset)
            if end >= 0:
                data = bytes(self.buffer[self.offset:end])
                self.offset = end + 2
                return data
            self.fill(len(self.buffer) - self.offset + 1)


class Payload:
    def __init__(self):
        self.size = 0
        self.head = b""
        self.tail = b""

    def add(self, data: bytes) -> None:
        self.size += len(data)
        if len(self.head) < 128:
            self.head += data[:128 - len(self.head)]
        self.tail = (self.tail + data[-128:])[-128:]


def running(observer: psycopg.Connection, marker: str) -> bool:
    return observer.execute(
        "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' "
        "AND query LIKE %s AND pid <> pg_backend_pid()",
        (f"%{marker}%",)).fetchone()[0] != 0


def resident(pid: int) -> int:
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    raise AssertionError("no VmRSS")


def chunked(stream: Stream, payload: Payload, limit: int | None = None) -> bool:
    while limit is None or payload.size < limit:
        size = int(stream.line(), 16)
        if size == 0:
            stream.line()
            return True
        payload.add(stream.take(size))
        stream.take(2)
    return False


@pytest.mark.parametrize("fmt", ["csv", "text"])
def test_an_http_client_that_stops_reading_holds_its_copy_back(server,
                                                               observer, fmt):
    marker = f"drv_copy_backpressure_http_{fmt}"
    sql = copy(marker, fmt).encode()
    auth = base64.b64encode(b"postgres:").decode()
    stream = Stream(connect(server.http_port))
    try:
        before = resident(server.pid())
        stream.sock.sendall(
            (f"POST /_test/copy HTTP/1.1\r\nHost: 127.0.0.1\r\n"
             f"Authorization: Basic {auth}\r\nContent-Type: text/plain\r\n"
             f"Content-Length: {len(sql)}\r\n\r\n").encode() + sql)
        assert stream.line() == b"HTTP/1.1 200 OK"
        headers = []
        while header := stream.line():
            headers.append(header.lower())
        assert b"transfer-encoding: chunked" in headers
        payload = Payload()
        assert not chunked(stream, payload, READ_FIRST)
        deadline = time.monotonic() + STALL
        while time.monotonic() < deadline:
            assert running(observer, marker), \
                "the COPY finished while its client did not read"
            time.sleep(0.1)
        assert resident(server.pid()) - before < GROWTH
        assert chunked(stream, payload)
        assert payload.size == ANSWER
        deadline = time.monotonic() + 30
        while running(observer, marker):
            assert time.monotonic() < deadline, "the COPY never finished"
            time.sleep(0.05)
    finally:
        stream.sock.close()


class Wire:
    def __init__(self, port: int):
        self.stream = Stream(connect(port))
        params = b"user\0postgres\0database\0postgres\0\0"
        self.stream.sock.sendall(
            struct.pack("!II", len(params) + 8, 196608) + params)
        self.until(b"Z")

    def message(self) -> tuple[bytes, int]:
        header = self.stream.take(5)
        return header[:1], struct.unpack("!I", header[1:])[0] - 4

    def until(self, kind: bytes) -> list[tuple[bytes, bytes]]:
        seen = []
        while True:
            got, size = self.message()
            body = self.stream.take(size)
            assert got != b"E", body
            seen.append((got, body))
            if got == kind:
                return seen

    def query(self, sql: str) -> None:
        body = sql.encode() + b"\0"
        self.stream.sock.sendall(b"Q" + struct.pack("!I", len(body) + 4) + body)

    def scalar(self, sql: str) -> str:
        self.query(sql)
        row = next(body for kind, body in self.until(b"Z") if kind == b"D")
        (size,) = struct.unpack("!i", row[2:6])
        return row[6:6 + size].decode()

    def copy_data(self, payload: Payload, limit: int | None = None) -> bytes:
        while limit is None or payload.size < limit:
            kind, size = self.message()
            if kind != b"d":
                return kind
            payload.add(self.stream.take(size))
        return b""


def copied(observer: psycopg.Connection, pid: int) -> int:
    deadline = time.monotonic() + 30
    last, same = -1, 0
    while time.monotonic() < deadline:
        row = observer.execute(
            "SELECT tuples_processed FROM pg_stat_progress_copy WHERE pid = %s",
            (pid,)).fetchone()
        assert row is not None, "the COPY finished while its client did not read"
        same = same + 1 if row[0] == last else 0
        if same == 5:
            return last
        last = row[0]
        time.sleep(0.1)
    pytest.fail("the COPY kept running ahead of its client")


@pytest.mark.parametrize("fmt, order", [
    ("csv", ""),
    ("csv", " ORDER BY i"),
    ("parquet", ""),
], ids=["csv", "ordered", "parquet"])
def test_a_pg_client_that_stops_reading_holds_its_copy_back(server, observer,
                                                            fmt, order):
    wire = Wire(server.port)
    try:
        pid = int(wire.scalar("SELECT pg_backend_pid()"))
        wire.query(copy("drv_copy_backpressure_pg", fmt, order))
        kind, size = wire.message()
        assert kind == b"H"
        wire.stream.take(size)
        payload = Payload()
        assert wire.copy_data(payload, READ_FIRST) == b""
        rows = copied(observer, pid)
        if not order:
            assert 0 < rows < ROWS // 2
        if fmt == "csv" and not order:
            assert rows * WIDTH - wire.stream.received <= UNSENT
        assert wire.copy_data(payload) == b"c"
        tag = wire.until(b"C")[-1][1]
        assert tag == f"COPY {ROWS}\0".encode()
        wire.until(b"Z")
        if fmt == "parquet":
            assert payload.head.startswith(b"PAR1")
            assert payload.tail.endswith(b"PAR1")
        else:
            assert payload.size == ANSWER
        if order:
            assert payload.head.startswith(b"0" * (WIDTH - 1) + b"\n")
            assert payload.tail.endswith(
                str(ROWS - 1).rjust(WIDTH - 1, "0").encode() + b"\n")
    finally:
        wire.stream.sock.close()


@pytest.mark.parametrize("fmt, order", [
    ("csv", " ORDER BY i"),
    ("parquet", ""),
], ids=["ordered", "parquet"])
def test_a_pg_client_that_leaves_mid_copy_ends_it(server, observer, fmt, order):
    wire = Wire(server.port)
    try:
        pid = int(wire.scalar("SELECT pg_backend_pid()"))
        wire.query(copy("drv_copy_backpressure_left", fmt, order))
        kind, size = wire.message()
        assert kind == b"H"
        wire.stream.take(size)
        assert wire.copy_data(Payload(), READ_FIRST) == b""
        copied(observer, pid)
    finally:
        wire.stream.sock.close()
    deadline = time.monotonic() + 30
    while observer.execute(
            "SELECT count(*) FROM pg_stat_progress_copy WHERE pid = %s",
            (pid,)).fetchone()[0]:
        assert time.monotonic() < deadline, "the COPY outlived its client"
        time.sleep(0.05)
    assert observer.execute("SELECT 1").fetchone() == (1,)
