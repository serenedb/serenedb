"""Raw pg-wire v3 protocol tests (no client driver).

Exercises the protocol surface drivers cannot reach or do not pin down:
exact message sequences (ParseComplete/EmptyQueryResponse/PortalSuspended),
Parse semantics (empty query, multi-statement 42601, failed-Parse name
recovery, duplicate 42P05), COPY FROM STDIN framing over both protocols,
cursor paging with max_rows including the paged-then-unlimited drain, and
binary result format.
"""

from __future__ import annotations

import socket
import struct

import pytest
from spec_loader import conn_kwargs


def _cstr(x: str) -> bytes:
    return x.encode() + b"\0"


class WireConn:
    """Minimal pg-wire v3 client: framed messages, no driver logic."""

    def __init__(self):
        kw = conn_kwargs()
        self.sock = socket.create_connection((kw["host"], kw["port"]))
        params = (
            _cstr("user")
            + _cstr(kw["user"])
            + _cstr("database")
            + _cstr(kw["dbname"])
            + b"\0"
        )
        self.sock.sendall(struct.pack("!II", len(params) + 8, 196608) + params)
        while True:
            t, p = self.read_msg()
            if t == "Z":
                break
            if t == "E":
                raise RuntimeError(f"startup error: {p!r}")

    def close(self):
        try:
            self.send("X")
        finally:
            self.sock.close()

    def _recv_exact(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise EOFError("connection closed")
            buf += chunk
        return buf

    def read_msg(self):
        t = self._recv_exact(1)
        (ln,) = struct.unpack("!I", self._recv_exact(4))
        return t.decode(), self._recv_exact(ln - 4)

    def send(self, t: str, payload: bytes = b""):
        self.sock.sendall(t.encode() + struct.pack("!I", len(payload) + 4) + payload)

    def parse(self, name: str, query: str, oids=()):
        payload = _cstr(name) + _cstr(query) + struct.pack("!H", len(oids))
        for oid in oids:
            payload += struct.pack("!I", oid)
        self.send("P", payload)

    def bind(self, portal: str, stmt: str, params=(), result_format: int | None = None):
        payload = _cstr(portal) + _cstr(stmt) + struct.pack("!H", 0)
        payload += struct.pack("!H", len(params))
        for v in params:
            if v is None:
                payload += struct.pack("!i", -1)
            else:
                payload += struct.pack("!I", len(v)) + v
        if result_format is None:
            payload += struct.pack("!H", 0)
        else:
            payload += struct.pack("!HH", 1, result_format)
        self.send("B", payload)

    def execute(self, portal: str, max_rows: int = 0):
        self.send("E", _cstr(portal) + struct.pack("!I", max_rows))

    def sync(self):
        self.send("S")

    def drain_to_ready(self):
        msgs = []
        while True:
            t, p = self.read_msg()
            msgs.append((t, p))
            if t == "Z":
                return msgs

    def run(self, query: str):
        """Extended-protocol round trip for setup statements."""
        self.parse("", query)
        self.bind("", "")
        self.execute("")
        self.sync()
        return self.drain_to_ready()


def types(msgs):
    return [t for t, _ in msgs]


def errors(msgs):
    out = []
    for t, p in msgs:
        if t != "E":
            continue
        fields = {}
        for part in p.split(b"\0"):
            if part:
                fields[chr(part[0])] = part[1:].decode()
        out.append(fields)
    return out


def rows(msgs):
    return [p for t, p in msgs if t == "D"]


def tags(msgs):
    return [p for t, p in msgs if t == "C"]


@pytest.fixture()
def conn():
    c = WireConn()
    yield c
    c.close()


def test_empty_query_extended(conn):
    conn.parse("", "")
    conn.bind("", "")
    conn.send("D", b"S" + _cstr(""))
    conn.send("D", b"P" + _cstr(""))
    conn.execute("")
    conn.sync()
    assert types(conn.drain_to_ready()) == ["1", "2", "t", "n", "n", "I", "Z"]


def test_multi_statement_parse_is_42601(conn):
    conn.parse("", "select 1; select 2")
    conn.sync()
    errs = errors(conn.drain_to_ready())
    assert len(errs) == 1
    assert errs[0].get("C") == "42601"
    assert "multiple commands" in errs[0].get("M", "")


def test_failed_parse_does_not_define_name(conn):
    conn.parse("stale_test", "selecct 1 frommm nowhere(")
    conn.sync()
    assert errors(conn.drain_to_ready())
    # PG precedence: the failed Parse must not have defined the name.
    conn.parse("stale_test", "select 42")
    conn.bind("", "stale_test")
    conn.execute("")
    conn.sync()
    m = conn.drain_to_ready()
    assert "E" not in types(m)
    assert len(rows(m)) == 1 and rows(m)[0].endswith(b"42")


def test_duplicate_named_statement_is_42p05(conn):
    conn.parse("dup_test", "select 1")
    conn.sync()
    assert not errors(conn.drain_to_ready())
    conn.parse("dup_test", "select 2")
    conn.sync()
    errs = errors(conn.drain_to_ready())
    assert len(errs) == 1 and errs[0].get("C") == "42P05"


def test_parameterized_select_with_type_hint(conn):
    conn.parse("", "select $1::int4 + 1", oids=(23,))
    conn.bind("", "", params=(b"41",))
    conn.send("D", b"P" + _cstr(""))
    conn.execute("")
    conn.sync()
    m = conn.drain_to_ready()
    assert len(rows(m)) == 1 and rows(m)[0].endswith(b"42")


def test_binary_result_format(conn):
    conn.parse("", "select 41 + 1")
    conn.bind("", "", result_format=1)
    conn.execute("")
    conn.sync()
    m = conn.drain_to_ready()
    assert "E" not in types(m), errors(m)
    (row,) = rows(m)
    # DataRow payload: int16 ncols, then per column int32 len + value;
    # binary int4 is 4 big-endian bytes.
    ncols, length = struct.unpack("!hi", row[:6])
    assert ncols == 1 and length == 4
    assert struct.unpack("!i", row[6:10])[0] == 42


def test_portal_paging_and_unlimited_drain(conn):
    conn.parse("", "SELECT i FROM generate_series(1,10) g(i)")
    conn.bind("", "")
    conn.execute("", max_rows=3)
    conn.execute("", max_rows=0)
    conn.sync()
    m = conn.drain_to_ready()
    assert "E" not in types(m), errors(m)
    assert len(rows(m)) == 10
    # First Execute suspends after 3 rows; the second drains the remaining 7
    # (resuming from the carried chunk) and reports only its own row count.
    assert types(m).count("s") == 1
    assert [t for t in tags(m)] == [b"SELECT 7\x00"]


def test_copy_from_stdin_extended(conn):
    conn.run("drop table if exists smoke_copy_ext")
    assert "E" not in types(conn.run("create table smoke_copy_ext(a int, b text)"))
    try:
        conn.parse("", "copy smoke_copy_ext from stdin")
        conn.bind("", "")
        conn.execute("")
        assert conn.read_msg()[0] == "1"
        assert conn.read_msg()[0] == "2"
        assert conn.read_msg()[0] == "G"  # CopyInResponse before Sync
        conn.send("d", b"1\tone\n")
        conn.send("d", b"2\ttwo\n")
        conn.send("c")
        conn.sync()
        m = conn.drain_to_ready()
        assert any(b"COPY 2" in t for t in tags(m)), types(m)

        m = conn.run("select count(*), min(b) from smoke_copy_ext")
        assert len(rows(m)) == 1
        assert b"2" in rows(m)[0] and b"one" in rows(m)[0]
    finally:
        conn.run("drop table if exists smoke_copy_ext")


def test_simple_protocol_empty_multi_and_copy(conn):
    conn.send("Q", _cstr(""))
    assert types(conn.drain_to_ready()) == ["I", "Z"]

    conn.send("Q", _cstr("select 1; select 2"))
    m = conn.drain_to_ready()
    assert types(m).count("C") == 2 and "E" not in types(m)

    conn.send("Q", _cstr("drop table if exists smoke_copy_simple"))
    conn.drain_to_ready()
    conn.send("Q", _cstr("create table smoke_copy_simple(a int, b text)"))
    assert "E" not in types(conn.drain_to_ready())
    try:
        conn.send("Q", _cstr("copy smoke_copy_simple from stdin"))
        assert conn.read_msg()[0] == "G"
        conn.send("d", b"3\tthree\n")
        conn.send("c")
        m = conn.drain_to_ready()
        assert any(b"COPY 1" in t for t in tags(m)), types(m)
    finally:
        conn.send("Q", _cstr("drop table smoke_copy_simple"))
        conn.drain_to_ready()


def test_back_to_back_simple_queries(conn):
    """Cheap canary for wire-framing races (RowDescription corruption class):
    rapid sequential simple queries on one connection must stay in sync."""
    for i in range(200):
        conn.send("Q", _cstr(f"select {i}"))
        m = conn.drain_to_ready()
        assert "E" not in types(m)
        assert rows(m)[0].endswith(str(i).encode())
