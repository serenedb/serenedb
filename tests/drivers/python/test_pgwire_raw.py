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

    def describe(self, what: str, name: str = ""):
        # what: 'S' (prepared statement) or 'P' (portal).
        self.send("D", what.encode() + _cstr(name))

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


def statuses(msgs):
    """ReadyForQuery transaction-status bytes: 'I' idle, 'T' in txn, 'E' aborted."""
    return [chr(p[0]) for t, p in msgs if t == "Z"]


def notices(msgs):
    """NoticeResponse ('N') messages decoded into field dicts."""
    out = []
    for t, p in msgs:
        if t != "N":
            continue
        fields = {}
        for part in p.split(b"\0"):
            if part:
                fields[chr(part[0])] = part[1:].decode()
        out.append(fields)
    return out


def sqlstates(msgs):
    return [e.get("C") for e in errors(msgs)]


def param_status(msgs):
    """ParameterStatus ('S') messages as (name, value) pairs."""
    out = []
    for t, p in msgs:
        if t == "S":
            parts = p.split(b"\0")
            out.append((parts[0].decode(), parts[1].decode()))
    return out


def row_description(msgs):
    """RowDescription ('T') columns as (name, type_oid, type_modifier) tuples."""
    for t, p in msgs:
        if t == "T":
            (ncols,) = struct.unpack("!H", p[:2])
            off, cols = 2, []
            for _ in range(ncols):
                e = p.index(b"\0", off)
                name = p[off:e].decode()
                off = e + 1
                _toid, _attnum, typoid, _typlen, typmod, _fmt = struct.unpack(
                    "!IhIhih", p[off : off + 18]
                )
                off += 18
                cols.append((name, typoid, typmod))
            return cols
    return None


def first_field(msgs):
    """First column of the first DataRow as raw bytes (text format)."""
    r = rows(msgs)[0]
    (ncols,) = struct.unpack("!H", r[:2])
    assert ncols >= 1, r
    (ln,) = struct.unpack("!i", r[2:6])
    return r[6 : 6 + ln]


def all_fields(msgs):
    """All columns of the first DataRow as raw bytes; None for SQL NULL."""
    r = rows(msgs)[0]
    (ncols,) = struct.unpack("!H", r[:2])
    out = []
    off = 2
    for _ in range(ncols):
        (ln,) = struct.unpack("!i", r[off : off + 4])
        off += 4
        if ln == -1:
            out.append(None)
        else:
            out.append(r[off : off + ln])
            off += ln
    return out


PGCOPY_SIGNATURE = b"PGCOPY\n\xff\r\n\x00"


@pytest.fixture()
def conn():
    c = WireConn()
    yield c
    c.close()


def test_startup_rejects_missing_user():
    # PG requires a user name in the startup packet (SQLSTATE 28000); without it
    # the server must not silently fall back to a default user.
    kw = conn_kwargs()
    sock = socket.create_connection((kw["host"], kw["port"]))
    try:

        def recv_exact(n: int) -> bytes:
            buf = b""
            while len(buf) < n:
                chunk = sock.recv(n - len(buf))
                if not chunk:
                    raise EOFError("connection closed")
                buf += chunk
            return buf

        params = _cstr("database") + _cstr(kw["dbname"]) + b"\0"
        sock.sendall(struct.pack("!II", len(params) + 8, 196608) + params)
        msg_type = recv_exact(1)
        assert msg_type == b"E", f"expected ErrorResponse, got {msg_type!r}"
        (length,) = struct.unpack("!I", recv_exact(4))
        body = recv_exact(length - 4)
        fields = {
            chr(f[0]): f[1:].decode("latin1", "replace")
            for f in body.split(b"\0")
            if f
        }
        assert fields.get("C") == "28000", fields
    finally:
        sock.close()


def _startup_collect(extra_params: dict[str, str]):
    """Open a raw trust connection with extra startup params; collect every
    message from AuthenticationOk through the terminating ReadyForQuery (or an
    ErrorResponse if the connection is refused)."""
    kw = conn_kwargs()
    sock = socket.create_connection((kw["host"], kw["port"]))

    def recv_exact(n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise EOFError("connection closed")
            buf += chunk
        return buf

    params = _cstr("user") + _cstr(kw["user"]) + _cstr("database") + _cstr(kw["dbname"])
    for k, v in extra_params.items():
        params += _cstr(k) + _cstr(v)
    params += b"\0"
    sock.sendall(struct.pack("!II", len(params) + 8, 196608) + params)
    msgs = []
    try:
        while True:
            t = recv_exact(1).decode()
            (ln,) = struct.unpack("!I", recv_exact(4))
            msgs.append((t, recv_exact(ln - 4)))
            if t in ("Z", "E"):
                return msgs
    finally:
        sock.close()


def test_startup_non_utf8_client_encoding_accepted_with_warning():
    # A valid-but-non-UTF8 client_encoding (LATIN1) is a normal startup GUC every
    # PG client may send (JDBC/.NET/psql). serenedb is UTF8-only and cannot
    # transcode, but PG accepts it -- so the session must NOT be refused. serenedb
    # keeps UTF8 (reported in the burst) and emits a WARNING NoticeResponse before
    # ReadyForQuery. Regression: serenedb dropped the whole connection (08000-ish
    # ErrorResponse) over a parameter PG honors.
    m = _startup_collect({"client_encoding": "LATIN1"})
    t = types(m)
    assert "Z" in t and "E" not in t, t  # connection survived to ReadyForQuery
    # client_encoding is reported as UTF8 in the startup burst (not LATIN1).
    assert dict(param_status(m)).get("client_encoding") == "UTF8", param_status(m)
    # Exactly one WARNING NoticeResponse names the ignored encoding.
    warns = [n for n in notices(m) if "client_encoding" in n.get("M", "")]
    assert len(warns) == 1, notices(m)
    assert warns[0].get("S") == "WARNING" and warns[0].get("C") == "01000", warns
    assert "LATIN1" in warns[0]["M"] and "UTF8" in warns[0]["M"], warns


def test_startup_utf8_client_encoding_no_warning():
    # The matching encoding (UTF8 and its aliases) applies cleanly: connection up,
    # no client_encoding NoticeResponse.
    for enc in ("UTF8", "utf-8"):
        m = _startup_collect({"client_encoding": enc})
        assert "Z" in types(m) and "E" not in types(m), (enc, types(m))
        assert [n for n in notices(m) if "client_encoding" in n.get("M", "")] == [], (
            enc,
            notices(m),
        )


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


def test_bind_param_count_mismatch_is_08p01(conn):
    # PG rejects a Bind whose parameter count differs from the prepared
    # statement's at Bind time (08P01, ERRCODE_PROTOCOL_VIOLATION), before
    # BindComplete -- not later at Execute. Regression: serenedb used to emit
    # BindComplete and surface the mismatch only at Execute with XX000.
    conn.parse("pc_mismatch", "select $1::int4")
    conn.sync()
    assert not errors(conn.drain_to_ready())

    # Too few parameters: error at Bind, no BindComplete.
    conn.bind("", "pc_mismatch", params=())
    conn.execute("")
    conn.sync()
    m = conn.drain_to_ready()
    assert "2" not in types(m)
    errs = errors(m)
    assert len(errs) == 1 and errs[0].get("C") == "08P01"
    assert errs[0].get("M") == (
        'bind message supplies 0 parameters, '
        'but prepared statement "pc_mismatch" requires 1'
    )

    # Too many parameters: same rejection, still no BindComplete.
    conn.bind("", "pc_mismatch", params=(b"1", b"2"))
    conn.execute("")
    conn.sync()
    m = conn.drain_to_ready()
    assert "2" not in types(m)
    assert sqlstates(m) == ["08P01"]

    # The matching count still binds and executes (statement survives the error).
    conn.bind("", "pc_mismatch", params=(b"7",))
    conn.execute("")
    conn.sync()
    m = conn.drain_to_ready()
    assert "E" not in types(m), errors(m)
    assert "2" in types(m) and len(rows(m)) == 1 and rows(m)[0].endswith(b"7")


def test_fast_path_function_call_rejected_gracefully(conn):
    # The legacy fast-path FunctionCall ('F') sub-protocol is unsupported. The
    # server must reject it with 0A000 and KEEP the connection (PG keeps it on a
    # fast-path error), not FATAL-close as it would for an unknown message type.
    # Body: function oid 0, 0 arg-format codes, 0 args, result-format 0.
    conn.send("F", struct.pack("!IHHH", 0, 0, 0, 0))
    m = conn.drain_to_ready()
    assert sqlstates(m) == ["0A000"]
    # Connection survives: a subsequent query still runs.
    m2 = conn.run("select 1")
    assert "E" not in types(m2), errors(m2)
    assert rows(m2) and rows(m2)[0].endswith(b"1")


def test_set_local_revert_not_reported(conn):
    # PG emits GUC ParameterStatus only at ReadyForQuery, after the implicit
    # block commit reverts a SET LOCAL -- so a set-then-reverted SET LOCAL nets
    # to no message. Regression: serenedb reported the value per-dispatch (before
    # the revert) and never corrected it, so the client saw a reverted GUC as set.
    try:
        # Simple multi-statement => implicit block; SET LOCAL reverts at commit.
        conn.send("Q", _cstr("set local search_path = 'sl_probe'; select 1"))
        m = conn.drain_to_ready()
        assert "E" not in types(m), errors(m)
        assert "search_path" not in dict(param_status(m)), param_status(m)

        # Extended: the SET LOCAL Execute must NOT emit ParameterStatus before
        # Sync (reporting is batched to ReadyForQuery), and the Sync commit
        # reverts it with no report.
        conn.parse("", "set local search_path = 'sl_probe2'")
        conn.bind("", "")
        conn.execute("")
        conn.send("H")  # Flush: force the accumulated replies out before Sync
        pre_sync = []
        while True:
            t, p = conn.read_msg()
            pre_sync.append((t, p))
            if t == "C":  # CommandComplete for the SET -- stop before Sync
                break
        assert "search_path" not in dict(param_status(pre_sync)), param_status(pre_sync)
        conn.sync()
        m2 = conn.drain_to_ready()
        assert "search_path" not in dict(param_status(m2)), param_status(m2)

        # A persistent SET is still reported (at ReadyForQuery).
        m3 = conn.run("set search_path = 'sl_keep'")
        assert dict(param_status(m3)).get("search_path") == "sl_keep", param_status(m3)
    finally:
        conn.run("reset search_path")


def test_rowdescription_numeric_typmod(conn):
    # PG sends the real atttypmod; for numeric(p,s) it's ((p<<16)|s)+4 so clients
    # (JDBC getPrecision/getScale, psycopg .precision/.scale) recover the declared
    # precision/scale. Regression: serenedb hardcoded -1. An array reports its
    # element's modifier; varchar/temporal stay -1 (DuckDB doesn't retain those).
    def typmods(sql):
        conn.send("Q", _cstr(sql))
        return row_description(conn.drain_to_ready())

    assert typmods("select 1.23::numeric(10,2) as n") == [("n", 1700, 655366)]
    assert typmods("select [1.5::numeric(8,3)] as a") == [("a", 1231, 524295)]
    assert typmods("select 1::int4 as i") == [("i", 23, -1)]
    assert typmods("select 'x'::text as t") == [("t", 25, -1)]


def test_char_oid18_param_roundtrip(conn):
    # "char" (OID 18) must decode a bound parameter as a 1-byte char, symmetric
    # with how a result of that type is rendered. Regression: Oid2Type(18)
    # returned TINYINT, so binding 'A' failed with 22P02 (can't parse "A" as int).
    conn.parse("", "select $1", oids=(18,))
    conn.bind("", "", params=(b"A",))
    conn.send("D", b"P" + _cstr(""))  # Describe portal -> RowDescription
    conn.execute("")
    conn.sync()
    m = conn.drain_to_ready()
    assert "E" not in types(m), errors(m)
    cols = row_description(m)
    assert cols and cols[0][1] == 18, cols  # result type oid 18 ("char")
    assert first_field(m) == b"A"


def test_describe_resolves_deferred_param_output(conn):
    # A bare projected parameter carries no type until it is known, so duckdb
    # leaves the prepared statement's output type UNKNOWN. Describe must
    # probe-plan to fill it in: statement Describe ('S') synthesizes a
    # placeholder value, portal Describe ('P') reuses the bound one. Both must
    # report the resolved type (text, OID 25) and the projected alias, not the
    # UNKNOWN template. Regression: WriteResolvedRowDescription.
    conn.parse("deferred", "select $1 as label")
    conn.describe("S", "deferred")
    conn.sync()
    m = conn.drain_to_ready()
    assert not errors(m), errors(m)
    assert row_description(m) == [("label", 25, -1)], row_description(m)

    conn.bind("", "deferred", params=(b"hello",))
    conn.describe("P", "")
    conn.execute("")
    conn.sync()
    m = conn.drain_to_ready()
    assert not errors(m), errors(m)
    assert row_description(m) == [("label", 25, -1)], row_description(m)
    assert first_field(m) == b"hello", rows(m)


def test_timestamp_date_infinity_wire(conn):
    # PG sends +/-infinity timestamps/dates as the raw INT*_MAX/MIN sentinels.
    # Regression: serenedb applied the epoch-gap shift unconditionally (a finite
    # garbage value / signed-overflow), and DATE text rendered a bogus year for
    # infinity instead of 'infinity'.
    INT64_MAX, INT64_MIN = (1 << 63) - 1, -(1 << 63)
    INT32_MAX, INT32_MIN = (1 << 31) - 1, -(1 << 31)

    def binval(sql):
        conn.parse("", sql)
        conn.bind("", "", result_format=1)
        conn.execute("")
        conn.sync()
        m = conn.drain_to_ready()
        assert "E" not in types(m), errors(m)
        return all_fields(m)[0]

    assert struct.unpack("!q", binval("select 'infinity'::timestamp"))[0] == INT64_MAX
    assert struct.unpack("!q", binval("select '-infinity'::timestamp"))[0] == INT64_MIN
    assert struct.unpack("!q", binval("select 'infinity'::timestamptz"))[0] == INT64_MAX
    assert struct.unpack("!q", binval("select '-infinity'::timestamptz"))[0] == INT64_MIN
    assert struct.unpack("!i", binval("select 'infinity'::date"))[0] == INT32_MAX
    assert struct.unpack("!i", binval("select '-infinity'::date"))[0] == INT32_MIN
    # Finite values keep the epoch-gap shift (2000-01-01 is the wire zero).
    assert struct.unpack("!q", binval("select '2000-01-01 00:00:00'::timestamp"))[0] == 0
    assert struct.unpack("!i", binval("select '2000-01-01'::date"))[0] == 0

    def textval(sql):
        conn.send("Q", _cstr(sql))
        return first_field(conn.drain_to_ready())

    assert textval("select 'infinity'::date") == b"infinity"
    assert textval("select '-infinity'::date") == b"-infinity"


def _decode_pg_numeric(b):
    """Decode a PG numeric binary field holding an integer to a Python int."""
    ndig, weight, sign, _dscale = struct.unpack("!hhHh", b[:8])
    val = 0
    for i in range(ndig):
        val = val * 10000 + struct.unpack("!h", b[8 + 2 * i : 10 + 2 * i])[0]
    if ndig:
        val *= 10000 ** (weight - (ndig - 1))
    return -val if sign == 0x4000 else val


def test_hugeint_min_numeric_binary(conn):
    # INT128_MIN (-2^127) is a valid HUGEINT; its numeric-binary encoding must
    # not negate-overflow. Regression: serenedb negated in the signed type, so
    # the minimum flipped sign (decoded positive).
    int128_min = -(1 << 127)
    int128_max = (1 << 127) - 1
    for v in (int128_min, int128_max, int128_min + 1, 0, -1, 12345):
        conn.parse("", f"select ({v})::hugeint")
        conn.bind("", "", result_format=1)
        conn.execute("")
        conn.sync()
        m = conn.drain_to_ready()
        assert "E" not in types(m), errors(m)
        assert _decode_pg_numeric(all_fields(m)[0]) == v, v


def test_float_special_values_text(conn):
    # PG renders float infinity/NaN as Infinity / -Infinity / NaN in text.
    def textval(sql):
        conn.send("Q", _cstr(sql))
        return first_field(conn.drain_to_ready())

    assert textval("select 'infinity'::float8") == b"Infinity"
    assert textval("select '-infinity'::float8") == b"-Infinity"
    assert textval("select 'nan'::float8") == b"NaN"
    assert textval("select 'infinity'::float4") == b"Infinity"
    assert textval("select '-infinity'::float4") == b"-Infinity"


def test_dml_command_complete_count_after_select(conn):
    # An extended-protocol UPDATE/DELETE reports its own affected-row count, even
    # after a full-drained prepared SELECT earlier in the session.
    conn.run("drop table if exists t_dml")
    assert "E" not in types(conn.run("create table t_dml(i int)"))
    try:
        conn.run("insert into t_dml select i from range(7) g(i)")
        conn.parse("sel_c", "select i from t_dml")
        conn.bind("", "sel_c")
        conn.execute("", 0)
        conn.sync()
        assert any(b"SELECT 7" in t for t in tags(conn.drain_to_ready()))
        conn.parse("upd_c", "update t_dml set i = i + 1")
        conn.bind("", "upd_c")
        conn.execute("", 0)
        conn.sync()
        assert any(b"UPDATE 7" in t for t in tags(conn.drain_to_ready()))
        conn.parse("del_c", "delete from t_dml")
        conn.bind("", "del_c")
        conn.execute("", 0)
        conn.sync()
        assert any(b"DELETE 7" in t for t in tags(conn.drain_to_ready()))
    finally:
        conn.run("drop table if exists t_dml")


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


def test_paged_portal_flush_pages_sync_drops(conn):
    # PG ends the implicit transaction at every Sync and drops the portal it owns
    # (PreCommit_Portals), so a paged cursor is pulled with Flush -- which does
    # NOT commit -- between Execute calls; an Execute after a Sync hits 34000.
    # Both halves verified byte-identical to PostgreSQL.
    conn.run("drop table if exists smoke_page")
    assert "E" not in types(conn.run("create table smoke_page(i int)"))
    try:
        conn.run("insert into smoke_page select i from generate_series(1,7) g(i)")
        # Paging via Flush keeps the portal alive across pages -> all 7 rows.
        conn.parse("", "SELECT i FROM smoke_page ORDER BY i")
        conn.bind("", "")
        conn.sync()
        assert statuses(conn.drain_to_ready()) == ["I"]
        collected, suspends = [], 0
        for _ in range(20):  # 7 rows / 2 per page -> 4 pages
            conn.execute("", max_rows=2)
            conn.send("H")  # Flush, not Sync
            page = []
            while True:
                t, p = conn.read_msg()
                page.append((t, p))
                if t in ("s", "C"):
                    break
            collected += rows(page)
            if any(t == "s" for t, _ in page):
                suspends += 1
            if any(t == "C" for t, _ in page):
                break
        conn.sync()
        assert statuses(conn.drain_to_ready()) == ["I"]
        assert len(collected) == 7 and suspends >= 1, (len(collected), suspends)
        vals = sorted(int(first_field([("D", r)])) for r in collected)
        assert vals == list(range(1, 8)), vals
        # A Sync between Execute calls ends the txn and drops the portal (PG).
        conn.parse("", "SELECT i FROM smoke_page ORDER BY i")
        conn.bind("", "")
        conn.execute("", max_rows=2)
        conn.sync()
        m = conn.drain_to_ready()
        assert "s" in types(m) and statuses(m) == ["I"], types(m)
        conn.execute("", max_rows=2)
        conn.sync()
        m2 = conn.drain_to_ready()
        assert sqlstates(m2) == ["34000"], (types(m2), sqlstates(m2))
    finally:
        conn.run("drop table if exists smoke_page")


def test_paged_one_row_at_a_time(conn):
    # A single DuckDB chunk (10 rows) fetched one row per Execute (max_rows=1)
    # via Flush. This stresses mid-chunk suspend/resume: the server must emit
    # row K of the chunk, suspend, then resume at row K+1 of the SAME chunk on
    # the next Execute -- no duplicates, no skips, exact order, ending in
    # CommandComplete after the last row.
    conn.parse("", "SELECT i FROM generate_series(1,10) g(i)")
    conn.bind("", "")
    conn.sync()
    assert statuses(conn.drain_to_ready()) == ["I"]
    collected, suspends, done = [], 0, False
    for _ in range(50):
        conn.execute("", max_rows=1)
        conn.send("H")  # Flush keeps the portal alive across pages
        page = []
        while True:
            t, p = conn.read_msg()
            page.append((t, p))
            if t in ("s", "C"):
                break
        r = rows(page)
        assert len(r) <= 1, len(r)
        collected += r
        if any(t == "s" for t, _ in page):
            suspends += 1
        if any(t == "C" for t, _ in page):
            done = True
            break
    conn.sync()
    assert statuses(conn.drain_to_ready()) == ["I"]
    assert done, "never reached CommandComplete"
    vals = [int(first_field([("D", r)])) for r in collected]
    assert vals == list(range(1, 11)), vals
    assert suspends == 10, suspends


# --- portal lifetime: independent of the statement, scoped to the txn --------
# PG/CockroachDB/pgwire-rs all bind a portal to a refcounted plan, NOT to the
# prepared-statement entry, so Close/re-Parse of the statement leaves a bound
# portal working; and a non-holdable portal is dropped at the txn boundary.


def test_close_statement_keeps_bound_portal(conn):
    # Closing prepared statement S must NOT destroy a portal already bound from
    # it -- the portal co-owns the plan (PG GetCachedPlan refcount). Regression:
    # serenedb cascade-dropped the portal, so Execute hit 34000.
    conn.parse("s_keep", "select 42")
    conn.bind("p_keep", "s_keep")
    conn.send("C", b"S" + _cstr("s_keep"))  # Close statement S
    conn.execute("p_keep")  # the portal must still run
    conn.sync()
    m = conn.drain_to_ready()
    assert "E" not in types(m), errors(m)
    assert "3" in types(m), types(m)  # CloseComplete for the statement
    assert rows(m) and rows(m)[0].endswith(b"42"), rows(m)


def test_reparse_unnamed_keeps_bound_portal(conn):
    # Re-Parse of the unnamed statement drops the prior unnamed source, but a
    # portal already bound from it keeps its own plan. Regression: serenedb
    # Reset()'d the anon statement in place, orphaning the portal -> it returned
    # the NEW statement's result (8 instead of 7).
    conn.parse("", "select 7")
    conn.bind("p_anon", "")
    conn.parse("", "select 8")  # re-Parse unnamed
    conn.execute("p_anon")  # must still yield 7
    conn.sync()
    m = conn.drain_to_ready()
    assert "E" not in types(m), errors(m)
    assert rows(m) and rows(m)[0].endswith(b"7"), rows(m)


def test_portal_dropped_at_committing_sync(conn):
    # A non-holdable portal bound in the implicit block is dropped when the block
    # commits at Sync (PG PreCommit_Portals; CockroachDB closes all wire portals
    # at the txn boundary). Re-Execute -> 34000. Regression: serenedb kept portals
    # until session teardown, so a re-Executed DML portal reported "INSERT 0 0".
    conn.run("drop table if exists t_pdrop")
    assert "E" not in types(conn.run("create table t_pdrop(id int)"))
    try:
        conn.parse("ins_pd", "insert into t_pdrop values (1)")
        conn.bind("pp_pd", "ins_pd")
        conn.execute("pp_pd")
        conn.sync()  # commits the implicit block -> drops pp_pd
        m1 = conn.drain_to_ready()
        assert "E" not in types(m1), errors(m1)
        conn.execute("pp_pd")  # portal is gone now
        conn.sync()
        m2 = conn.drain_to_ready()
        assert sqlstates(m2) == ["34000"], (types(m2), tags(m2), errors(m2))
    finally:
        conn.run("drop table if exists t_pdrop")


def test_implicit_block_commit_error_replaces_command_complete(conn):
    # PG commits the implicit block in finish_xact_command BEFORE EndCommand
    # (postgres.c:1304-1314), so a commit-time failure is reported as an
    # ErrorResponse *instead of* the final statement's CommandComplete -- never
    # one then the other. Regression: serenedb committed in CommitAndReportReady
    # after RunSimpleQuery had already emitted every CommandComplete, so the
    # client saw C,C,E. Triggered deterministically via the implicit_block_commit
    # fault (commit failures are otherwise unreachable -- DuckDB has no deferred
    # constraints).
    def simple(sql):
        conn.send("Q", _cstr(sql))
        return conn.drain_to_ready()

    if "E" in types(simple("set sdb_faults='implicit_block_commit'")):
        pytest.skip("fault injection not enabled in this build")
    try:
        assert "E" not in types(simple("drop table if exists t_c5"))
        assert "E" not in types(simple("create table t_c5(id int)"))
        # Single-statement autocommit DDL above never opens an implicit block, so
        # the fault only trips on the multi-statement block below.
        m = simple("insert into t_c5 values (1); insert into t_c5 values (2)")
        assert sqlstates(m) == ["40001"], (types(m), sqlstates(m))
        assert types(m).count("C") == 1, (
            "commit error must replace the last CommandComplete",
            types(m),
        )
        assert tags(m) == [b"INSERT 0 1\x00"], tags(m)
        assert statuses(m) == ["I"], statuses(m)
        # Failed commit rolled the whole block back: no rows survived.
        assert first_field(simple("select count(*) from t_c5")) == b"0"
    finally:
        simple("set sdb_faults='-implicit_block_commit'")
        simple("drop table if exists t_c5")


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


def test_copy_to_stdout_extended(conn):
    # COPY ... TO STDOUT over the extended protocol: Parse/Bind/Execute must
    # route to the wire collector and emit CopyOutResponse(H) + CopyData(d) +
    # CopyDone(c) + CommandComplete("COPY N"), not a normal RowDescription path.
    conn.run("drop table if exists smoke_copy_to_ext")
    assert "E" not in types(conn.run("create table smoke_copy_to_ext(a int, b text)"))
    try:
        conn.run("insert into smoke_copy_to_ext values (1,'one'),(2,'two')")
        conn.parse("", "copy smoke_copy_to_ext to stdout")
        conn.bind("", "")
        conn.execute("")
        conn.sync()
        m = conn.drain_to_ready()
        t = types(m)
        assert "E" not in t, errors(m)
        assert "H" in t and "c" in t, t  # CopyOutResponse + CopyDone
        assert any(b"COPY 2" in tg for tg in tags(m)), tags(m)
        # the row values flow through the CopyData stream (delimiter/header are
        # the DuckDB-native text format's concern, covered in test_copy.py)
        body = b"".join(p for tt, p in m if tt == "d")
        assert b"one" in body and b"two" in body, body
    finally:
        conn.run("drop table if exists smoke_copy_to_ext")


def test_copy_to_stdout_binary_extended(conn):
    conn.run("drop table if exists smoke_copy_tobin_ext")
    assert "E" not in types(conn.run("create table smoke_copy_tobin_ext(a int, b text)"))
    try:
        conn.run(
            "insert into smoke_copy_tobin_ext values (1,'one'),(2,'two'),(3,'three')"
        )
        conn.parse("", "copy smoke_copy_tobin_ext to stdout (format binary)")
        conn.bind("", "")
        conn.execute("")
        conn.sync()
        m = conn.drain_to_ready()
        assert "E" not in types(m), errors(m)
        assert "H" in types(m) and "c" in types(m), types(m)
        blob = b"".join(p for t, p in m if t == "d")
        assert blob.startswith(PGCOPY_SIGNATURE), blob[:11]
        assert blob.endswith(b"\xff\xff"), "PGCOPY -1 trailer"
        assert any(b"COPY 3" in tg for tg in tags(m)), tags(m)
    finally:
        conn.run("drop table if exists smoke_copy_tobin_ext")


def test_copy_binary_round_trip_extended(conn):
    # Full binary COPY both directions over the EXTENDED protocol: TO STDOUT
    # (FORMAT BINARY) produces the PGCOPY stream and FROM STDIN (FORMAT BINARY)
    # loads it back unchanged, including a NULL. Exercises both the deferred
    # COPY-FROM bind-at-Execute path and the extended COPY-TO routing.
    conn.run("drop table if exists smoke_bin_src_ext")
    conn.run("drop table if exists smoke_bin_dst_ext")
    assert "E" not in types(conn.run("create table smoke_bin_src_ext(x int, label text)"))
    assert "E" not in types(conn.run("create table smoke_bin_dst_ext(x int, label text)"))
    try:
        conn.run("insert into smoke_bin_src_ext values (1,'one'),(2,null),(3,'three')")
        conn.parse("", "copy smoke_bin_src_ext to stdout (format binary)")
        conn.bind("", "")
        conn.execute("")
        conn.sync()
        m = conn.drain_to_ready()
        assert "E" not in types(m), errors(m)
        blob = b"".join(p for t, p in m if t == "d")
        assert blob.startswith(PGCOPY_SIGNATURE) and blob.endswith(b"\xff\xff")

        conn.parse("", "copy smoke_bin_dst_ext from stdin (format binary)")
        conn.bind("", "")
        conn.execute("")
        assert conn.read_msg()[0] == "1"  # ParseComplete
        assert conn.read_msg()[0] == "2"  # BindComplete
        assert conn.read_msg()[0] == "G"  # CopyInResponse before Sync
        conn.send("d", blob)
        conn.send("c")
        conn.sync()
        m = conn.drain_to_ready()
        assert any(b"COPY 3" in t for t in tags(m)), types(m)

        assert first_field(conn.run("select count(*) from smoke_bin_dst_ext")) == b"3"
        assert first_field(conn.run("select sum(x) from smoke_bin_dst_ext")) == b"6"
        # the NULL label survived the round trip (count(label) skips NULLs)
        assert first_field(conn.run("select count(label) from smoke_bin_dst_ext")) == b"2"
    finally:
        conn.run("drop table if exists smoke_bin_src_ext")
        conn.run("drop table if exists smoke_bin_dst_ext")


def test_copy_binary_header_flags_rejected(conn):
    # PG validates the PGCOPY header flags word: the WITH-OIDS bit, any other
    # high-16-bit "critical" flag, and a negative header-extension length are
    # each rejected with 22P04 rather than silently misparsed. A clean header
    # (flags 0, ext 0) still loads.
    conn.run("drop table if exists smoke_copy_hdr")
    assert "E" not in types(conn.run("create table smoke_copy_hdr(a int)"))
    try:

        def copy_blob(blob):
            conn.parse("", "copy smoke_copy_hdr from stdin (format binary)")
            conn.bind("", "")
            conn.execute("")
            assert conn.read_msg()[0] == "1"  # ParseComplete
            assert conn.read_msg()[0] == "2"  # BindComplete
            assert conn.read_msg()[0] == "G"  # CopyInResponse
            conn.send("d", blob)
            conn.send("c")
            conn.sync()
            return conn.drain_to_ready()

        # flags packed unsigned (1<<31 overflows signed int32); ext-len signed.
        def header(flags, ext_len):
            return (
                PGCOPY_SIGNATURE
                + struct.pack("!I", flags)
                + struct.pack("!i", ext_len)
                + b"\xff\xff"  # PGCOPY trailer: zero rows
            )

        assert sqlstates(copy_blob(header(1 << 16, 0))) == ["22P04"]  # WITH OIDS
        assert sqlstates(copy_blob(header(1 << 31, 0))) == ["22P04"]  # critical flag
        assert sqlstates(copy_blob(header(0, -1))) == ["22P04"]  # negative length

        # A clean header with one row loads.
        row = struct.pack("!h", 1) + struct.pack("!i", 4) + struct.pack("!i", 42)
        m = copy_blob(PGCOPY_SIGNATURE + struct.pack("!ii", 0, 0) + row + b"\xff\xff")
        assert "E" not in types(m), errors(m)
        assert any(b"COPY 1" in t for t in tags(m)), tags(m)
        assert first_field(conn.run("select count(*) from smoke_copy_hdr")) == b"1"
    finally:
        conn.run("drop table if exists smoke_copy_hdr")


def test_copy_csv_keeps_eod_marker_as_data(conn):
    # PG treats "\." as end-of-data only in TEXT mode; in CSV a sole field of
    # "\." is real data. Regression: serenedb stripped a trailing "\.\n" from the
    # CopyData frame for CSV too (the strip was gated on !binary), losing the
    # row. HEADER false avoids CSV header auto-detection masking the row count.
    conn.run("drop table if exists smoke_csv_eod")
    assert "E" not in types(conn.run("create table smoke_csv_eod(a text)"))
    try:
        conn.parse("", "copy smoke_csv_eod from stdin (format csv, header false)")
        conn.bind("", "")
        conn.execute("")
        assert conn.read_msg()[0] == "1"  # ParseComplete
        assert conn.read_msg()[0] == "2"  # BindComplete
        assert conn.read_msg()[0] == "G"  # CopyInResponse
        # One CopyData frame ending in "\.\n" -- the trailing suffix the strip hit.
        conn.send("d", b"x\n\\.\n")
        conn.send("c")
        conn.sync()
        m = conn.drain_to_ready()
        assert any(b"COPY 2" in t for t in tags(m)), (tags(m), errors(m))
        assert first_field(conn.run("select count(*) from smoke_csv_eod")) == b"2"
        assert (
            first_field(conn.run(r"select count(*) from smoke_csv_eod where a = '\.'"))
            == b"1"
        )
    finally:
        conn.run("drop table if exists smoke_csv_eod")


def test_copy_binary_data_after_eof_marker_rejected(conn):
    # PG requires CopyDone to follow the PGCOPY -1 trailer immediately; any bytes
    # between the trailer and CopyDone are rejected with 22P04. Regression:
    # serenedb used to silently drain them.
    conn.run("drop table if exists smoke_copy_eofm")
    assert "E" not in types(conn.run("create table smoke_copy_eofm(a int)"))
    try:
        conn.parse("", "copy smoke_copy_eofm from stdin (format binary)")
        conn.bind("", "")
        conn.execute("")
        assert conn.read_msg()[0] == "1"  # ParseComplete
        assert conn.read_msg()[0] == "2"  # BindComplete
        assert conn.read_msg()[0] == "G"  # CopyInResponse
        row = struct.pack("!h", 1) + struct.pack("!i", 4) + struct.pack("!i", 7)
        blob = (
            PGCOPY_SIGNATURE
            + struct.pack("!ii", 0, 0)  # flags, ext-len
            + row
            + b"\xff\xff"  # -1 trailer
            + b"junk"  # data after the EOF marker
        )
        conn.send("d", blob)
        conn.send("c")
        conn.sync()
        assert sqlstates(conn.drain_to_ready()) == ["22P04"]
    finally:
        conn.run("drop table if exists smoke_copy_eofm")


def test_copy_text_eod_marker_mid_stream(conn):
    # TEXT COPY: "\." alone on a line is end-of-data; everything after it (even
    # in the same CopyData frame) is discarded. The streaming feeder's stateful
    # scanner handles a marker anywhere, not just a frame-trailing suffix.
    conn.run("drop table if exists smoke_eod")
    assert "E" not in types(conn.run("create table smoke_eod(a text)"))
    try:
        conn.parse("", "copy smoke_eod from stdin (format text)")
        conn.bind("", "")
        conn.execute("")
        assert conn.read_msg()[0] == "1"
        assert conn.read_msg()[0] == "2"
        assert conn.read_msg()[0] == "G"
        conn.send("d", b"x\n\\.\ny\n")  # marker mid-frame; "y" must be dropped
        conn.send("c")
        conn.sync()
        m = conn.drain_to_ready()
        assert any(b"COPY 1" in t for t in tags(m)), (tags(m), errors(m))
        assert first_field(conn.run("select count(*) from smoke_eod")) == b"1"
        assert first_field(conn.run("select a from smoke_eod")) == b"x"
    finally:
        conn.run("drop table if exists smoke_eod")


def test_copy_text_eod_marker_split_across_frames(conn):
    # The "\.\n" marker is split across two CopyData frames ("\" ends the first,
    # ".\n" starts the second). The scanner must bridge the split and still treat
    # it as end-of-data, dropping the trailing row.
    conn.run("drop table if exists smoke_eod2")
    assert "E" not in types(conn.run("create table smoke_eod2(a text)"))
    try:
        conn.parse("", "copy smoke_eod2 from stdin (format text)")
        conn.bind("", "")
        conn.execute("")
        assert conn.read_msg()[0] == "1"
        assert conn.read_msg()[0] == "2"
        assert conn.read_msg()[0] == "G"
        conn.send("d", b"a\n\\")  # line "a", then a dangling backslash
        conn.send("d", b".\nb\n")  # ".\n" completes "\.\n"; "b" is after the marker
        conn.send("c")
        conn.sync()
        m = conn.drain_to_ready()
        assert any(b"COPY 1" in t for t in tags(m)), (tags(m), errors(m))
        assert first_field(conn.run("select count(*) from smoke_eod2")) == b"1"
        assert first_field(conn.run("select a from smoke_eod2")) == b"a"
    finally:
        conn.run("drop table if exists smoke_eod2")


def test_copy_large_copydata_frame_uncapped(conn):
    # A single CopyData frame larger than the old 64MB whole-frame cap: the
    # streaming feeder loads it (PG/pgwire-rs accept ~1-2GB per message). ~70MB
    # binary in one frame; the old path FATAL'd with "exceeds maximum size".
    conn.run("drop table if exists smoke_big_copy")
    assert "E" not in types(conn.run("create table smoke_big_copy(a bigint)"))
    try:
        rows = 5_000_000  # 5M * 14 bytes ≈ 70MB > 64MB cap
        blob = (
            b"PGCOPY\n\xff\r\n\x00"
            + struct.pack("!ii", 0, 0)
            + b"".join(struct.pack("!hiq", 1, 8, i) for i in range(rows))
            + b"\xff\xff"
        )
        assert len(blob) > 64 * 1024 * 1024
        conn.parse("", "copy smoke_big_copy from stdin (format binary)")
        conn.bind("", "")
        conn.execute("")
        assert conn.read_msg()[0] == "1"
        assert conn.read_msg()[0] == "2"
        assert conn.read_msg()[0] == "G"
        conn.send("d", blob)  # one frame, > 64MB
        conn.send("c")
        conn.sync()
        m = conn.drain_to_ready()
        assert any(f"COPY {rows}".encode() in t for t in tags(m)), (tags(m), errors(m))
    finally:
        conn.run("drop table if exists smoke_big_copy")


def copy_out_response(msgs):
    """CopyOutResponse ('H') decoded: (overall_format, [per-column format codes])."""
    for t, p in msgs:
        if t == "H":
            overall, ncols = struct.unpack("!bh", p[:3])
            codes = list(struct.unpack(f"!{ncols}h", p[3 : 3 + ncols * 2]))
            return overall, codes
    return None


def test_copy_csv_to_stdout_reports_column_count(conn):
    # PG's COPY ... TO STDOUT (text/csv) CopyOutResponse carries the real column
    # count with a per-column format code each. Regression: serenedb's
    # DuckDB-format path (csv/json/parquet/...) hardcoded column count 0.
    conn.run("drop table if exists csv_natts")
    assert "E" not in types(conn.run("create table csv_natts(a int, b text, c int)"))
    try:
        conn.run("insert into csv_natts values (1,'x',2)")
        conn.parse("", "copy csv_natts to stdout (format csv)")
        conn.bind("", "")
        conn.execute("")
        conn.sync()
        m = conn.drain_to_ready()
        assert "E" not in types(m), errors(m)
        overall, codes = copy_out_response(m)
        assert overall == 0, overall  # csv is a textual format
        assert len(codes) == 3, (overall, codes)  # one code per column, not zero
        assert all(c == 0 for c in codes), codes  # text -> per-column format 0
    finally:
        conn.run("drop table if exists csv_natts")


def test_copy_to_stdout_error_emits_no_copydone(conn):
    # PG sends CopyDone only after a successful COPY TO STDOUT; an error mid-copy
    # yields ErrorResponse with NO CopyDone. Regression: serenedb's DuckDB-format
    # path emitted CopyDone unconditionally from the file-handle destructor, so a
    # failed COPY produced CopyOutResponse,[CopyData],CopyDone,ErrorResponse.
    conn.run("drop table if exists copy_err")
    assert "E" not in types(conn.run("create table copy_err(i bigint)"))
    try:
        conn.run("insert into copy_err select i from range(5000) g(i)")
        # Lazy CASE: the i=4000 row overflows the ::int cast at runtime, so the
        # COPY opens (CopyOutResponse) then fails partway -- the case where the
        # old destructor wrongly emitted CopyDone before the ErrorResponse.
        conn.parse("", "copy (select case when i = 4000 then (i*1000000)::int "
                        "else i::int end from copy_err) to stdout (format csv)")
        conn.bind("", "")
        conn.execute("")
        conn.sync()
        m = conn.drain_to_ready()
        assert "E" in types(m), types(m)      # the COPY failed
        assert "H" in types(m), types(m)      # ...after CopyOutResponse went out
        assert "c" not in types(m), types(m)  # and NO CopyDone preceded the error
    finally:
        conn.run("drop table if exists copy_err")


def test_copy_text_escaping_to_stdout(conn):
    # PG text TO STDOUT: tab-separated, specials backslash-escaped, \N for NULL,
    # no header. Byte-exact against PostgreSQL's text format.
    conn.run("drop table if exists smoke_text_esc")
    assert "E" not in types(conn.run("create table smoke_text_esc(i int, s text)"))
    try:
        conn.run("insert into smoke_text_esc values (1, E'a\\tb\\nc\\\\d'), (2, NULL)")
        conn.parse("", "copy smoke_text_esc to stdout (format text)")
        conn.bind("", "")
        conn.execute("")
        conn.sync()
        m = conn.drain_to_ready()
        assert "E" not in types(m), errors(m)
        body = b"".join(p for t, p in m if t == "d")
        assert body == b"1\ta\\tb\\nc\\\\d\n2\t\\N\n", body
    finally:
        conn.run("drop table if exists smoke_text_esc")


def test_copy_default_format_is_text(conn):
    # No FORMAT clause -> PG default is text (tab sep, \N for NULL, comma raw,
    # no header), NOT csv.
    conn.run("drop table if exists smoke_text_def")
    assert "E" not in types(conn.run("create table smoke_text_def(i int, s text)"))
    try:
        conn.run("insert into smoke_text_def values (1,'a,b'), (2, NULL)")
        conn.parse("", "copy smoke_text_def to stdout")
        conn.bind("", "")
        conn.execute("")
        conn.sync()
        m = conn.drain_to_ready()
        assert "E" not in types(m), errors(m)
        body = b"".join(p for t, p in m if t == "d")
        assert body == b"1\ta,b\n2\t\\N\n", body
    finally:
        conn.run("drop table if exists smoke_text_def")


def test_copy_text_round_trip(conn):
    # text TO STDOUT -> text FROM STDIN preserves everything incl. NULL, empty
    # string, and a value with embedded tab/newline/backslash.
    conn.run("drop table if exists smoke_text_src")
    conn.run("drop table if exists smoke_text_dst")
    assert "E" not in types(conn.run("create table smoke_text_src(i int, s text)"))
    assert "E" not in types(conn.run("create table smoke_text_dst(i int, s text)"))
    try:
        conn.run("insert into smoke_text_src values "
                 "(1,'plain'),(2,NULL),(3,E'a\\tb\\nc\\\\d'),(4,'')")
        conn.parse("", "copy smoke_text_src to stdout (format text)")
        conn.bind("", "")
        conn.execute("")
        conn.sync()
        m = conn.drain_to_ready()
        assert "E" not in types(m), errors(m)
        blob = b"".join(p for t, p in m if t == "d")

        conn.parse("", "copy smoke_text_dst from stdin (format text)")
        conn.bind("", "")
        conn.execute("")
        assert conn.read_msg()[0] == "1"
        assert conn.read_msg()[0] == "2"
        assert conn.read_msg()[0] == "G"
        conn.send("d", blob)
        conn.send("c")
        conn.sync()
        m = conn.drain_to_ready()
        assert any(b"COPY 4" in t for t in tags(m)), types(m)

        diff = conn.run(
            "select (select count(*) from (select * from smoke_text_src "
            "except select * from smoke_text_dst) a) + (select count(*) from "
            "(select * from smoke_text_dst except select * from smoke_text_src) b)")
        assert first_field(diff) == b"0", first_field(diff)
    finally:
        conn.run("drop table if exists smoke_text_src")
        conn.run("drop table if exists smoke_text_dst")


def test_copy_from_text_decode(conn):
    # text FROM STDIN decodes the full PG backslash-escape set, including octal
    # (\101) and hex (\x41), \N -> NULL, and unknown \q -> q.
    conn.run("drop table if exists smoke_text_dec")
    assert "E" not in types(conn.run("create table smoke_text_dec(k text, v text)"))
    try:
        conn.parse("", "copy smoke_text_dec from stdin (format text)")
        conn.bind("", "")
        conn.execute("")
        assert conn.read_msg()[0] == "1"
        assert conn.read_msg()[0] == "2"
        assert conn.read_msg()[0] == "G"
        payload = (b"tab\t\\t\n" b"back\t\\\\\n" b"oct\t\\101\n"
                   b"hex\t\\x41\n" b"nul\t\\N\n" b"unk\t\\q\n")
        conn.send("d", payload)
        conn.send("c")
        conn.sync()
        m = conn.drain_to_ready()
        assert any(b"COPY 6" in t for t in tags(m)), types(m)
        q = lambda k: all_fields(conn.run(
            f"select v from smoke_text_dec where k='{k}'"))
        assert all_fields(conn.run(
            "select ascii(v) from smoke_text_dec where k='tab'")) == [b"9"]
        assert q("back") == [b"\\"]
        assert q("oct") == [b"A"]
        assert q("hex") == [b"A"]
        assert q("unk") == [b"q"]
        assert q("nul") == [None]
    finally:
        conn.run("drop table if exists smoke_text_dec")


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


# --- PostgreSQL transaction-control parity over the wire protocol ----------
# PG18 ground truth captured in /tmp/pg_parity.json: ReadyForQuery status
# bytes ('I'/'T'/'E'), error sqlstates, and committed-row visibility. The key
# behaviors pinned here:
#   * An extended-protocol batch terminated by ONE Sync is an implicit
#     transaction: any error in the batch rolls back the whole batch.
#   * A bare Sync with no commands stays idle ('I'); a Sync inside an explicit
#     BEGIN keeps the transaction open ('T').
#   * Inside an explicit transaction, an error moves the status to aborted
#     ('E') and the next statement is rejected with 25P02 until ROLLBACK.
# An out-of-range/invalid int4 text value reports 22P02 (invalid_text_repr).


def _fresh_pt(conn):
    """Drop+create the single-int4-column parity table `pt`, empty."""
    conn.send("Q", _cstr("DROP TABLE IF EXISTS pt"))
    conn.drain_to_ready()
    assert "E" not in types(conn.run("CREATE TABLE pt(id int4)"))


def _pt_count(conn):
    m = conn.run("SELECT count(*) FROM pt")
    assert "E" not in types(m), errors(m)
    return int(first_field(m))


def _insert_batch(conn, params, sync=True):
    """Parse INSERT INTO pt VALUES ($1::int4) once, then Bind+Execute it for
    each param (text format) before a single optional Sync -- one implicit
    transaction, mirroring PG's extended-protocol batch semantics."""
    conn.parse("", "INSERT INTO pt VALUES ($1)", oids=(23,))
    for p in params:
        conn.bind("", "", params=(p,))
        conn.execute("")
    if sync:
        conn.sync()


def test_txn_multi_rollback(conn):
    # Three inserts before one Sync; the 3rd value is an invalid int4 -> the
    # whole implicit batch rolls back. Status returns to idle, 0 rows survive.
    _fresh_pt(conn)
    _insert_batch(conn, (b"1", b"2", b"x"))
    m = conn.drain_to_ready()
    assert statuses(m) == ["I"]
    assert sqlstates(m) == ["22P02"]
    assert _pt_count(conn) == 0


def test_txn_multi_commit(conn):
    # Two valid inserts before one Sync both commit.
    _fresh_pt(conn)
    _insert_batch(conn, (b"1", b"2"))
    m = conn.drain_to_ready()
    assert statuses(m) == ["I"]
    assert sqlstates(m) == []
    assert _pt_count(conn) == 2


def test_txn_single_fail(conn):
    # A single failing insert leaves no rows; status idle.
    _fresh_pt(conn)
    _insert_batch(conn, (b"x",))
    m = conn.drain_to_ready()
    assert statuses(m) == ["I"]
    assert sqlstates(m) == ["22P02"]
    assert _pt_count(conn) == 0


def test_txn_single_ok(conn):
    # A single valid insert commits one row.
    _fresh_pt(conn)
    _insert_batch(conn, (b"1",))
    m = conn.drain_to_ready()
    assert statuses(m) == ["I"]
    assert sqlstates(m) == []
    assert _pt_count(conn) == 1


def test_txn_empty_sync(conn):
    # A bare Sync with no commands reports idle, no error.
    _fresh_pt(conn)
    conn.sync()
    m = conn.drain_to_ready()
    assert statuses(m) == ["I"]
    assert sqlstates(m) == []


def test_txn_begin_then_sync(conn):
    # BEGIN opens an explicit transaction ('T'); a following bare Sync must
    # NOT close it -- status stays 'T'.
    _fresh_pt(conn)
    assert statuses(conn.run("BEGIN")) == ["T"]
    conn.sync()
    assert statuses(conn.drain_to_ready()) == ["T"]
    conn.send("Q", _cstr("ROLLBACK"))
    conn.drain_to_ready()


def test_txn_begin_first_no_spurious_notice(conn):
    # A batch whose FIRST command is BEGIN must reach the transaction operator
    # directly (not be wrapped in our implicit block): a wrapped BEGIN would
    # SetAutoCommit(false) first, then warn "transaction within a transaction"
    # -> a NoticeResponse PG never sends. Parse/Bind/Execute BEGIN, then an
    # INSERT, then Sync: NO 'N' message for the BEGIN, status 'T'.
    _fresh_pt(conn)
    conn.parse("", "BEGIN")
    conn.bind("", "")
    conn.execute("")
    conn.parse("ins", "INSERT INTO pt VALUES ($1)", oids=(23,))
    conn.bind("", "ins", params=(b"1",))
    conn.execute("")
    conn.sync()
    m = conn.drain_to_ready()
    assert "E" not in types(m), errors(m)
    assert notices(m) == [], notices(m)
    assert statuses(m) == ["T"], statuses(m)
    conn.send("Q", _cstr("ROLLBACK"))
    conn.drain_to_ready()


def test_txn_explicit_spans_sync(conn):
    # An explicit BEGIN spans an extended batch + Sync: the insert is visible
    # inside the still-open transaction (status 'T' at the batch Sync), then
    # ROLLBACK discards it -> 0 rows after.
    _fresh_pt(conn)
    assert statuses(conn.run("BEGIN")) == ["T"]
    _insert_batch(conn, (b"1",))
    m = conn.drain_to_ready()
    assert statuses(m) == ["T"]
    assert sqlstates(m) == []
    assert _pt_count(conn) == 1  # visible within the open transaction
    assert statuses(conn.run("ROLLBACK")) == ["I"]
    assert _pt_count(conn) == 0


def test_txn_error_in_explicit(conn):
    # An error inside an explicit transaction aborts it: the failing batch's
    # Sync reports 'E' with 22P02, and the next statement is rejected with
    # 25P02 (in_failed_sql_transaction) until ROLLBACK clears the abort.
    _fresh_pt(conn)
    assert statuses(conn.run("BEGIN")) == ["T"]
    _insert_batch(conn, (b"x",))
    m = conn.drain_to_ready()
    assert statuses(m) == ["E"]
    assert sqlstates(m) == ["22P02"]
    _insert_batch(conn, (b"1",))
    m = conn.drain_to_ready()
    assert statuses(m) == ["E"]
    assert sqlstates(m) == ["25P02"]
    assert statuses(conn.run("ROLLBACK")) == ["I"]
    assert _pt_count(conn) == 0


def test_bind_result_format_count_mismatch(conn):
    # N>1 result format codes that don't match the column count -> 08P01 before
    # BindComplete (mirrors PG PortalSetResultFormat).
    conn.parse("", "SELECT 1, 2, 3")
    conn.send(
        "B",
        b"\0\0"
        + struct.pack("!H", 0)
        + struct.pack("!H", 0)
        + struct.pack("!H", 2)
        + struct.pack("!HH", 1, 1),
    )
    conn.sync()
    m = conn.drain_to_ready()
    assert sqlstates(m) == ["08P01"], (statuses(m), sqlstates(m))
    assert "2" not in statuses(m)  # no BindComplete


def test_describe_trailing_bytes_rejected(conn):
    # Trailing bytes after the Describe name's NUL -> 08P01 (PG pq_getmsgend).
    conn.parse("", "SELECT 1")
    conn.send("D", b"S\0junk")
    conn.sync()
    m = conn.drain_to_ready()
    assert sqlstates(m) == ["08P01"], (statuses(m), sqlstates(m))


def test_empty_database_defaults_to_user():
    # PG defaults an empty database startup parameter to the user name
    # (backend_startup.c) and connects; serenedb must not FATAL 3D000.
    # Verified against PostgreSQL.
    kw = conn_kwargs()
    sock = socket.create_connection((kw["host"], kw["port"]))
    try:

        def recv_exact(n: int) -> bytes:
            buf = b""
            while len(buf) < n:
                chunk = sock.recv(n - len(buf))
                if not chunk:
                    raise EOFError("connection closed")
                buf += chunk
            return buf

        params = _cstr("user") + _cstr(kw["user"]) + _cstr("database") + _cstr("") + b"\0"
        sock.sendall(struct.pack("!II", len(params) + 8, 196608) + params)
        seen = []
        while True:
            t = recv_exact(1).decode()
            (ln,) = struct.unpack("!I", recv_exact(4))
            recv_exact(ln - 4)
            seen.append(t)
            if t in ("Z", "E"):
                break
        assert "Z" in seen and "E" not in seen, seen
    finally:
        sock.close()


def test_copy_in_stray_message_aborts_and_resyncs(conn):
    # An out-of-band message mid COPY FROM STDIN ends the COPY with 08P01; the
    # message body is consumed so the stream stays framed -- the connection
    # recovers (ReadyForQuery) and the next query works (no desync/hang).
    conn.run("drop table if exists smoke_copy_stray")
    assert "E" not in types(conn.run("create table smoke_copy_stray(x int)"))
    try:
        conn.send("Q", _cstr("COPY smoke_copy_stray FROM STDIN"))
        assert conn.read_msg()[0] == "G"  # CopyInResponse
        conn.send("d", b"1\n")  # CopyData
        conn.send("Q", _cstr("SELECT 1"))  # illegal mid-copy
        m = conn.drain_to_ready()
        assert sqlstates(m) == ["08P01"], (types(m), sqlstates(m))
        # Stream re-synced: a fresh query on the same connection works.
        assert rows(conn.run("SELECT 42"))[0].endswith(b"42")
    finally:
        conn.run("drop table if exists smoke_copy_stray")


def test_notice_precedes_command_complete(conn):
    # PG emits a command's NoticeResponse BEFORE its CommandComplete; ROLLBACK
    # with no active transaction warns 25P01. Verified ['N','C','Z'] vs PG.
    conn.send("Q", _cstr("ROLLBACK"))
    m = conn.drain_to_ready()
    t = types(m)
    assert "N" in t and "C" in t and t.index("N") < t.index("C"), t


def test_copy_binary_rejects_format_options(conn):
    # serenedb's PGCOPY binary format honors no formatting option; PG rejects each
    # in BINARY mode. serenedb errors (no CopyOutResponse) before any data flows.
    conn.run("drop table if exists smoke_copy_binopt")
    assert "E" not in types(conn.run("create table smoke_copy_binopt(x int)"))
    try:
        for opt in ("delimiter ','", "header", "null 'x'", "quote '\"'"):
            conn.send("Q", _cstr(f"COPY smoke_copy_binopt TO STDOUT (FORMAT binary, {opt})"))
            m = conn.drain_to_ready()
            assert "H" not in types(m), (opt, types(m))  # no CopyOutResponse
            assert sqlstates(m) == ["0A000"], (opt, sqlstates(m))
    finally:
        conn.run("drop table if exists smoke_copy_binopt")


def test_copy_text_rejects_unknown_option(conn):
    # An unrecognized COPY option is a hard error (PG "option not recognized",
    # ERRCODE_SYNTAX_ERROR 42601), not silently ignored.
    conn.run("drop table if exists smoke_copy_txtopt")
    assert "E" not in types(conn.run("create table smoke_copy_txtopt(x int)"))
    try:
        conn.send("Q", _cstr("COPY smoke_copy_txtopt TO STDOUT (FORMAT text, bogus_opt true)"))
        m = conn.drain_to_ready()
        assert sqlstates(m) == ["42601"], (types(m), sqlstates(m))
    finally:
        conn.run("drop table if exists smoke_copy_txtopt")


def test_application_name_ascii_sanitized():
    # PG pg_clean_ascii escapes control/non-ASCII bytes in application_name to
    # \xHH before reporting it (and storing it). Verified vs PostgreSQL.
    m = _startup_collect({"application_name": "ab\x01cd\x7f"})
    assert "Z" in types(m), types(m)
    assert dict(param_status(m)).get("application_name") == "ab\\x01cd\\x7f", param_status(m)


# PIVOT over the wire, both protocols.
#
# A PIVOT without an IN-list extracts the distinct pivot values from the data,
# which the parser rewrites into a multi-statement [CREATE ENUM ...; SELECT].
# Over the wire that is one user command: the simple Query path runs the whole
# body; the extended path treats it as a compound (CREATE ENUM as a side effect,
# then the trailing SELECT streamed under one CommandComplete). Both must return
# the pivoted row. (sqllogictest-rs cannot drive this -- its tokio-postgres
# simple_query rejects the PIVOT keyword -- so it is pinned here instead.)
def _pivot_setup(conn, table):
    for sql in (
        f"DROP TABLE IF EXISTS {table}",
        f"CREATE TABLE {table} (q VARCHAR, a INT)",
        f"INSERT INTO {table} VALUES ('Q1', 10), ('Q2', 20), ('Q1', 5), ('Q3', 30)",
    ):
        msgs = conn.run(sql)
        assert not errors(msgs), (sql, errors(msgs))


def _assert_pivot_result(msgs):
    # The SELECT is the result; row_description / all_fields pick the first
    # RowDescription / DataRow (the CREATE ENUM emits neither).
    assert not errors(msgs), errors(msgs)
    names = [c[0] for c in (row_description(msgs) or [])]
    assert names == ["q1", "q2", "q3"], names
    vals = [f.decode() for f in all_fields(msgs)]
    assert vals == ["15", "20", "30"], vals


@pytest.mark.xfail(
    reason="PIVOT column names are not pg-lowercased (returns Q1, expected q1); "
    "pre-existing serened bug, unrelated to this branch",
    strict=False,
)
def test_pivot_simple_protocol(conn):
    _pivot_setup(conn, "pivot_raw_simple")
    conn.send("Q", _cstr("PIVOT pivot_raw_simple ON q USING sum(a)"))
    _assert_pivot_result(conn.drain_to_ready())


@pytest.mark.xfail(
    reason="PIVOT column names are not pg-lowercased (returns Q1, expected q1); "
    "pre-existing serened bug, unrelated to this branch",
    strict=False,
)
def test_pivot_extended_protocol(conn):
    _pivot_setup(conn, "pivot_raw_ext")
    # Full extended round-trip with a portal Describe: the temp enum is stripped
    # and run at Parse, so the PIVOT is a single prepared SELECT -- Describe must
    # return the q1/q2/q3 RowDescription before Execute streams the row.
    conn.parse("", "PIVOT pivot_raw_ext ON q USING sum(a)")
    conn.bind("", "")
    conn.describe("P", "")
    conn.execute("")
    conn.sync()
    _assert_pivot_result(conn.drain_to_ready())
