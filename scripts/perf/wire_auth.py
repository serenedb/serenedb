#!/usr/bin/env python3
# Minimal pg-wire startup + authentication for the raw-socket perf clients
# (wire_serialize_drain.py, wire_copy_in.py). The query workloads go through
# pgbench/libpq, which honors PGPASSWORD; these hand-rolled byte-counting clients
# used to do a trust-only startup, so any password-required engine (e.g. CedarDB)
# left them blocked on the AuthenticationRequest. This responds to whatever the
# server asks: trust (AuthenticationOk), cleartext, MD5, or SCRAM-SHA-256.
import base64
import hashlib
import hmac
import os
import struct


def _recv_message(sock, buf):
    """Return (type:bytes1, payload:bytes, rest:bytes), reading as needed."""
    while True:
        if len(buf) >= 5:
            typ = buf[0:1]
            (ln,) = struct.unpack("!i", buf[1:5])
            if len(buf) >= 1 + ln:
                return typ, buf[5 : 1 + ln], buf[1 + ln :]
        data = sock.recv(65536)
        if not data:
            raise RuntimeError("eof during startup")
        buf += data


def _error_message(payload):
    off = 0
    while off < len(payload) and payload[off : off + 1] != b"\x00":
        code = payload[off : off + 1]
        end = payload.index(b"\x00", off + 1)
        if code == b"M":
            return payload[off + 1 : end].decode("ascii", "replace")
        off = end + 1
    return "unknown error"


def _password_message(body):
    return b"p" + struct.pack("!i", 4 + len(body)) + body


def _scram(sock, buf, password):
    # SCRAM-SHA-256 (RFC 5802 + RFC 7677), channel-binding-free (gs2 'n,,').
    cnonce = base64.b64encode(os.urandom(18)).decode("ascii")
    first_bare = f"n=,r={cnonce}"
    initial = b"SCRAM-SHA-256\x00" + struct.pack("!i", len(first_bare) + 3) + b"n,," + first_bare.encode()
    sock.sendall(_password_message(initial))

    typ, payload, buf = _recv_message(sock, buf)
    if typ == b"E":
        raise RuntimeError(_error_message(payload))
    # AuthenticationSASLContinue: 'R' + int32 code(11) + server-first
    (code,) = struct.unpack("!i", payload[:4])
    server_first = payload[4:].decode("ascii")
    attrs = dict(p.split("=", 1) for p in server_first.split(","))
    rnonce, salt, iters = attrs["r"], base64.b64decode(attrs["s"]), int(attrs["i"])
    if not rnonce.startswith(cnonce):
        raise RuntimeError("scram nonce mismatch")

    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iters)
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    final_bare = f"c=biws,r={rnonce}"
    auth_msg = f"{first_bare},{server_first},{final_bare}".encode()
    client_sig = hmac.new(stored_key, auth_msg, hashlib.sha256).digest()
    proof = base64.b64encode(bytes(a ^ b for a, b in zip(client_key, client_sig))).decode("ascii")
    final = f"{final_bare},p={proof}".encode()
    sock.sendall(_password_message(final))
    return buf


def startup(sock, user, db, password):
    """Send StartupMessage, satisfy the auth handshake, return once the first
    ReadyForQuery arrives. Raises RuntimeError on auth/protocol error."""
    params = b"user\x00" + user.encode() + b"\x00database\x00" + db.encode() + b"\x00\x00"
    sock.sendall(struct.pack("!ii", 8 + len(params), 196608) + params)
    buf = b""
    while True:
        typ, payload, buf = _recv_message(sock, buf)
        if typ == b"E":
            raise RuntimeError(_error_message(payload))
        if typ == b"Z":  # ReadyForQuery: startup complete
            return buf
        if typ != b"R":  # ParameterStatus / BackendKeyData / NoticeResponse
            continue
        (code,) = struct.unpack("!i", payload[:4])
        if code == 0:  # AuthenticationOk (trust)
            continue
        if code == 3:  # AuthenticationCleartextPassword
            sock.sendall(_password_message(password.encode() + b"\x00"))
        elif code == 5:  # AuthenticationMD5Password
            salt = payload[4:8]
            inner = hashlib.md5(password.encode() + user.encode()).hexdigest()
            token = b"md5" + hashlib.md5(inner.encode() + salt).hexdigest().encode()
            sock.sendall(_password_message(token + b"\x00"))
        elif code == 10:  # AuthenticationSASL -> SCRAM-SHA-256
            buf = _scram(sock, buf, password)
        else:
            raise RuntimeError(f"unsupported auth code {code}")


def env_creds():
    return (
        os.environ.get("RUN_ONE_USER") or "postgres",
        os.environ.get("RUN_ONE_DB") or "postgres",
        os.environ.get("PGPASSWORD") or "",
    )
