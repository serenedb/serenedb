"""What the fake object stores share: a threaded HTTP server in the test process
that counts requests by kind, a ranged read held in the middle of its body
(`hold_read`), and the path, date, listing and ranged-read rules S3 and Azure
Blob Storage have in common.
"""

from __future__ import annotations

import collections
import email.utils
import http.server
import threading
import time
import urllib.parse
from typing import Self


def http_date(seconds: float) -> str:
    return email.utils.formatdate(seconds, usegmt=True)


def split_path(path: str, account: str = "") -> tuple[str, str]:
    path = urllib.parse.unquote(path).lstrip("/")
    head, _, rest = path.partition("/")
    container, _, name = (rest if account and head == account else path).partition("/")
    return container, name


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True
    account = ""
    fake: FakeStore

    def log_message(self, *args) -> None:
        pass

    def _target(self) -> tuple[str, str, dict[str, list[str]]]:
        url = urllib.parse.urlsplit(self.path)
        container, name = split_path(url.path, self.account)
        return container, name, urllib.parse.parse_qs(url.query, keep_blank_values=True)

    def _body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length) if length else b""

    def _send_held(self, name: str, body: bytes) -> bytes:
        """Sends the start of [body] when the read of [name] is the held one, and returns the
        rest once it is resumed."""
        with self.fake.lock:
            held = self.fake.held_read
            if held is None or held[0] not in name or len(body) <= held[1]:
                return body
            self.fake.held_read = None
        self.wfile.write(body[:held[1]])
        self.wfile.flush()
        self.fake.read_held.set()
        if not self.fake.read_resumed.wait(10):
            self.fake.read_held.clear()
        return body[held[1]:]

    def _send_body(self, body: bytes, trickle: bool, held: str | None) -> None:
        if not body or self.command == "HEAD":
            return
        if held is not None:
            body = self._send_held(held, body)
        rate = self.fake.trickle if trickle else 0
        if not rate:
            self.wfile.write(body)
            return
        chunk = max(1, rate // 10)
        try:
            for offset in range(0, len(body), chunk):
                self.wfile.write(body[offset:offset + chunk])
                self.wfile.flush()
                time.sleep(chunk / rate)
        except OSError:
            self.close_connection = True

    def _dropped(self, kind: str, name: str) -> bool:
        with self.fake.lock:
            if self.fake.drop[kind] == 0:
                return False
            self.fake.drop[kind] -= 1
            self.fake.dropped.append(name)
        self.close_connection = True
        return True


class _Server(http.server.ThreadingHTTPServer):
    request_queue_size = 1024
    daemon_threads = True


class FakeStore:
    def __init__(self, handler: type[Handler], port: int = 0) -> None:
        self.lock = threading.Lock()
        self.requests: collections.Counter[str] = collections.Counter()
        self.held_read: tuple[str, int] | None = None
        self.read_held = threading.Event()
        self.read_resumed = threading.Event()
        self.trickle = 0
        self.drop: collections.Counter[str] = collections.Counter()
        self.dropped: list[str] = []
        store = self

        class BoundHandler(handler):
            fake = store

        self.server = _Server(("127.0.0.1", port), BoundHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> Self:
        self.thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.server.shutdown()
        self.server.server_close()

    def count(self, kind: str) -> None:
        with self.lock:
            self.requests[kind] += 1

    def hold_read(self, name: str, after: int) -> None:
        """The next ranged read of more than [after] bytes of an object whose name contains [name]
        sends [after] of them, sets `read_held`, and sends the rest once `read_resumed` is set. It
        gives up after ten seconds, before a client's deadline, and clears `read_held`: what
        returns while `read_held` is set did not wait for the rest."""
        self.read_held.clear()
        self.read_resumed.clear()
        with self.lock:
            self.held_read = (name, after)


def list_page(names: list[str], prefix: str, delimiter: str, after: str,
              limit: int) -> tuple[list[tuple[str, bool]], bool]:
    entries: list[tuple[str, bool]] = []
    for name in names:
        cut = name.find(delimiter, len(prefix)) if delimiter else -1
        entry = (name[:cut + len(delimiter)], True) if cut != -1 else (name, False)
        if (after and entry[0] <= after) or (entries and entries[-1] == entry):
            continue
        entries.append(entry)
    return entries[:limit], len(entries) > limit


def byte_range(header: str, data: bytes) -> tuple[bytes, str] | None:
    first, _, last = header.removeprefix("bytes=").partition("-")
    start = int(first)
    if start >= len(data):
        return None
    end = min(int(last) if last else len(data) - 1, len(data) - 1)
    return data[start:end + 1], f"bytes {start}-{end}/{len(data)}"
