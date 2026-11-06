"""What the fake object stores share: a threaded HTTP server in the test process
that counts requests by kind, and the listing and ranged-read rules S3 and Azure
Blob Storage have in common.
"""

from __future__ import annotations

import collections
import http.server
import threading
from typing import Self


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True
    fake: FakeStore

    def log_message(self, *args) -> None:
        pass

    def _body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length) if length else b""


class _Server(http.server.ThreadingHTTPServer):
    request_queue_size = 1024
    daemon_threads = True


class FakeStore:
    def __init__(self, handler: type[Handler], port: int = 0) -> None:
        self.lock = threading.Lock()
        self.requests: collections.Counter[str] = collections.Counter()
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
