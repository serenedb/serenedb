"""An Azure Blob Storage endpoint in the test process, for the Azure client without a real store.

Path-style URLs under one storage account, containers that exist on first use, block blobs
staged and committed by block lists, ranged reads with conditions, blob listings, same-account
copies, single and batched deletes. Requests are not authenticated, except that while
`sas_signatures` is set, a request whose SAS signature is not one of them is refused with 403
AuthenticationFailed, as an expired SAS is. It counts requests by kind; `latency` delays every
request.

Point a secret at it with `connection_string`, or with `sas_connection_string(signature)`.
"""

from __future__ import annotations

import base64
import collections
import email.utils
import hashlib
import http.server
import threading
import time
import urllib.parse
import uuid
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

ACCOUNT = "devstoreaccount1"
KEY = base64.b64encode(b"fake-azure-account-key-for-tests").decode()


class _Blob:
    def __init__(self, data: bytes, blocks: dict[str, bytes] | None = None) -> None:
        self.data = data
        self.blocks = blocks or {}
        self.copy_id = ""
        self.etag = '"0x' + hashlib.md5(data + uuid.uuid4().bytes).hexdigest()[:16].upper() + '"'
        self.modified = time.time()


class FakeAzure:
    def __init__(self, latency: float = 0.0, port: int = 0) -> None:
        self.latency = latency
        self.sas_signatures: set[str] | None = None
        self.lock = threading.Lock()
        self.blobs: dict[tuple[str, str], _Blob] = {}
        self.staged: dict[tuple[str, str], dict[str, bytes]] = {}
        self.requests: collections.Counter[str] = collections.Counter()
        store = self

        class Handler(_Handler):
            fake = store

        self.server = _Server(("127.0.0.1", port), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}/{ACCOUNT}"

    @property
    def connection_string(self) -> str:
        return (f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT};AccountKey={KEY};"
                f"BlobEndpoint={self.endpoint};")

    def sas_connection_string(self, signature: str) -> str:
        return (f"BlobEndpoint={self.endpoint};SharedAccessSignature=sv=2022-11-02&ss=b&srt=sco"
                f"&sp=rwdlacx&se=2099-12-31T00:00:00Z&sig={signature}")

    def __enter__(self) -> FakeAzure:
        self.thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.server.shutdown()
        self.server.server_close()

    def names(self, container: str, prefix: str = "") -> list[str]:
        with self.lock:
            return sorted(name for c, name in self.blobs
                          if c == container and name.startswith(prefix))

    def replace(self, container: str, name: str, data: bytes) -> None:
        with self.lock:
            self.blobs[(container, name)] = _Blob(data)

    def count(self, kind: str) -> None:
        with self.lock:
            self.requests[kind] += 1


class _Server(http.server.ThreadingHTTPServer):
    request_queue_size = 1024


def _http_date(seconds: float) -> str:
    return email.utils.formatdate(seconds, usegmt=True)


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True
    fake: FakeAzure

    def log_message(self, *args) -> None:
        pass

    def _target(self) -> tuple[str, str, dict[str, list[str]]]:
        url = urllib.parse.urlsplit(self.path)
        path = urllib.parse.unquote(url.path).lstrip("/")
        account, _, rest = path.partition("/")
        if account != ACCOUNT:
            rest = path
        container, _, name = rest.partition("/")
        query = urllib.parse.parse_qs(url.query, keep_blank_values=True)
        return container, name, query

    def _body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length) if length else b""

    def _wait(self) -> None:
        if self.fake.latency:
            time.sleep(self.fake.latency)

    def _forbidden(self) -> bool:
        signatures = self.fake.sas_signatures
        if signatures is None:
            return False
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        if query.get("sig", [""])[0] in signatures:
            return False
        self.fake.count("FORBIDDEN")
        self._body()
        self._error(403, "AuthenticationFailed")
        return True

    def _reply(self, status: int, body: bytes = b"", headers: dict[str, str] | None = None,
               content_length: int | None = None) -> None:
        self.send_response(status)
        self.send_header("x-ms-request-id", str(uuid.uuid4()))
        self.send_header("x-ms-version", self.headers.get("x-ms-version", "2026-06-06"))
        self.send_header("Date", _http_date(time.time()))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body) if content_length is None else content_length))
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def _error(self, status: int, code: str) -> None:
        if self.command == "HEAD":
            self._reply(status, headers={"x-ms-error-code": code})
            return
        body = (f'<?xml version="1.0" encoding="utf-8"?><Error><Code>{code}</Code>'
                f"<Message>{code}</Message></Error>").encode()
        self._reply(status, body, {"x-ms-error-code": code, "Content-Type": "application/xml"})

    def _find(self, container: str, name: str) -> _Blob | None:
        with self.fake.lock:
            return self.fake.blobs.get((container, name))

    def _properties(self, blob: _Blob) -> dict[str, str]:
        headers = {"ETag": blob.etag, "Last-Modified": _http_date(blob.modified),
                   "x-ms-creation-time": _http_date(blob.modified), "x-ms-blob-type": "BlockBlob",
                   "x-ms-server-encrypted": "true", "Accept-Ranges": "bytes",
                   "Content-Type": "application/octet-stream"}
        if blob.copy_id:
            headers["x-ms-copy-id"] = blob.copy_id
            headers["x-ms-copy-status"] = "success"
        return headers

    def _conditions_fail(self, blob: _Blob) -> bool:
        if_match = self.headers.get("If-Match")
        return bool(if_match) and if_match != "*" and if_match != blob.etag

    def _occupied(self, container: str, name: str) -> bool:
        return self.headers.get("If-None-Match") == "*" and (container, name) in self.fake.blobs

    def do_HEAD(self) -> None:
        if self._forbidden():
            return
        container, name, _ = self._target()
        self.fake.count("HEAD")
        self._wait()
        blob = self._find(container, name)
        if blob is None:
            self._error(404, "BlobNotFound")
            return
        self._reply(200, headers=self._properties(blob), content_length=len(blob.data))

    def do_GET(self) -> None:
        if self._forbidden():
            return
        container, name, query = self._target()
        if query.get("comp", [""])[0] == "list":
            self.fake.count("LIST")
            self._wait()
            self._list(container, query)
            return
        self.fake.count("GET")
        self._wait()
        blob = self._find(container, name)
        if blob is None:
            self._error(404, "BlobNotFound")
            return
        if self._conditions_fail(blob):
            self._error(412, "ConditionNotMet")
            return
        headers = self._properties(blob)
        ranged = self.headers.get("x-ms-range") or self.headers.get("Range")
        if not ranged:
            self._reply(200, blob.data, headers)
            return
        first, _, last = ranged.removeprefix("bytes=").partition("-")
        start = int(first)
        end = min(int(last) if last else len(blob.data) - 1, len(blob.data) - 1)
        if start >= len(blob.data):
            self._error(416, "InvalidRange")
            return
        headers["Content-Range"] = f"bytes {start}-{end}/{len(blob.data)}"
        self._reply(206, blob.data[start:end + 1], headers)

    def _list(self, container: str, query: dict[str, list[str]]) -> None:
        prefix = query.get("prefix", [""])[0]
        delimiter = query.get("delimiter", [""])[0]
        marker = query.get("marker", [""])[0]
        max_results = int(query.get("maxresults", ["5000"])[0])
        entries: list[tuple[str, bool]] = []
        for name in self.fake.names(container, prefix):
            cut = name.find(delimiter, len(prefix)) if delimiter else -1
            entry = (name[:cut + len(delimiter)], True) if cut != -1 else (name, False)
            if (marker and entry[0] <= marker) or (entries and entries[-1] == entry):
                continue
            entries.append(entry)
        page, rest = entries[:max_results], entries[max_results:]
        body = [f'<?xml version="1.0" encoding="utf-8"?><EnumerationResults '
                f'ServiceEndpoint="{escape(self.fake.endpoint)}" ContainerName="{escape(container)}">'
                f"<Prefix>{escape(prefix)}</Prefix><MaxResults>{max_results}</MaxResults>"]
        if delimiter:
            body.append(f"<Delimiter>{escape(delimiter)}</Delimiter>")
        body.append("<Blobs>")
        for name, is_prefix in page:
            if is_prefix:
                body.append(f"<BlobPrefix><Name>{escape(name)}</Name></BlobPrefix>")
                continue
            blob = self._find(container, name)
            if blob is None:
                continue
            modified = _http_date(blob.modified)
            body.append(f"<Blob><Name>{escape(name)}</Name><Properties>"
                        f"<Creation-Time>{modified}</Creation-Time><Last-Modified>{modified}</Last-Modified>"
                        f"<Etag>{escape(blob.etag)}</Etag><Content-Length>{len(blob.data)}</Content-Length>"
                        "<Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType>"
                        "<ServerEncrypted>true</ServerEncrypted></Properties></Blob>")
        body.append("</Blobs>")
        body.append(f"<NextMarker>{escape(page[-1][0]) if rest else ''}</NextMarker></EnumerationResults>")
        self._reply(200, "".join(body).encode(), {"Content-Type": "application/xml"})

    def do_PUT(self) -> None:
        if self._forbidden():
            return
        container, name, query = self._target()
        body = self._body()
        comp = query.get("comp", [""])[0]
        if comp == "block":
            self.fake.count("PUT_BLOCK")
            self._wait()
            with self.fake.lock:
                self.fake.staged.setdefault((container, name), {})[query["blockid"][0]] = body
            self._reply(201, headers={"x-ms-request-server-encrypted": "true"})
            return
        if comp == "blocklist":
            self.fake.count("PUT_BLOCK_LIST")
            self._wait()
            ids = [element.text or "" for element in ET.fromstring(body)]
            with self.fake.lock:
                exists = self._occupied(container, name)
                blocks = dict(getattr(self.fake.blobs.get((container, name)), "blocks", {}))
                blocks.update(self.fake.staged.get((container, name), {}))
                if exists or any(block not in blocks for block in ids):
                    blob = None
                else:
                    blob = _Blob(b"".join(blocks[block] for block in ids),
                                 {block: blocks[block] for block in ids})
                    self.fake.blobs[(container, name)] = blob
                    self.fake.staged.pop((container, name), None)
            if exists:
                self._error(409, "BlobAlreadyExists")
                return
            if blob is None:
                self._error(400, "InvalidBlockList")
                return
            self._reply(201, headers={"ETag": blob.etag, "Last-Modified": _http_date(blob.modified),
                                      "x-ms-request-server-encrypted": "true"})
            return
        source = self.headers.get("x-ms-copy-source")
        if source:
            self.fake.count("COPY")
            self._wait()
            path = urllib.parse.unquote(urllib.parse.urlsplit(source).path).lstrip("/")
            account, _, rest = path.partition("/")
            if account != ACCOUNT:
                rest = path
            src_container, _, src_name = rest.partition("/")
            original = self._find(src_container, src_name)
            if original is None:
                self._error(404, "CannotVerifyCopySource")
                return
            blob = _Blob(original.data, original.blocks)
            blob.copy_id = str(uuid.uuid4())
            with self.fake.lock:
                self.fake.blobs[(container, name)] = blob
            self._reply(202, headers={"ETag": blob.etag, "Last-Modified": _http_date(blob.modified),
                                      "x-ms-copy-id": blob.copy_id, "x-ms-copy-status": "success"})
            return
        self.fake.count("PUT")
        self._wait()
        blob = _Blob(body)
        with self.fake.lock:
            exists = self._occupied(container, name)
            if not exists:
                self.fake.blobs[(container, name)] = blob
        if exists:
            self._error(409, "BlobAlreadyExists")
            return
        self._reply(201, headers={"ETag": blob.etag, "Last-Modified": _http_date(blob.modified),
                                  "x-ms-request-server-encrypted": "true"})

    def do_DELETE(self) -> None:
        if self._forbidden():
            return
        container, name, _ = self._target()
        self.fake.count("DELETE")
        self._wait()
        with self.fake.lock:
            removed = self.fake.blobs.pop((container, name), None)
        if removed is None:
            self._error(404, "BlobNotFound")
            return
        self._reply(202, headers={"x-ms-delete-type-permanent": "true"})

    def do_POST(self) -> None:
        if self._forbidden():
            return
        container, _, query = self._target()
        body = self._body()
        if query.get("comp", [""])[0] != "batch":
            self._error(400, "InvalidQueryParameterValue")
            return
        self.fake.count("DELETE_BATCH")
        self._wait()
        boundary = self.headers.get("Content-Type", "").partition("boundary=")[2].strip('"')
        responses = []
        for part in body.split(f"--{boundary}".encode())[1:]:
            text = part.decode("utf-8", "replace")
            if text.startswith("--"):
                break
            content_id = ""
            request_line = ""
            for line in text.split("\r\n"):
                if line.lower().startswith("content-id:"):
                    content_id = line.split(":", 1)[1].strip()
                elif line.startswith("DELETE "):
                    request_line = line
            target = urllib.parse.unquote(request_line.split(" ")[1]).lstrip("/") if request_line else ""
            account, _, rest = target.partition("/")
            if account != ACCOUNT:
                rest = target
            blob_container, _, blob_name = rest.partition("/")
            with self.fake.lock:
                removed = self.fake.blobs.pop((blob_container, blob_name.partition("?")[0]), None)
            status = "202 Accepted" if removed is not None else "404 The specified blob does not exist."
            responses.append((content_id, status))
        response_boundary = "batchresponse_" + uuid.uuid4().hex
        chunks = []
        for content_id, status in responses:
            chunks.append(f"--{response_boundary}\r\nContent-Type: application/http\r\n"
                          f"Content-ID: {content_id}\r\n\r\nHTTP/1.1 {status}\r\n"
                          f"x-ms-request-id: {uuid.uuid4()}\r\nx-ms-version: 2026-06-06\r\n"
                          "x-ms-delete-type-permanent: true\r\n\r\n")
        chunks.append(f"--{response_boundary}--")
        self._reply(202, "".join(chunks).encode(),
                    {"Content-Type": f"multipart/mixed; boundary={response_boundary}"})
