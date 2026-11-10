"""An Azure Blob Storage endpoint in the test process, for the Azure client without a real store.

Path-style URLs under one storage account, containers that exist on first use, block blobs
staged and committed by block lists with the metadata the commit sets, Data Lake files created,
appended in order and flushed, Data Lake directories created, listed, renamed into and deleted with
what is below them, ranged reads with conditions, blob listings, same-account copies, single and
batched deletes. Requests are not authenticated, except that while
`sas_signatures` is set, a request whose SAS signature is not one of them is refused with 403
AuthenticationFailed, as an expired SAS is. It counts requests by kind. `trickle` sends the bodies
of reads at that many bytes a second. The next `drop[kind]` requests of a kind (PUT_BLOCK_LIST) take
effect but get no response: the connection closes, and their blob names are listed in `dropped`.

Point a secret at it with `connection_string`, or with `sas_connection_string(signature)`.
"""

from __future__ import annotations

import base64
import collections
import hashlib
import json
import time
import urllib.parse
import uuid
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

from fake_store import FakeStore, Handler, byte_range, http_date, list_page, split_path

ACCOUNT = "devstoreaccount1"
KEY = base64.b64encode(b"fake-azure-account-key-for-tests").decode()


class _Blob:
    def __init__(self, data: bytes, blocks: dict[str, bytes] | None = None,
                 metadata: dict[str, str] | None = None) -> None:
        self.data = data
        self.blocks = blocks or {}
        self.metadata = metadata or {}
        self.etag = '"0x' + hashlib.md5(data + uuid.uuid4().bytes).hexdigest()[:16].upper() + '"'
        self.modified = time.time()


class FakeAzure(FakeStore):
    def __init__(self) -> None:
        super().__init__(_Handler)
        self.sas_signatures: set[str] | None = None
        self.blobs: dict[tuple[str, str], _Blob] = {}
        self.staged: dict[tuple[str, str], dict[str, bytes]] = {}
        self.appended: dict[tuple[str, str], bytearray] = {}
        self.directories: set[tuple[str, str]] = set()
        self.trickle = 0
        self.drop: collections.Counter[str] = collections.Counter()
        self.dropped: list[str] = []

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}/{ACCOUNT}"

    @property
    def connection_string(self) -> str:
        return (f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT};AccountKey={KEY};"
                f"BlobEndpoint={self.endpoint};DfsEndpoint={self.endpoint};")

    def sas_connection_string(self, signature: str) -> str:
        return (f"BlobEndpoint={self.endpoint};SharedAccessSignature=sv=2022-11-02&ss=b&srt=sco"
                f"&sp=rwdlacx&se=2099-12-31T00:00:00Z&sig={signature}")

    def names(self, container: str, prefix: str = "") -> list[str]:
        with self.lock:
            return sorted(name for c, name in self.blobs
                          if c == container and name.startswith(prefix))

    def replace(self, container: str, name: str, data: bytes) -> None:
        with self.lock:
            self.blobs[(container, name)] = _Blob(data)

    def add_parents(self, container: str, name: str) -> None:
        while "/" in name:
            name = name.rpartition("/")[0]
            self.directories.add((container, name))


class _Handler(Handler):
    account = ACCOUNT
    fake: FakeAzure

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
               content_length: int | None = None, trickle: bool = False) -> None:
        self.send_response(status)
        self.send_header("x-ms-request-id", str(uuid.uuid4()))
        self.send_header("x-ms-version", self.headers.get("x-ms-version", "2026-06-06"))
        self.send_header("Date", http_date(time.time()))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body) if content_length is None else content_length))
        self.end_headers()
        if not body or self.command == "HEAD":
            return
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

    def _not_found(self, container: str) -> str:
        with self.fake.lock:
            data_lake = any(c == container for c, _ in self.fake.directories)
        return "PathNotFound" if data_lake else "BlobNotFound"

    def _properties(self, blob: _Blob) -> dict[str, str]:
        return {"ETag": blob.etag, "Last-Modified": http_date(blob.modified),
                "x-ms-creation-time": http_date(blob.modified), "x-ms-blob-type": "BlockBlob",
                "x-ms-server-encrypted": "true", "Accept-Ranges": "bytes",
                "Content-Type": "application/octet-stream",
                **{f"x-ms-meta-{name}": value for name, value in blob.metadata.items()}}

    def _conditions_fail(self, blob: _Blob) -> bool:
        if_match = self.headers.get("If-Match")
        return bool(if_match) and if_match != "*" and if_match != blob.etag

    def _occupied(self, container: str, name: str) -> bool:
        return self.headers.get("If-None-Match") == "*" and (container, name) in self.fake.blobs

    def _dropped(self, kind: str, name: str) -> bool:
        with self.fake.lock:
            if self.fake.drop[kind] == 0:
                return False
            self.fake.drop[kind] -= 1
            self.fake.dropped.append(name)
        self.close_connection = True
        return True

    def do_HEAD(self) -> None:
        if self._forbidden():
            return
        container, name, _ = self._target()
        self.fake.count("HEAD")
        blob = self._find(container, name)
        if blob is None:
            with self.fake.lock:
                directory = (container, name.rstrip("/")) in self.fake.directories
            if not directory:
                self._error(404, self._not_found(container))
                return
            self._reply(200, headers={**self._properties(_Blob(b"")), "x-ms-resource-type": "directory",
                                      "x-ms-meta-hdi_isfolder": "true"}, content_length=0)
            return
        self._reply(200, headers=self._properties(blob), content_length=len(blob.data))

    def do_GET(self) -> None:
        if self._forbidden():
            return
        container, name, query = self._target()
        if query.get("comp", [""])[0] == "list":
            self.fake.count("LIST")
            self._list(container, query)
            return
        if query.get("resource", [""])[0] == "filesystem":
            self.fake.count("LIST_PATHS")
            self._list_paths(container, query)
            return
        self.fake.count("GET")
        blob = self._find(container, name)
        if blob is None:
            self._error(404, self._not_found(container))
            return
        if self._conditions_fail(blob):
            self._error(412, "ConditionNotMet")
            return
        headers = self._properties(blob)
        ranged = self.headers.get("x-ms-range")
        if not ranged:
            self._reply(200, blob.data, headers, trickle=True)
            return
        part = byte_range(ranged, blob.data)
        if part is None:
            self._error(416, "InvalidRange")
            return
        headers["Content-Range"] = part[1]
        self._reply(206, part[0], headers, trickle=True)

    def _list(self, container: str, query: dict[str, list[str]]) -> None:
        prefix = query.get("prefix", [""])[0]
        delimiter = query.get("delimiter", [""])[0]
        marker = query.get("marker", [""])[0]
        max_results = int(query.get("maxresults", ["5000"])[0])
        page, truncated = list_page(self.fake.names(container, prefix), prefix, delimiter, marker,
                                    max_results)
        body =[f'<?xml version="1.0" encoding="utf-8"?><EnumerationResults '
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
            modified = http_date(blob.modified)
            body.append(f"<Blob><Name>{escape(name)}</Name><Properties>"
                        f"<Creation-Time>{modified}</Creation-Time><Last-Modified>{modified}</Last-Modified>"
                        f"<Etag>{escape(blob.etag)}</Etag><Content-Length>{len(blob.data)}</Content-Length>"
                        "<Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType>"
                        "<ServerEncrypted>true</ServerEncrypted></Properties></Blob>")
        body.append("</Blobs>")
        body.append(f"<NextMarker>{escape(page[-1][0]) if truncated else ''}</NextMarker></EnumerationResults>")
        self._reply(200, "".join(body).encode(), {"Content-Type": "application/xml"})

    def _list_paths(self, container: str, query: dict[str, list[str]]) -> None:
        directory = query.get("directory", [""])[0].strip("/")
        recursive = query.get("recursive", ["false"])[0] == "true"
        prefix = f"{directory}/" if directory else ""
        with self.fake.lock:
            known = not directory or (container, directory) in self.fake.directories
            files = {name: blob for (c, name), blob in self.fake.blobs.items()
                     if c == container and name.startswith(prefix)}
            directories = {name for c, name in self.fake.directories
                           if c == container and name.startswith(prefix)}
        if not known:
            self._error(404, "PathNotFound")
            return
        paths = []
        for name in sorted(files.keys() | directories):
            if not recursive and "/" in name[len(prefix):]:
                continue
            blob = files.get(name)
            entry = {"name": name, "owner": "$superuser", "group": "$superuser", "permissions": "rw-r-----",
                     "contentLength": str(len(blob.data) if blob else 0),
                     "lastModified": http_date(blob.modified if blob else time.time()),
                     "etag": (blob.etag if blob else '"0x0"').strip('"')}
            if blob is None:
                entry["isDirectory"] = "true"
            paths.append(entry)
        self._reply(200, json.dumps({"paths": paths}).encode(), {"Content-Type": "application/json"})

    def _rename(self, container: str, name: str, source: str) -> None:
        self.fake.count("RENAME")
        src_container, src_name = split_path(urllib.parse.urlsplit(source).path, ACCOUNT)
        with self.fake.lock:
            blob = self.fake.blobs.pop((src_container, src_name), None)
            if blob is not None:
                self.fake.blobs[(container, name)] = blob
                self.fake.add_parents(container, name)
        if blob is None:
            self._error(404, "SourcePathNotFound")
            return
        self._reply(201, headers={"ETag": blob.etag, "Last-Modified": http_date(blob.modified)})

    def do_PUT(self) -> None:
        if self._forbidden():
            return
        container, name, query = self._target()
        body = self._body()
        source = self.headers.get("x-ms-rename-source")
        if source:
            self._rename(container, name, source)
            return
        if query.get("resource", [""])[0] == "directory":
            self.fake.count("CREATE_DIRECTORY")
            directory = name.rstrip("/")
            with self.fake.lock:
                exists = (self.headers.get("If-None-Match") == "*"
                          and (container, directory) in self.fake.directories)
                if not exists:
                    self.fake.directories.add((container, directory))
                    self.fake.add_parents(container, directory)
            if exists:
                self._error(409, "PathAlreadyExists")
                return
            self._reply(201, headers={"ETag": '"0x0"', "Last-Modified": http_date(time.time()),
                                      "x-ms-request-server-encrypted": "true"})
            return
        if query.get("resource", [""])[0] == "file":
            self.fake.count("CREATE_FILE")
            with self.fake.lock:
                exists = self._occupied(container, name)
                if not exists:
                    blob = _Blob(b"")
                    self.fake.blobs[(container, name)] = blob
                    self.fake.appended[(container, name)] = bytearray()
                    self.fake.add_parents(container, name)
            if exists:
                self._error(409, "PathAlreadyExists")
                return
            self._reply(201, headers={"ETag": blob.etag, "Last-Modified": http_date(blob.modified),
                                      "x-ms-request-server-encrypted": "true"})
            return
        comp = query.get("comp", [""])[0]
        if comp == "block":
            self.fake.count("PUT_BLOCK")
            with self.fake.lock:
                self.fake.staged.setdefault((container, name), {})[query["blockid"][0]] = body
            self._reply(201, headers={"x-ms-request-server-encrypted": "true"})
            return
        if comp == "blocklist":
            self.fake.count("PUT_BLOCK_LIST")
            ids = [element.text or "" for element in ET.fromstring(body)]
            metadata = {header[len("x-ms-meta-"):]: value for header, value in self.headers.items()
                        if header.lower().startswith("x-ms-meta-")}
            with self.fake.lock:
                exists = self._occupied(container, name)
                blocks = dict(getattr(self.fake.blobs.get((container, name)), "blocks", {}))
                blocks.update(self.fake.staged.get((container, name), {}))
                if exists or any(block not in blocks for block in ids):
                    blob = None
                else:
                    blob = _Blob(b"".join(blocks[block] for block in ids),
                                 {block: blocks[block] for block in ids}, metadata)
                    self.fake.blobs[(container, name)] = blob
                    self.fake.staged.pop((container, name), None)
            if exists:
                self._error(409, "BlobAlreadyExists")
                return
            if blob is None:
                self._error(400, "InvalidBlockList")
                return
            if self._dropped("PUT_BLOCK_LIST", name):
                return
            self._reply(201, headers={"ETag": blob.etag, "Last-Modified": http_date(blob.modified),
                                      "x-ms-request-server-encrypted": "true"})
            return
        source = self.headers.get("x-ms-copy-source")
        if source:
            self.fake.count("COPY")
            src_container, src_name = split_path(urllib.parse.urlsplit(source).path, ACCOUNT)
            original = self._find(src_container, src_name)
            if original is None:
                self._error(404, "CannotVerifyCopySource")
                return
            blob = _Blob(original.data, original.blocks)
            with self.fake.lock:
                self.fake.blobs[(container, name)] = blob
            self._reply(202, headers={"ETag": blob.etag, "Last-Modified": http_date(blob.modified),
                                      "x-ms-copy-id": str(uuid.uuid4()), "x-ms-copy-status": "success"})
            return
        self._error(400, "InvalidQueryParameterValue")

    def do_PATCH(self) -> None:
        if self._forbidden():
            return
        container, name, query = self._target()
        body = self._body()
        action = query.get("action", [""])[0]
        position = int(query.get("position", ["0"])[0])
        if action not in ("append", "flush"):
            self._error(400, "InvalidQueryParameterValue")
            return
        self.fake.count(action.upper())
        with self.fake.lock:
            appended = self.fake.appended.get((container, name))
            if appended is None:
                error = (404, "PathNotFound")
            elif position != len(appended):
                error = (400, "InvalidAppendPosition" if action == "append" else "InvalidFlushPosition")
            else:
                error = None
                if action == "append":
                    appended.extend(body)
                else:
                    blob = _Blob(bytes(appended))
                    self.fake.blobs[(container, name)] = blob
        if error is not None:
            self._error(*error)
            return
        if action == "append":
            self._reply(202, headers={"x-ms-request-server-encrypted": "true"})
            return
        self._reply(200, headers={"ETag": blob.etag, "Last-Modified": http_date(blob.modified),
                                  "x-ms-request-server-encrypted": "true"})

    def do_DELETE(self) -> None:
        if self._forbidden():
            return
        container, name, query = self._target()
        if "recursive" in query:
            self.fake.count("DELETE_DIRECTORY")
            directory = name.rstrip("/")
            with self.fake.lock:
                known = (container, directory) in self.fake.directories
                for key in [key for key in self.fake.blobs
                            if key[0] == container and key[1].startswith(f"{directory}/")]:
                    del self.fake.blobs[key]
                self.fake.directories = {
                    key for key in self.fake.directories
                    if key[0] != container or (key[1] != directory and not key[1].startswith(f"{directory}/"))}
            if not known:
                self._error(404, "PathNotFound")
                return
            self._reply(200)
            return
        self.fake.count("DELETE")
        with self.fake.lock:
            removed = self.fake.blobs.pop((container, name), None)
        if removed is None:
            self._error(404, self._not_found(container))
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
            target = urllib.parse.urlsplit(request_line.split(" ")[1]).path if request_line else ""
            blob_container, blob_name = split_path(target, ACCOUNT)
            with self.fake.lock:
                removed = self.fake.blobs.pop((blob_container, blob_name), None)
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
