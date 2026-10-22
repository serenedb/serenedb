"""An S3 endpoint in the test process, for the S3 client without a real store.

Path-style buckets that exist on first use, single and multipart uploads,
ranged reads, ListObjectsV2, server-side copies, single and batched deletes.
Requests are not authenticated. It counts requests by kind, accepted
connections and the most part uploads in flight at once; `latency` delays every request, `trickle`
sends the bodies of reads at that many bytes a second, and the part numbers in `failing_parts` are
refused with a 500. The next `throttle` requests are refused with 503 SlowDown, and the next
`drop[kind]` requests of a kind (COMPLETE_UPLOAD, COPY) take effect but get no response: the
connection closes, and their keys are listed in `dropped`. `stall(kind, key)` holds the next
request of a kind (PUT_PART, COPY, DELETE) whose key contains `key` before it takes effect, sets
`stalled`, and after `release` is set closes the connection without applying it: a test kills the
server while the request is held. While `session_tokens` is set, a request whose
X-Amz-Security-Token is not one of them is refused with 400 ExpiredToken, as a store refuses a
request signed with an expired session token.

A CRC32C checksum sent with an upload is checked as S3 checks it: the body of
a PUT or a part up to `checksum_limit` bytes against its own checksum, and a
multipart upload created with the full-object type against the checksum its
completion sends, which the checksums of its parts combine to. A mismatch is
refused with BadDigest; CHECKSUM counts the checksums that matched.

Run as a script, it serves until interrupted and answers on the `__admin`
bucket: GET /__admin/stats returns the counters as JSON, POST /__admin/reset
clears them, PUT /__admin/latency?seconds=S sets the latency, and after
PUT /__admin/log?enabled=1 each GET of an object is logged with its key, range
and start and end times, which GET /__admin/log returns and clears.
"""

from __future__ import annotations

import argparse
import base64
import collections
import email.utils
import hashlib
import http.server
import json
import threading
import time
import urllib.parse
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from xml.sax.saxutils import escape

NS = "http://s3.amazonaws.com/doc/2006-03-01/"
CRC32C_POLY = 0x82F63B78


def _crc32c_table() -> list[int]:
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ CRC32C_POLY if crc & 1 else crc >> 1
        table.append(crc)
    return table


_CRC32C_TABLE = _crc32c_table()


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    table = _CRC32C_TABLE
    for byte in data:
        crc = table[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def _gf2_times(matrix: list[int], vector: int) -> int:
    result = 0
    row = 0
    while vector:
        if vector & 1:
            result ^= matrix[row]
        vector >>= 1
        row += 1
    return result


def _gf2_square(matrix: list[int]) -> list[int]:
    return [_gf2_times(matrix, matrix[row]) for row in range(32)]


def crc32c_combine(first: int, second: int, second_length: int) -> int:
    """The CRC32C of two byte strings joined, from their checksums."""
    if second_length == 0:
        return first
    odd = [CRC32C_POLY] + [1 << row for row in range(31)]
    even = _gf2_square(odd)
    odd = _gf2_square(even)
    while True:
        even = _gf2_square(odd)
        if second_length & 1:
            first = _gf2_times(even, first)
        second_length >>= 1
        if not second_length:
            break
        odd = _gf2_square(even)
        if second_length & 1:
            first = _gf2_times(odd, first)
        second_length >>= 1
        if not second_length:
            break
    return first ^ second


def _encode_crc32c(value: int) -> str:
    return base64.b64encode(value.to_bytes(4, "big")).decode()


def _decode_crc32c(header: str) -> int | None:
    try:
        raw = base64.b64decode(header, validate=True)
    except ValueError:
        return None
    return int.from_bytes(raw, "big") if len(raw) == 4 else None


class _Upload:
    def __init__(self, bucket: str, key: str, full_object_crc32c: bool) -> None:
        self.bucket = bucket
        self.key = key
        self.full_object_crc32c = full_object_crc32c
        self.parts: dict[int, tuple[bytes, int | None]] = {}


class _Object:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.etag = '"' + hashlib.md5(data).hexdigest() + '"'
        self.modified = time.time()


class FakeS3:
    def __init__(self, latency: float = 0.0, port: int = 0,
                 checksum_limit: int = 1 << 20) -> None:
        self.latency = latency
        self.trickle = 0
        self.checksum_limit = checksum_limit
        self.lock = threading.Lock()
        self.objects: dict[tuple[str, str], _Object] = {}
        self.uploads: dict[str, _Upload] = {}
        self.requests: collections.Counter[str] = collections.Counter()
        self.parts_in_flight = 0
        self.max_parts_in_flight = 0
        self.failing_parts: set[int] = set()
        self.throttle = 0
        self.session_tokens: set[str] | None = None
        self.drop: collections.Counter[str] = collections.Counter()
        self.dropped: list[str] = []
        self.stalling: tuple[str, str] | None = None
        self.stalled = threading.Event()
        self.release = threading.Event()
        self.log: list[dict] | None = None
        store = self

        class Handler(_Handler):
            fake = store

            def setup(self) -> None:
                super().setup()
                store.count("CONNECTION")

        self.server = _Server(("127.0.0.1", port), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)

    @property
    def endpoint(self) -> str:
        return f"127.0.0.1:{self.server.server_address[1]}"

    def __enter__(self) -> FakeS3:
        self.thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.server.shutdown()
        self.server.server_close()

    def keys(self, bucket: str, prefix: str = "") -> list[str]:
        with self.lock:
            return sorted(key for b, key in self.objects
                          if b == bucket and key.startswith(prefix))

    def replace(self, bucket: str, key: str, data: bytes) -> None:
        with self.lock:
            self.objects[(bucket, key)] = _Object(data)

    def stall(self, kind: str, key: str) -> None:
        self.stalled.clear()
        self.release.clear()
        with self.lock:
            self.stalling = (kind, key)

    def count(self, kind: str) -> None:
        with self.lock:
            self.requests[kind] += 1

    def stats(self) -> dict:
        with self.lock:
            return {"requests": dict(self.requests),
                    "max_parts_in_flight": self.max_parts_in_flight,
                    "objects": len(self.objects),
                    "bytes": sum(len(obj.data) for obj in self.objects.values())}

    def reset(self) -> None:
        with self.lock:
            self.requests.clear()
            self.max_parts_in_flight = self.parts_in_flight


class _Server(http.server.ThreadingHTTPServer):
    request_queue_size = 1024


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True
    fake: FakeS3

    def log_message(self, *args) -> None:
        pass

    def _target(self) -> tuple[str, str, dict[str, list[str]]]:
        url = urllib.parse.urlsplit(self.path)
        path = urllib.parse.unquote(url.path).lstrip("/")
        bucket, _, key = path.partition("/")
        query = urllib.parse.parse_qs(url.query, keep_blank_values=True)
        return bucket, key, query

    def _body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length) if length else b""

    def _reply(self, status: int, body: bytes = b"",
               headers: dict[str, str] | None = None,
               content_length: int | None = None, trickle: bool = False) -> None:
        self.send_response(status)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length",
                         str(len(body) if content_length is None else content_length))
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

    def _xml(self, status: int, body: str) -> None:
        self._reply(status, ('<?xml version="1.0" encoding="UTF-8"?>' + body).encode(),
                    {"Content-Type": "application/xml"})

    def _missing(self, key: str) -> None:
        if self.command == "HEAD":
            self._reply(404)
        else:
            self._xml(404, f"<Error><Code>NoSuchKey</Code><Key>{escape(key)}</Key></Error>")

    def _wait(self) -> None:
        if self.fake.latency:
            time.sleep(self.fake.latency)

    def _checksum(self, body: bytes) -> tuple[bool, int | None]:
        header = self.headers.get("x-amz-checksum-crc32c")
        if header is None:
            return True, None
        value = _decode_crc32c(header)
        if value is None:
            return False, None
        if len(body) <= self.fake.checksum_limit:
            if crc32c(body) != value:
                return False, value
            self.fake.count("CHECKSUM")
        return True, value

    def _bad_digest(self) -> None:
        self._xml(400, "<Error><Code>BadDigest</Code></Error>")

    def _expired(self) -> bool:
        tokens = self.fake.session_tokens
        if tokens is None or self.headers.get("x-amz-security-token") in tokens:
            return False
        self.fake.count("EXPIRED")
        self._body()
        self._xml(400, "<Error><Code>ExpiredToken</Code><Message>The provided token has expired."
                       "</Message></Error>")
        return True

    def _throttled(self) -> bool:
        with self.fake.lock:
            if self.fake.throttle == 0:
                return False
            self.fake.throttle -= 1
            self.fake.requests["THROTTLED"] += 1
        self._body()
        self._xml(503, "<Error><Code>SlowDown</Code><Message>Please reduce your request rate."
                       "</Message></Error>")
        return True

    def _stalled(self, kind: str, key: str) -> bool:
        with self.fake.lock:
            stalling = self.fake.stalling
            if stalling is None or stalling[0] != kind or stalling[1] not in key:
                return False
            self.fake.stalling = None
        self.fake.stalled.set()
        self.fake.release.wait(120)
        self.close_connection = True
        return True

    def _dropped(self, kind: str, key: str) -> bool:
        with self.fake.lock:
            if self.fake.drop[kind] == 0:
                return False
            self.fake.drop[kind] -= 1
            self.fake.dropped.append(key)
        self.close_connection = True
        return True

    def _find(self, bucket: str, key: str) -> _Object | None:
        with self.fake.lock:
            return self.fake.objects.get((bucket, key))

    def _admin(self, bucket: str, key: str, query: dict[str, list[str]]) -> bool:
        if bucket != "__admin":
            return False
        self._body()
        if self.command == "GET" and key == "stats":
            self._reply(200, json.dumps(self.fake.stats()).encode(),
                        {"Content-Type": "application/json"})
        elif self.command == "POST" and key == "reset":
            self.fake.reset()
            self._reply(204)
        elif self.command == "PUT" and key == "latency":
            self.fake.latency = float(query.get("seconds", ["0"])[0])
            self._reply(204)
        elif self.command == "PUT" and key == "log":
            with self.fake.lock:
                self.fake.log = [] if query.get("enabled", ["1"])[0] == "1" else None
            self._reply(204)
        elif self.command == "GET" and key == "log":
            with self.fake.lock:
                entries = self.fake.log or []
                if self.fake.log is not None:
                    self.fake.log = []
            self._reply(200, json.dumps(entries).encode(),
                        {"Content-Type": "application/json"})
        else:
            self._reply(404)
        return True

    def do_HEAD(self) -> None:
        bucket, key, _ = self._target()
        if self._expired() or self._throttled():
            return
        self.fake.count("HEAD")
        self._wait()
        obj = self._find(bucket, key)
        if obj is None:
            self._missing(key)
            return
        self._reply(200, headers=self._object_headers(obj),
                    content_length=len(obj.data))

    def _object_headers(self, obj: _Object) -> dict[str, str]:
        return {"ETag": obj.etag, "Accept-Ranges": "bytes",
                "Last-Modified": email.utils.formatdate(obj.modified, usegmt=True)}

    def do_GET(self) -> None:
        bucket, key, query = self._target()
        if self._admin(bucket, key, query) or self._expired() or self._throttled():
            return
        if not key and "list-type" in query:
            self.fake.count("LIST")
            self._wait()
            self._list(bucket, query)
            return
        self.fake.count("GET")
        began = time.monotonic()
        self._wait()
        obj = self._find(bucket, key)
        if obj is None:
            self._missing(key)
            return
        if self.fake.log is not None:
            with self.fake.lock:
                if self.fake.log is not None:
                    self.fake.log.append({"key": key, "range": self.headers.get("Range"),
                                          "began": began, "ended": time.monotonic()})
        if_match = self.headers.get("If-Match")
        if if_match and if_match != obj.etag:
            self._xml(412, "<Error><Code>PreconditionFailed</Code></Error>")
            return
        headers = self._object_headers(obj)
        ranged = self.headers.get("Range")
        if not ranged:
            self._reply(200, obj.data, headers, trickle=True)
            return
        first, _, last = ranged.removeprefix("bytes=").partition("-")
        start = int(first)
        end = min(int(last) if last else len(obj.data) - 1, len(obj.data) - 1)
        if start >= len(obj.data):
            self._xml(416, "<Error><Code>InvalidRange</Code></Error>")
            return
        headers["Content-Range"] = f"bytes {start}-{end}/{len(obj.data)}"
        self._reply(206, obj.data[start:end + 1], headers, trickle=True)

    def _list(self, bucket: str, query: dict[str, list[str]]) -> None:
        prefix = query.get("prefix", [""])[0]
        delimiter = query.get("delimiter", [""])[0]
        start_after = query.get("continuation-token", [""])[0]
        max_keys = int(query.get("max-keys", ["1000"])[0])
        contents, prefixes = [], []
        for key in self.fake.keys(bucket, prefix):
            if start_after and key <= start_after:
                continue
            if delimiter:
                cut = key.find(delimiter, len(prefix))
                if cut != -1:
                    common = key[:cut + len(delimiter)]
                    if not prefixes or prefixes[-1] != common:
                        prefixes.append(common)
                    continue
            contents.append(key)
        entries = sorted([(key, False) for key in contents] + [(p, True) for p in prefixes])
        truncated = len(entries) > max_keys
        entries = entries[:max_keys]
        body = [f'<ListBucketResult xmlns="{NS}"><Name>{escape(bucket)}</Name>'
                f"<Prefix>{escape(prefix)}</Prefix><KeyCount>{len(entries)}</KeyCount>"
                f"<MaxKeys>{max_keys}</MaxKeys><IsTruncated>{str(truncated).lower()}</IsTruncated>"]
        if truncated:
            body.append(f"<NextContinuationToken>{escape(entries[-1][0])}</NextContinuationToken>")
        for name, is_prefix in entries:
            if is_prefix:
                body.append(f"<CommonPrefixes><Prefix>{escape(name)}</Prefix></CommonPrefixes>")
                continue
            obj = self._find(bucket, name)
            if obj is None:
                continue
            modified = datetime.fromtimestamp(obj.modified, timezone.utc)
            body.append(f"<Contents><Key>{escape(name)}</Key>"
                        f"<LastModified>{modified.strftime('%Y-%m-%dT%H:%M:%S.000Z')}</LastModified>"
                        f"<ETag>{escape(obj.etag)}</ETag><Size>{len(obj.data)}</Size>"
                        "<StorageClass>STANDARD</StorageClass></Contents>")
        body.append("</ListBucketResult>")
        self._xml(200, "".join(body))

    def do_PUT(self) -> None:
        bucket, key, query = self._target()
        if self._admin(bucket, key, query) or self._expired() or self._throttled():
            return
        if "partNumber" in query and "uploadId" in query:
            if self._stalled("PUT_PART", key):
                return
            self._put_part(int(query["partNumber"][0]), query["uploadId"][0])
            return
        source = self.headers.get("x-amz-copy-source")
        body = self._body()
        if source:
            if self._stalled("COPY", key):
                return
            self.fake.count("COPY")
            self._wait()
            src_bucket, _, src_key = urllib.parse.unquote(source).lstrip("/").partition("/")
            obj = self._find(src_bucket, src_key)
            if obj is None:
                self._missing(src_key)
                return
            copy = _Object(obj.data)
            with self.fake.lock:
                self.fake.objects[(bucket, key)] = copy
            if self._dropped("COPY", key):
                return
            self._xml(200, f'<CopyObjectResult xmlns="{NS}"><ETag>{escape(copy.etag)}</ETag></CopyObjectResult>')
            return
        self.fake.count("PUT")
        self._wait()
        matches, checksum = self._checksum(body)
        if not matches:
            self._bad_digest()
            return
        obj = _Object(body)
        with self.fake.lock:
            self.fake.objects[(bucket, key)] = obj
        headers = {"ETag": obj.etag}
        if checksum is not None:
            headers["x-amz-checksum-crc32c"] = _encode_crc32c(checksum)
        self._reply(200, headers=headers)

    def _put_part(self, number: int, upload_id: str) -> None:
        with self.fake.lock:
            self.fake.requests["PUT_PART"] += 1
            self.fake.parts_in_flight += 1
            self.fake.max_parts_in_flight = max(self.fake.max_parts_in_flight,
                                                self.fake.parts_in_flight)
        try:
            body = self._body()
            self._wait()
            if number in self.fake.failing_parts:
                self._xml(500, "<Error><Code>InternalError</Code></Error>")
                return
            matches, checksum = self._checksum(body)
            if not matches:
                self._bad_digest()
                return
            with self.fake.lock:
                upload = self.fake.uploads.get(upload_id)
                if upload is not None:
                    upload.parts[number] = (body, checksum)
            if upload is None:
                self._xml(404, "<Error><Code>NoSuchUpload</Code></Error>")
                return
            if upload.full_object_crc32c and checksum is None:
                self._xml(400, "<Error><Code>InvalidRequest</Code></Error>")
                return
            headers = {"ETag": '"' + hashlib.md5(body).hexdigest() + '"'}
            if checksum is not None:
                headers["x-amz-checksum-crc32c"] = _encode_crc32c(checksum)
            self._reply(200, headers=headers)
        finally:
            with self.fake.lock:
                self.fake.parts_in_flight -= 1

    def do_POST(self) -> None:
        bucket, key, query = self._target()
        if self._admin(bucket, key, query) or self._expired() or self._throttled():
            return
        body = self._body()
        if "uploads" in query:
            self.fake.count("CREATE_UPLOAD")
            self._wait()
            upload_id = uuid.uuid4().hex
            full_object = (self.headers.get("x-amz-checksum-algorithm", "").upper() == "CRC32C" and
                           self.headers.get("x-amz-checksum-type", "").upper() == "FULL_OBJECT")
            with self.fake.lock:
                self.fake.uploads[upload_id] = _Upload(bucket, key, full_object)
            checksum = ("<ChecksumAlgorithm>CRC32C</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType>"
                        if full_object else "")
            self._xml(200, f'<InitiateMultipartUploadResult xmlns="{NS}"><Bucket>{escape(bucket)}</Bucket>'
                           f"<Key>{escape(key)}</Key><UploadId>{upload_id}</UploadId>{checksum}"
                           "</InitiateMultipartUploadResult>")
            return
        if "uploadId" in query:
            self.fake.count("COMPLETE_UPLOAD")
            self._wait()
            self._complete(bucket, key, query["uploadId"][0], body)
            return
        if "delete" in query:
            self.fake.count("DELETE_BATCH")
            self._wait()
            root = ET.fromstring(body)
            names = [element.text or "" for element in root.iter() if element.tag.endswith("Key")]
            with self.fake.lock:
                for name in names:
                    self.fake.objects.pop((bucket, name), None)
            deleted = "".join(f"<Deleted><Key>{escape(name)}</Key></Deleted>" for name in names)
            self._xml(200, f'<DeleteResult xmlns="{NS}">{deleted}</DeleteResult>')
            return
        self._xml(400, "<Error><Code>InvalidRequest</Code></Error>")

    def _complete(self, bucket: str, key: str, upload_id: str, body: bytes) -> None:
        root = ET.fromstring(body)
        numbers = [int(element.text or "0") for element in root.iter() if element.tag.endswith("PartNumber")]
        with self.fake.lock:
            upload = self.fake.uploads.pop(upload_id, None)
        if upload is None or any(number not in upload.parts for number in numbers):
            self._xml(400, "<Error><Code>InvalidPart</Code></Error>")
            return
        parts = [upload.parts[number] for number in numbers]
        data = b"".join(part for part, _ in parts)
        checksum = ""
        if upload.full_object_crc32c:
            expected = _decode_crc32c(self.headers.get("x-amz-checksum-crc32c", ""))
            combined = 0
            for part, part_checksum in parts:
                combined = crc32c_combine(combined, part_checksum, len(part))
            if expected is None or combined != expected or (
                    len(data) <= self.fake.checksum_limit and crc32c(data) != expected):
                self._bad_digest()
                return
            self.fake.count("CHECKSUM")
            checksum = (f"<ChecksumCRC32C>{_encode_crc32c(expected)}</ChecksumCRC32C>"
                        "<ChecksumType>FULL_OBJECT</ChecksumType>")
        obj = _Object(data)
        with self.fake.lock:
            self.fake.objects[(bucket, key)] = obj
        if self._dropped("COMPLETE_UPLOAD", key):
            return
        self._xml(200, f'<CompleteMultipartUploadResult xmlns="{NS}"><Bucket>{escape(bucket)}</Bucket>'
                       f"<Key>{escape(key)}</Key><ETag>{escape(obj.etag)}</ETag>{checksum}"
                       "</CompleteMultipartUploadResult>")

    def do_DELETE(self) -> None:
        bucket, key, query = self._target()
        if self._expired() or self._throttled():
            return
        if "uploadId" in query:
            self.fake.count("ABORT_UPLOAD")
            self._wait()
            with self.fake.lock:
                self.fake.uploads.pop(query["uploadId"][0], None)
            self._reply(204)
            return
        if self._stalled("DELETE", key):
            return
        self.fake.count("DELETE")
        self._wait()
        with self.fake.lock:
            self.fake.objects.pop((bucket, key), None)
        self._reply(204)


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve a fake S3 endpoint until interrupted")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--latency", type=float, default=0.0,
                        help="seconds added to every request")
    args = parser.parse_args()
    with FakeS3(args.latency, args.port) as fake:
        print(fake.endpoint, flush=True)
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
