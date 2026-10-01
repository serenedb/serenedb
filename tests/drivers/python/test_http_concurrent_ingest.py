"""Concurrent OTLP/HTTP and Elasticsearch _bulk ingestion on one server.

Writers on separate keep-alive connections interleave every OTLP signal and
_bulk into two indexes, while readers query over pg-wire and _search. Then the
server must still be alive and every row must be readable.
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import pytest

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)

FIXTURES = (
    Path(__file__).resolve().parents[3] / "resources" / "otel" / "conformance"
)
AUTH = {"Authorization": "Basic cG9zdGdyZXM6"}
OTEL_TABLES = (
    "otel_logs", "otel_traces", "otel_metrics_gauge", "otel_metrics_sum",
    "otel_metrics_histogram", "otel_metrics_exponential_histogram",
    "otel_metrics_summary",
)
OTLP = (
    ("/v1/logs", "logs/upstream", "application/json"),
    ("/v1/traces", "traces/upstream", "application/json"),
    ("/v1/metrics", "metrics/upstream", "application/json"),
    ("/v1/logs", "logs/basic", "application/x-protobuf"),
    ("/v1/metrics", "metrics/exponential_histogram", "application/x-protobuf"),
    ("/v1/metrics", "metrics/mixed_batch", "application/x-protobuf"),
    ("/v1/metrics", "metrics/int_and_multi_datapoint", "application/json"),
)
INDEXES = ("cc_a", "cc_b")
WRITERS = 6
ROUNDS = 15
DOCS_PER_BULK = 50
READERS = 3


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_http(port: int, deadline: float) -> None:
    while True:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=2):
                return
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


def _connect(port: int, deadline: float) -> psycopg.Connection:
    while True:
        try:
            return psycopg.connect(
                host="127.0.0.1", port=port, user="postgres", dbname="postgres",
                autocommit=True,
            )
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


def _body(fixture: str, content_type: str) -> bytes:
    suffix = ".pb" if content_type == "application/x-protobuf" else ".json"
    return (FIXTURES / (fixture + suffix)).read_bytes()


def _call(conn: http.client.HTTPConnection, method: str, path: str,
          body: bytes | None = None,
          content_type: str = "application/json") -> tuple[int, bytes]:
    conn.request(method, path, body=body,
                 headers={"Content-Type": content_type, **AUTH})
    response = conn.getresponse()
    return response.status, response.read()


@pytest.fixture
def server(tmp_path: Path):
    pg_port, http_port = _free_port(), _free_port()
    listen = (f"postgres://127.0.0.1:{pg_port},"
              f"http://127.0.0.1:{http_port}?api=otel&api=es")
    proc = subprocess.Popen(
        [SERENED_BIN, str(tmp_path), f"--listen={listen}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    pg = _connect(pg_port, time.monotonic() + 60)
    # The HTTP listener binds independently of pg-wire, so pg answering does
    # not mean this one is up yet -- under a sanitizer the gap is seconds.
    _wait_http(http_port, time.monotonic() + 60)
    try:
        yield proc, pg, http_port
    finally:
        pg.close()
        proc.terminate()
        _, stderr = proc.communicate(timeout=60)
        assert proc.returncode in (0, -15), stderr.decode(errors="replace")


def _counts(pg: psycopg.Connection) -> dict[str, int]:
    counts = {}
    for table in OTEL_TABLES:
        pg.execute(f"VACUUM (REFRESH_TABLE) {table}")
        counts[table] = pg.execute(
            f"SELECT count(*) FROM {table}").fetchone()[0]
    return counts


def _bulk_payload(writer: int, round_: int) -> str:
    lines = []
    for k in range(DOCS_PER_BULK):
        doc_id = f"w{writer}-r{round_}-d{k}"
        lines.append(json.dumps({"index": {"_id": doc_id}}))
        lines.append(json.dumps({"n": k, "writer": writer,
                                 "title": f"doc {doc_id}"}))
    return "\n".join(lines) + "\n"


def test_concurrent_otel_and_es_ingest(server) -> None:
    proc, pg, http_port = server

    setup = http.client.HTTPConnection("127.0.0.1", http_port, timeout=120)
    mapping = json.dumps({"mappings": {"properties": {
        "n": {"type": "integer"}, "writer": {"type": "integer"},
        "title": {"type": "text"}}}}).encode()
    for index in INDEXES:
        assert _call(setup, "PUT", f"/{index}", mapping)[0] == 200

    before = _counts(pg)
    for path, fixture, content_type in OTLP:
        status, body = _call(setup, "POST", path,
                             _body(fixture, content_type), content_type)
        assert status == 200, (path, fixture, body)
    per_round = {t: n - before[t] for t, n in _counts(pg).items()}
    assert all(per_round[t] > 0 for t in OTEL_TABLES), per_round
    setup.close()
    start = _counts(pg)

    stop = threading.Event()
    failures: list[str] = []

    def writer(w: int) -> None:
        conn = http.client.HTTPConnection("127.0.0.1", http_port, timeout=120)
        try:
            for r in range(ROUNDS):
                for path, fixture, content_type in OTLP:
                    status, body = _call(conn, "POST", path,
                                         _body(fixture, content_type),
                                         content_type)
                    if status != 200:
                        failures.append(f"{path} {fixture}: {status} {body!r}")
                index = INDEXES[(w + r) % len(INDEXES)]
                status, body = _call(conn, "POST", f"/{index}/_bulk",
                                     _bulk_payload(w, r).encode(),
                                     "application/x-ndjson")
                reply = json.loads(body) if status == 200 else {}
                if status != 200 or reply.get("errors"):
                    failures.append(f"_bulk {index}: {status} {body[:500]!r}")
        finally:
            conn.close()

    def reader() -> None:
        pg_conn = _connect(pg.info.port, time.monotonic() + 60)
        http_conn = http.client.HTTPConnection("127.0.0.1", http_port,
                                               timeout=120)
        try:
            while not stop.is_set():
                for table in OTEL_TABLES:
                    pg_conn.execute(f"SELECT count(*) FROM {table}").fetchone()
                for index in INDEXES:
                    pg_conn.execute(
                        f"SELECT count(*), sum(n) FROM es.{index}").fetchone()
                    status, body = _call(
                        http_conn, "POST", f"/{index}/_search",
                        json.dumps({"query": {"match": {"title": "doc"}}})
                        .encode())
                    if status != 200:
                        failures.append(f"_search {index}: {status} {body!r}")
        finally:
            pg_conn.close()
            http_conn.close()

    with ThreadPoolExecutor(WRITERS + READERS) as pool:
        readers = [pool.submit(reader) for _ in range(READERS)]
        writers = [pool.submit(writer, w) for w in range(WRITERS)]
        for future in writers:
            future.result()
        stop.set()
        for future in readers:
            future.result()

    assert proc.poll() is None, "serened died during concurrent ingestion"
    assert not failures, failures[:10]

    counts = _counts(pg)
    for table in OTEL_TABLES:
        assert counts[table] - start[table] == per_round[table] * WRITERS * ROUNDS, table

    check = http.client.HTTPConnection("127.0.0.1", http_port, timeout=120)
    assert _call(check, "POST", "/_refresh")[0] == 200
    expected_docs = {index: 0 for index in INDEXES}
    for w in range(WRITERS):
        for r in range(ROUNDS):
            expected_docs[INDEXES[(w + r) % len(INDEXES)]] += DOCS_PER_BULK
    for index in INDEXES:
        status, body = _call(check, "GET", f"/{index}/_count")
        assert status == 200
        assert json.loads(body)["count"] == expected_docs[index], index
        docs, total = pg.execute(
            f"SELECT count(*), sum(n) FROM es.{index}").fetchone()
        assert docs == expected_docs[index]
        rounds = expected_docs[index] // DOCS_PER_BULK
        assert total == rounds * sum(range(DOCS_PER_BULK))
        status, body = _call(check, "POST", f"/{index}/_search", json.dumps(
            {"query": {"term": {"writer": 0}}, "size": 0}).encode())
        assert status == 200, body
        hits = json.loads(body)["hits"]["total"]["value"]
        assert hits == pg.execute(
            f"SELECT count(*) FROM es.{index} WHERE writer = 0").fetchone()[0]
    check.close()

    row = pg.execute(
        "SELECT count(*), count(DISTINCT service_name) FROM otel_logs"
    ).fetchone()
    assert row[0] == counts["otel_logs"] and row[1] >= 1
    assert pg.execute(
        "SELECT count(*) FROM otel_traces WHERE span_id IS NOT NULL"
    ).fetchone()[0] == counts["otel_traces"]
    assert proc.poll() is None
