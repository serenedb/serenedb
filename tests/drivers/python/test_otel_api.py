"""OTLP/HTTP API tests (POST /v1/logs, /v1/traces, /v1/metrics).

Exercises the otel_parse_*() table functions through the thin HTTP handlers, using
the same conformance payloads the sqllogic contract test uses. Skipped
wholesale when no HTTP endpoint is configured (SDB_DRV_HTTP_PORT).
"""

from __future__ import annotations

import base64
import gzip
import http.client
import json
import os
import pathlib
import socket

import psycopg
import pytest
from spec_loader import conn_kwargs

HOST = os.environ.get("SDB_DRV_HOST", "localhost")
PORT = int(os.environ.get("SDB_DRV_HTTP_PORT", "9200"))
USER = os.environ.get("SDB_DRV_USER", "postgres")
PASSWORD = os.environ.get("SDB_DRV_PASSWORD", "")

TOKEN = os.environ.get("SDB_DRV_HTTP_TOKEN", "")
AUTH = (
    f"Bearer {TOKEN}"
    if TOKEN
    else "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
)

FIXTURES = (
    pathlib.Path(__file__).resolve().parents[3] / "resources" / "otel" / "conformance"
)

SIGNALS = [
    ("logs", "logs/basic.json"),
    ("logs", "logs/anyvalue_types.json"),
    ("logs", "logs/empty_ids_and_zero_timestamp.json"),
    ("logs", "logs/multi_resource_scope.json"),
    ("traces", "traces/span_with_events_and_links.json"),
    ("traces", "traces/kinds_and_statuses.json"),
    ("metrics", "metrics/mixed_batch.json"),
    ("metrics", "metrics/exponential_histogram.json"),
    ("metrics", "metrics/int_and_multi_datapoint.json"),
    ("logs", "logs/upstream.json"),
    ("traces", "traces/upstream.json"),
    ("traces", "traces/enum_names_and_unknown_fields.json"),
    ("metrics", "metrics/upstream.json"),
]


def _reachable() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=2):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _reachable(), reason=f"no HTTP endpoint at {HOST}:{PORT}"
)


@pytest.fixture()
def conn():
    c = http.client.HTTPConnection(HOST, PORT, timeout=30)
    yield c
    c.close()


def _post(conn, path, body, content_type="application/json", headers=None):
    all_headers = {"Authorization": AUTH}
    if content_type:
        all_headers["Content-Type"] = content_type
    all_headers.update(headers or {})
    conn.request("POST", path, body=body, headers=all_headers)
    response = conn.getresponse()
    payload = response.read().decode()
    return response.status, payload


@pytest.mark.parametrize("signal,fixture", SIGNALS)
def test_json_export_accepts_conformance_fixture(conn, signal, fixture):
    body = (FIXTURES / fixture).read_bytes()
    status, payload = _post(conn, f"/v1/{signal}", body)
    assert status == 200, payload
    assert json.loads(payload) == {}


@pytest.mark.parametrize("signal,fixture", SIGNALS)
def test_protobuf_export_accepts_conformance_fixture(conn, signal, fixture):
    wire = FIXTURES / fixture.replace(".json", ".pb")
    if not wire.exists():
        pytest.skip(f"{wire.name} not generated; run scripts/otel/fixtures.py")
    status, payload = _post(
        conn, f"/v1/{signal}", wire.read_bytes(), content_type="application/x-protobuf"
    )
    assert status == 200, payload
    # A full-success Export<Signal>ServiceResponse is an empty message.
    assert payload == ""


def test_protobuf_errors_answer_in_protobuf(conn):
    conn.request(
        "POST",
        "/v1/logs",
        body=b"\xff\xff\xff",
        headers={"Authorization": AUTH, "Content-Type": "application/x-protobuf"},
    )
    response = conn.getresponse()
    body = response.read()
    assert response.status == 400
    assert response.getheader("Content-Type") == "application/x-protobuf"
    # google.rpc.Status: field 1 (code) as a varint, tag byte 0x08.
    assert body.startswith(b"\x08")


@pytest.mark.parametrize("signal", ["logs", "traces", "metrics"])
def test_empty_request_is_accepted(conn, signal):
    status, payload = _post(conn, f"/v1/{signal}", b"{}")
    assert status == 200, payload
    assert json.loads(payload) == {}


def test_unknown_content_type_is_rejected(conn):
    status, payload = _post(conn, "/v1/logs", b"{}", content_type="text/csv")
    assert status == 400, payload
    assert "Content-Type" in json.loads(payload)["message"]


@pytest.mark.parametrize("signal,fixture", SIGNALS)
def test_gzip_export_accepts_conformance_fixture(conn, signal, fixture):
    body = gzip.compress((FIXTURES / fixture).read_bytes())
    status, payload = _post(
        conn, f"/v1/{signal}", body, headers={"Content-Encoding": "gzip"}
    )
    assert status == 200, payload
    assert json.loads(payload) == {}


def test_corrupt_gzip_is_rejected(conn):
    status, payload = _post(
        conn, "/v1/logs", b"not gzip", headers={"Content-Encoding": "gzip"}
    )
    assert status == 400, payload
    assert "gzip" in json.loads(payload)["message"]


def test_unknown_content_encoding_is_rejected(conn):
    status, payload = _post(
        conn, "/v1/logs", b"{}", headers={"Content-Encoding": "br"}
    )
    assert status == 400, payload
    assert "Content-Encoding" in json.loads(payload)["message"]


def test_malformed_json_is_rejected(conn):
    status, payload = _post(conn, "/v1/logs", b'{"resourceLogs":')
    assert status == 400, payload
    assert json.loads(payload)["code"] == 3


def test_empty_body_is_rejected(conn):
    status, payload = _post(conn, "/v1/logs", b"")
    assert status == 400, payload


def test_get_is_not_routed(conn):
    conn.request("GET", "/v1/logs", headers={"Authorization": AUTH})
    response = conn.getresponse()
    response.read()
    assert response.status == 404


def _logs_ddl() -> list[str]:
    sql = (FIXTURES.parent / "otel_schema.sql").read_text()
    return [
        statement
        for statement in sql.split(";")
        if "otel_logs (" in statement or "ON otel_logs " in statement
    ]


def test_mistyped_schema_is_rejected(conn):
    with psycopg.connect(**conn_kwargs(), autocommit=True) as pg:
        pg.execute("DROP TABLE otel_logs")
        pg.execute(
            'CREATE TABLE otel_logs ("timestamp" VARCHAR NOT NULL) '
            "WITH (storage = 'search')"
        )
        try:
            body = (FIXTURES / "logs/basic.json").read_bytes()
            status, payload = _post(conn, "/v1/logs", body)
            assert status == 500, payload
            message = json.loads(payload)["message"]
            assert "invalid OpenTelemetry schema" in message, message
            assert '"timestamp"' in message, message
        finally:
            pg.execute("DROP TABLE otel_logs")
            for statement in _logs_ddl():
                pg.execute(statement)
