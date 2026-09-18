"""OTLP/HTTP API tests (POST /v1/logs, /v1/traces, /v1/metrics).

Exercises the otlp_*() table functions through the thin HTTP handlers, using
the same conformance payloads the sqllogic contract test uses. Skipped
wholesale when no OTLP endpoint is configured (SDB_DRV_OTLP_PORT).
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import pathlib
import socket

import pytest

HOST = os.environ.get("SDB_DRV_HOST", "localhost")
PORT = int(os.environ.get("SDB_DRV_OTLP_PORT", "4318"))
USER = os.environ.get("SDB_DRV_USER", "postgres")
PASSWORD = os.environ.get("SDB_DRV_PASSWORD", "")

AUTH = "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()

FIXTURES = (
    pathlib.Path(__file__).resolve().parents[3] / "resources" / "otel" / "conformance"
)

SIGNALS = [
    ("logs", "logs/basic.otlp.json"),
    ("logs", "logs/anyvalue_types.otlp.json"),
    ("logs", "logs/empty_ids_and_zero_timestamp.otlp.json"),
    ("logs", "logs/multi_resource_scope.otlp.json"),
    ("traces", "traces/span_with_events_and_links.otlp.json"),
    ("traces", "traces/kinds_and_statuses.otlp.json"),
    ("metrics", "metrics/mixed_batch.otlp.json"),
    ("metrics", "metrics/exponential_histogram.otlp.json"),
    ("metrics", "metrics/int_and_multi_datapoint.otlp.json"),
    ("logs", "logs/upstream.otlp.json"),
    ("traces", "traces/upstream.otlp.json"),
    ("traces", "traces/enum_names_and_unknown_fields.otlp.json"),
    ("metrics", "metrics/upstream.otlp.json"),
]


def _reachable() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=2):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _reachable(), reason=f"no OTLP endpoint at {HOST}:{PORT}"
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
    wire = FIXTURES / fixture.replace(".otlp.json", ".otlp.pb")
    if not wire.exists():
        pytest.skip(f"{wire.name} not generated; run scripts/otel_fixtures.py")
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


def test_content_encoding_is_rejected(conn):
    status, payload = _post(
        conn, "/v1/logs", b"{}", headers={"Content-Encoding": "gzip"}
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
