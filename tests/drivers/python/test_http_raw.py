"""Raw HTTP/1.1 tests for the SereneDB HTTP endpoint (tier-0 + test API).

Covers the session/protocol surface: head framing, fixed and chunked bodies
in both directions, keep-alive reuse, Expect: 100-continue, error statuses.
Skipped wholesale when no HTTP endpoint is configured (SDB_DRV_HTTP_PORT).
"""

from __future__ import annotations

import http.client
import json
import os
import socket

import pytest

HOST = os.environ.get("SDB_DRV_HOST", "localhost")
PORT = int(os.environ.get("SDB_DRV_HTTP_PORT", "9200"))


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
    c = http.client.HTTPConnection(HOST, PORT, timeout=10)
    yield c
    c.close()


def test_root_banner(conn):
    conn.request("GET", "/")
    response = conn.getresponse()
    assert response.status == 200
    body = json.loads(response.read())
    assert body["name"] == "serenedb"
    assert body["tagline"] == "You Know, for Search"


def test_head_root_has_length_no_body(conn):
    conn.request("HEAD", "/")
    response = conn.getresponse()
    assert response.status == 200
    assert int(response.getheader("Content-Length")) > 0
    assert response.read() == b""


def test_cluster_health(conn):
    conn.request("GET", "/_cluster/health")
    response = conn.getresponse()
    assert response.status == 200
    assert json.loads(response.read())["status"] == "green"


def test_not_found(conn):
    conn.request("GET", "/definitely/not/a/route")
    response = conn.getresponse()
    assert response.status == 404
    assert json.loads(response.read())["error"] == "not_found"


def test_keep_alive_reuse(conn):
    for i in range(50):
        conn.request("GET", "/_cluster/health")
        response = conn.getresponse()
        assert response.status == 200
        response.read()
        assert response.getheader("Connection") == "keep-alive"


def test_connection_close_honored(conn):
    conn.request("GET", "/", headers={"Connection": "close"})
    response = conn.getresponse()
    assert response.status == 200
    assert response.getheader("Connection") == "close"
    response.read()


class TestTestApi:
    """Requires --network_http_test_api; skipped when /_test/ping is absent."""

    @pytest.fixture(autouse=True)
    def _require_test_api(self, conn):
        conn.request("GET", "/_test/ping")
        response = conn.getresponse()
        response.read()
        if response.status == 404:
            pytest.skip("test API not enabled")

    def test_echo_fixed_body(self, conn):
        payload = b"x" * 100_000
        conn.request("POST", "/_test/echo", body=payload,
                     headers={"Content-Type": "application/octet-stream"})
        response = conn.getresponse()
        assert response.status == 200
        assert int(response.getheader("Content-Length")) == len(payload)
        assert response.read() == payload

    def test_echo_chunked_request(self, conn):
        def chunks():
            for piece in (b"alpha-", b"beta-", b"gamma"):
                yield piece

        conn.request("POST", "/_test/echo", body=chunks(),
                     headers={"Content-Type": "text/plain"})
        response = conn.getresponse()
        assert response.status == 200
        assert response.read() == b"alpha-beta-gamma"

    def test_chunked_response_streaming(self, conn):
        n = 1_000_000
        conn.request("GET", f"/_test/bytes?n={n}")
        response = conn.getresponse()
        assert response.status == 200
        assert response.getheader("Transfer-Encoding") == "chunked"
        body = response.read()
        assert len(body) == n
        assert body[:26] == b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    def test_expect_100_continue(self, conn):
        payload = b"continue-me"
        conn.request("POST", "/_test/echo", body=payload,
                     headers={"Expect": "100-continue",
                              "Content-Type": "text/plain"})
        response = conn.getresponse()
        assert response.status == 200
        assert response.read() == payload

    def test_status_passthrough(self, conn):
        for code in (200, 201, 404, 418, 503):
            conn.request("GET", f"/_test/status?code={code}")
            response = conn.getresponse()
            assert response.status == code
            response.read()

    def test_fuzz_binary_garbage(self, conn):
        payload = bytes(range(256)) * 64
        conn.request("POST", "/_test/fuzz", body=payload)
        response = conn.getresponse()
        assert response.status == 200
        assert json.loads(response.read())["received"] == len(payload)
