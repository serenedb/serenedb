"""ES-compatible REST API tests (index lifecycle).

Exercises the es_*() table functions through the thin HTTP handlers:
create/exists/mapping/_cat/delete plus every error envelope. Skipped
wholesale when no HTTP endpoint is configured (SDB_DRV_HTTP_PORT).
"""

from __future__ import annotations

import json
import os
import socket

import pytest

HOST = os.environ.get("SDB_DRV_HOST", "localhost")
PORT = int(os.environ.get("SDB_DRV_HTTP_PORT", "9200"))

INDEX = "drv_es_lifecycle"

MAPPINGS = {
    "mappings": {
        "properties": {
            "title": {"type": "text"},
            "author": {"type": "keyword"},
            "year": {"type": "integer"},
            "rating": {"type": "double"},
            "published": {"type": "date"},
            "in_print": {"type": "boolean"},
        }
    }
}


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
    import http.client

    c = http.client.HTTPConnection(HOST, PORT, timeout=30)
    yield c
    c.close()


@pytest.fixture()
def index(conn):
    """A fresh index for one test; dropped afterwards."""
    _request(conn, "DELETE", f"/{INDEX}")
    status, body = _request(conn, "PUT", f"/{INDEX}", MAPPINGS)
    assert status == 200, body
    yield INDEX
    _request(conn, "DELETE", f"/{INDEX}")


def _request(conn, method, path, body=None):
    payload = json.dumps(body) if body is not None else None
    headers = {"Content-Type": "application/json"} if payload else {}
    conn.request(method, path, body=payload, headers=headers)
    response = conn.getresponse()
    raw = response.read()
    parsed = json.loads(raw) if raw else None
    return response.status, parsed


def test_create_index(conn):
    _request(conn, "DELETE", "/drv_es_create")
    status, body = _request(conn, "PUT", "/drv_es_create", MAPPINGS)
    assert status == 200
    assert body == {
        "acknowledged": True,
        "shards_acknowledged": True,
        "index": "drv_es_create",
    }
    status, body = _request(conn, "DELETE", "/drv_es_create")
    assert status == 200
    assert body == {"acknowledged": True}


def test_create_index_no_body(conn):
    _request(conn, "DELETE", "/drv_es_nobody")
    status, body = _request(conn, "PUT", "/drv_es_nobody")
    assert status == 200
    assert body["acknowledged"] is True
    status, body = _request(conn, "GET", "/drv_es_nobody/_mapping")
    assert status == 200
    assert body == {"drv_es_nobody": {"mappings": {"properties": {}}}}
    _request(conn, "DELETE", "/drv_es_nobody")


def test_create_existing_index_conflicts(conn, index):
    status, body = _request(conn, "PUT", f"/{index}", MAPPINGS)
    assert status == 400
    assert body["error"]["type"] == "resource_already_exists_exception"
    assert body["status"] == 400


def test_index_exists(conn, index):
    conn.request("HEAD", f"/{index}")
    response = conn.getresponse()
    response.read()
    assert response.status == 200

    conn.request("HEAD", "/drv_es_missing")
    response = conn.getresponse()
    response.read()
    assert response.status == 404


def test_get_mapping_round_trip(conn, index):
    status, body = _request(conn, "GET", f"/{index}/_mapping")
    assert status == 200
    # ES normalizes: properties come back alphabetical, type-only fields
    # unchanged. text vs keyword survives the round trip.
    assert body == {index: MAPPINGS}


def test_mapping_missing_index(conn):
    status, body = _request(conn, "GET", "/drv_es_missing/_mapping")
    assert status == 404
    assert body["error"]["type"] == "index_not_found_exception"
    assert "drv_es_missing" in body["error"]["reason"]


def test_delete_missing_index(conn):
    status, body = _request(conn, "DELETE", "/drv_es_missing")
    assert status == 404
    assert body["error"]["type"] == "index_not_found_exception"


def test_invalid_index_name(conn):
    for bad in ["BadCase", "_leading", "x" * 256]:
        status, body = _request(conn, "PUT", f"/{bad}")
        assert status == 400, bad
        assert body["error"]["type"] == "invalid_index_name_exception", bad


def test_invalid_mapping_type(conn):
    status, body = _request(
        conn,
        "PUT",
        "/drv_es_badtype",
        {"mappings": {"properties": {"f": {"type": "geo_shape"}}}},
    )
    assert status == 400
    assert body["error"]["type"] == "mapper_parsing_exception"
    assert "geo_shape" in body["error"]["reason"]


def test_invalid_mapping_field(conn):
    status, body = _request(
        conn,
        "PUT",
        "/drv_es_badfield",
        {"mappings": {"properties": {"_meta": {"type": "long"}}}},
    )
    assert status == 400
    assert body["error"]["type"] == "mapper_parsing_exception"


def test_malformed_body(conn):
    conn.request(
        "PUT",
        "/drv_es_badjson",
        body="not json at all",
        headers={"Content-Type": "application/json"},
    )
    response = conn.getresponse()
    body = json.loads(response.read())
    assert response.status == 400
    assert body["error"]["type"] == "mapper_parsing_exception"


def test_cat_indices(conn, index):
    conn.request("GET", "/_cat/indices")
    response = conn.getresponse()
    assert response.status == 200
    text = response.read().decode()
    line = next(l for l in text.splitlines() if f" {index} " in l)
    cols = line.split()
    assert cols[0] == "green"
    assert cols[1] == "open"
    assert cols[2] == index

    status, body = _request(conn, "GET", "/_cat/indices?format=json")
    assert status == 200
    row = next(r for r in body if r["index"] == index)
    assert row["health"] == "green"
    assert row["status"] == "open"
    assert row["docs.count"] == "0"
