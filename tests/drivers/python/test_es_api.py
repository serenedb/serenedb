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


# --- write path: _bulk / _doc / _refresh / get / count ----------------------


def _bulk(conn, index, payload, refresh=False):
    path = f"/{index}/_bulk" + ("?refresh=true" if refresh else "")
    conn.request(
        "POST",
        path,
        body=payload,
        headers={"Content-Type": "application/x-ndjson"},
    )
    response = conn.getresponse()
    return response.status, json.loads(response.read())


def test_bulk_index_and_create(conn, index):
    payload = (
        '{"index":{"_id":"1"}}\n'
        '{"title":"The Pelican Brief","author":"john","year":1992,'
        '"rating":4.5,"published":"1992-02-01T00:00:00Z","in_print":true}\n'
        '{"create":{}}\n'
        '{"title":"A Time to Kill","year":1989,"published":612918000000}\n'
        '{"index":{"_id":"3","_index":"%s"}}\n'
        '{"title":"The Firm","unmapped_extra":"source only"}\n'
    ) % index
    status, body = _bulk(conn, index, payload, refresh=True)
    assert status == 200
    assert body["errors"] is False
    assert len(body["items"]) == 3
    first = body["items"][0]["index"]
    assert first["_id"] == "1"
    assert first["status"] == 201
    assert first["result"] == "created"
    created = body["items"][1]["create"]
    assert len(created["_id"]) == 20  # autogenerated
    assert body["items"][2]["index"]["_id"] == "3"

    status, body = _request(conn, "GET", f"/{index}/_count")
    assert status == 200
    assert body["count"] == 3


def test_bulk_errors(conn, index):
    cases = [
        ('{"delete":{"_id":"1"}}\n', "illegal_argument_exception"),
        ('{}\n{"f":1}\n', "illegal_argument_exception"),
        ('{"index":{"_index":"other"}}\n{"f":1}\n',
         "illegal_argument_exception"),
        ('{"index":{"_id":"9"}}\n', "illegal_argument_exception"),
        ('{"index":{"_id":""}}\n{"f":1}\n', "illegal_argument_exception"),
        ('{"index":{}}\n{"year":"not a number"}\n',
         "mapper_parsing_exception"),
        ('{"index":{}}\n[1,2]\n', "mapper_parsing_exception"),
    ]
    for payload, expected_type in cases:
        status, body = _bulk(conn, index, payload)
        assert status == 400, payload
        assert body["error"]["type"] == expected_type, payload
    # a failed bulk inserts nothing
    status, body = _request(conn, "GET", f"/{index}/_count")
    assert body["count"] == 0


def test_bulk_missing_index(conn):
    status, body = _bulk(conn, "drv_es_missing", '{"index":{}}\n{"f":1}\n')
    assert status == 404
    assert body["error"]["type"] == "index_not_found_exception"


def test_doc_lifecycle(conn, index):
    doc = {"title": "The Client", "year": 1993, "rating": None}
    status, body = _request(conn, "PUT", f"/{index}/_doc/4", doc)
    assert status == 201
    assert body["_id"] == "4"
    assert body["result"] == "created"

    status, body = _request(conn, "POST", f"/{index}/_doc", doc)
    assert status == 201
    assert len(body["_id"]) == 20

    # source round trip is byte-faithful
    status, body = _request(conn, "GET", f"/{index}/_doc/4")
    assert status == 200
    assert body["found"] is True
    assert body["_source"] == doc

    status, body = _request(conn, "GET", f"/{index}/_source/4")
    assert status == 200
    assert body == doc


def test_doc_conflict_and_errors(conn, index):
    status, _ = _request(conn, "PUT", f"/{index}/_doc/dup", {"year": 1})
    assert status == 201
    status, body = _request(conn, "PUT", f"/{index}/_doc/dup", {"year": 2})
    assert status == 409
    assert body["error"]["type"] == "version_conflict_engine_exception"

    conn.request("POST", f"/{index}/_doc", body="")
    response = conn.getresponse()
    body = json.loads(response.read())
    assert response.status == 400
    assert body["error"]["type"] == "illegal_argument_exception"

    status, body = _request(conn, "PUT", "/drv_es_missing/_doc/1", {"a": 1})
    assert status == 404
    assert body["error"]["type"] == "index_not_found_exception"


def test_get_doc_missing(conn, index):
    status, body = _request(conn, "GET", f"/{index}/_doc/zzz")
    assert status == 404
    assert body == {"_index": index, "_id": "zzz", "found": False}

    status, body = _request(conn, "GET", f"/{index}/_source/zzz")
    assert status == 404
    assert body["error"]["type"] == "resource_not_found_exception"

    status, body = _request(conn, "GET", "/drv_es_missing/_doc/1")
    assert status == 404
    assert body["error"]["type"] == "index_not_found_exception"


def test_refresh(conn, index):
    status, body = _request(conn, "POST", f"/{index}/_refresh")
    assert status == 200
    assert body == {"_shards": {"total": 1, "successful": 1, "failed": 0}}

    status, body = _request(conn, "POST", "/_refresh")
    assert status == 200

    status, body = _request(conn, "POST", "/drv_es_missing/_refresh")
    assert status == 404
    assert body["error"]["type"] == "index_not_found_exception"


def test_count(conn, index):
    status, body = _request(conn, "GET", f"/{index}/_count")
    assert status == 200
    assert body["count"] == 0
    assert body["_shards"]["successful"] == 1

    # clients send an empty object body for match_all
    status, body = _request(conn, "POST", f"/{index}/_count", {})
    assert status == 200
    assert body["count"] == 0

    status, body = _request(conn, "POST", f"/{index}/_count",
                            {"query": {"match_all": {}}})
    assert status == 200
    assert body["count"] == 0

    # only a top-level "query" key is legal, like ES
    status, body = _request(conn, "POST", f"/{index}/_count",
                            {"match_all": {}})
    assert status == 400
    assert body["error"]["type"] == "parsing_exception"

    status, body = _request(conn, "GET", "/drv_es_missing/_count")
    assert status == 404
    assert body["error"]["type"] == "index_not_found_exception"


def test_product_header(conn, index):
    """Official ES clients gate on X-Elastic-Product on every response."""
    for method, path in [
        ("GET", "/"),
        ("GET", f"/{index}/_count"),
        ("GET", f"/{index}/_mapping"),
        ("GET", "/missing_index_zz/_mapping"),
    ]:
        conn.request(method, path)
        response = conn.getresponse()
        response.read()
        assert response.getheader("X-Elastic-Product") == "Elasticsearch", path


# --- _search -----------------------------------------------------------------


SEARCH_DOCS = (
    '{"index":{"_id":"1"}}\n'
    '{"title":"The Quick Brown Fox","author":"aesop","year":1900,'
    '"rating":4.5}\n'
    '{"index":{"_id":"2"}}\n'
    '{"title":"The lazy dog sleeps","author":"aesop","year":1950}\n'
    '{"index":{"_id":"3"}}\n'
    '{"title":"quick thinking saves the day","author":"verne","year":1950}\n'
)


@pytest.fixture()
def corpus(conn, index):
    status, body = _bulk(conn, index, SEARCH_DOCS, refresh=True)
    assert status == 200, body
    return index


def _search(conn, index, body=None, qs=""):
    return _request(conn, "POST", f"/{index}/_search{qs}", body)


def test_search_match_all_defaults(conn, corpus):
    status, body = _request(conn, "GET", f"/{corpus}/_search")
    assert status == 200
    assert body["timed_out"] is False
    assert body["_shards"]["successful"] == 1
    hits = body["hits"]
    assert hits["total"] == {"value": 3, "relation": "eq"}
    assert hits["max_score"] == 1.0
    assert len(hits["hits"]) == 3
    first = hits["hits"][0]
    assert first["_index"] == corpus
    assert first["_score"] == 1.0
    assert "_source" in first


def test_search_match(conn, corpus):
    # operator=or, analysis is case-insensitive
    status, body = _search(conn, corpus,
                           {"query": {"match": {"title": "QUICK dog"}}})
    assert status == 200
    assert body["hits"]["total"]["value"] == 3

    status, body = _search(
        conn, corpus,
        {"query": {"match": {"title": {"query": "quick fox",
                                       "operator": "and"}}}})
    assert status == 200
    assert [h["_id"] for h in body["hits"]["hits"]] == ["1"]


def test_search_term_range_sort(conn, corpus):
    status, body = _search(conn, corpus, {
        "query": {"term": {"author": "aesop"}},
        "sort": [{"year": {"order": "desc"}}],
    })
    assert status == 200
    hits = body["hits"]
    assert [h["_id"] for h in hits["hits"]] == ["2", "1"]
    assert hits["max_score"] is None  # field sort -> unscored, like ES
    assert hits["hits"][0]["_score"] is None
    assert hits["hits"][0]["sort"] == [1950]

    status, body = _search(
        conn, corpus, {"query": {"range": {"year": {"gte": 1940}}}})
    assert sorted(h["_id"] for h in body["hits"]["hits"]) == ["2", "3"]


def test_search_bool(conn, corpus):
    status, body = _search(conn, corpus, {"query": {"bool": {
        "must": [{"match": {"title": "quick"}}],
        "filter": [{"range": {"year": {"gte": 1900}}}],
        "must_not": [{"term": {"author": "verne"}}],
    }}})
    assert status == 200
    assert [h["_id"] for h in body["hits"]["hits"]] == ["1"]

    status, body = _search(conn, corpus, {"query": {"bool": {
        "should": [{"match": {"title": "fox"}}, {"match": {"title": "dog"}}],
    }}})
    assert sorted(h["_id"] for h in body["hits"]["hits"]) == ["1", "2"]


def test_search_paging_and_source(conn, corpus):
    status, body = _search(conn, corpus, {
        "query": {"match_all": {}}, "size": 2, "from": 1, "sort": ["year"],
    })
    assert status == 200
    assert body["hits"]["total"]["value"] == 3
    assert len(body["hits"]["hits"]) == 2

    status, body = _search(
        conn, corpus, {"query": {"match_all": {}}, "_source": False})
    assert "_source" not in body["hits"]["hits"][0]

    # URL params apply when the body does not override them
    status, body = _search(conn, corpus, None, qs="?size=1&from=0")
    assert len(body["hits"]["hits"]) == 1
    assert body["hits"]["total"]["value"] == 3

    status, body = _search(conn, corpus, {"query": {"match_all": {}}},
                           qs="?rest_total_hits_as_int=true")
    assert body["hits"]["total"] == 3


def test_search_visibility_follows_refresh(conn, corpus):
    match_quick = {"query": {"match": {"title": "quick"}}}
    status, body = _request(conn, "POST", f"/{corpus}/_count", match_quick)
    assert body["count"] == 2

    _request(conn, "PUT", f"/{corpus}/_doc/4",
             {"title": "quick addendum", "year": 2000})
    status, body = _request(conn, "POST", f"/{corpus}/_count", match_quick)
    assert body["count"] == 2  # not searchable until refresh

    _request(conn, "POST", f"/{corpus}/_refresh")
    status, body = _request(conn, "POST", f"/{corpus}/_count", match_quick)
    assert body["count"] == 3


def test_search_errors(conn, corpus):
    cases = [
        ({"query": {"fuzzy": {"title": "x"}}}, "illegal_argument_exception"),
        ({"query": {"bool": {"should": [{"match": {"title": "fox"}},
                                        {"term": {"author": "aesop"}}]}}},
         "illegal_argument_exception"),
        ({"size": 20000}, "illegal_argument_exception"),
        ({"aggs": {}}, "illegal_argument_exception"),
        ({"query": {"term": {"missing_field": "x"}}},
         "query_shard_exception"),
    ]
    for body, expected in cases:
        status, response = _search(conn, corpus, body)
        assert status == 400, body
        assert response["error"]["type"] == expected, body

    status, body = _request(conn, "GET", "/drv_es_missing/_search")
    assert status == 404
    assert body["error"]["type"] == "index_not_found_exception"
