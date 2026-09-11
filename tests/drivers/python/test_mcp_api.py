"""MCP (Model Context Protocol) endpoint tests.

Exercises the stateless Streamable HTTP transport at /_mcp: JSON-RPC
initialize, tools/list and the documentation tools over the embedded docs.
Skipped wholesale when no HTTP endpoint is configured (SDB_DRV_HTTP_PORT).
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import socket

import pytest

HOST = os.environ.get("SDB_DRV_HOST", "localhost")
PORT = int(os.environ.get("SDB_DRV_HTTP_PORT", "9200"))
USER = os.environ.get("SDB_DRV_USER", "postgres")
PASSWORD = os.environ.get("SDB_DRV_PASSWORD", "")
AUTH = "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()


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


def post(conn, payload, raw: str | None = None):
    body = raw if raw is not None else json.dumps(payload)
    conn.request(
        "POST",
        "/_mcp",
        body=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": AUTH,
        },
    )
    response = conn.getresponse()
    data = response.read()
    return response, (json.loads(data) if data else None)


def rpc(conn, method, params=None, id=1):
    response, body = post(
        conn, {"jsonrpc": "2.0", "id": id, "method": method, "params": params or {}}
    )
    assert response.status == 200, body
    assert body["jsonrpc"] == "2.0" and body["id"] == id
    return body


def call_tool(conn, name, arguments):
    body = rpc(conn, "tools/call", {"name": name, "arguments": arguments}, id=7)
    assert "result" in body, body
    result = body["result"]
    assert result["content"][0]["type"] == "text"
    return result["content"][0]["text"], result.get("isError", False)


def test_get_is_method_not_allowed(conn):
    conn.request("GET", "/_mcp", headers={"Authorization": AUTH})
    response = conn.getresponse()
    body = json.loads(response.read())
    assert response.status == 405
    assert response.getheader("Allow") == "POST"
    assert body["error"]["code"] == -32000


def test_initialize(conn):
    body = rpc(
        conn,
        "initialize",
        {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "pytest", "version": "0"},
        },
    )
    result = body["result"]
    assert result["protocolVersion"] == "2025-03-26"
    assert result["serverInfo"]["name"] == "serenedb"
    assert result["capabilities"]["tools"] == {"listChanged": False}
    assert "instructions" in result


def test_initialize_unknown_version_falls_back(conn):
    body = rpc(conn, "initialize", {"protocolVersion": "1999-01-01"})
    assert body["result"]["protocolVersion"] == "2025-06-18"


def test_initialized_notification_is_accepted(conn):
    response, body = post(
        conn, {"jsonrpc": "2.0", "method": "notifications/initialized"}
    )
    assert response.status == 202
    assert body is None


def test_ping(conn):
    assert rpc(conn, "ping", id="p-1")["result"] == {}


def test_tools_list(conn):
    tools = rpc(conn, "tools/list")["result"]["tools"]
    names = [t["name"] for t in tools]
    assert names == [
        "search_docs",
        "read_doc",
        "list_docs",
        "list_objects",
        "describe_object",
    ]
    for tool in tools:
        assert tool["description"]
        assert tool["inputSchema"]["type"] == "object"
    assert rpc(conn, "tools/list")["result"].get("nextCursor") is None


def test_unknown_method(conn):
    body = rpc(conn, "no/such/method")
    assert body["error"]["code"] == -32601


def test_parse_error(conn):
    response, body = post(conn, None, raw="{not json")
    assert response.status == 400
    assert body["error"]["code"] == -32700
    assert body["id"] is None


def test_batch_is_rejected(conn):
    response, body = post(conn, None, raw="[]")
    assert response.status == 400
    assert body["error"]["code"] == -32600


def test_unknown_tool(conn):
    body = rpc(conn, "tools/call", {"name": "nope", "arguments": {}})
    assert body["error"]["code"] == -32602


def test_invalid_argument_type(conn):
    body = rpc(
        conn, "tools/call", {"name": "search_docs", "arguments": {"query": 5}}
    )
    assert body["error"]["code"] == -32602


def test_search_docs(conn):
    text, is_error = call_tool(
        conn, "search_docs", {"query": "inverted index", "limit": 3}
    )
    assert not is_error
    assert text.startswith("[1] ")
    assert "path: sql/indexes/inverted/" in text
    assert text.count("\npath: ") == 3
    assert " - " in text.split("\n")[0]


def test_search_docs_empty_query(conn):
    text, is_error = call_tool(conn, "search_docs", {"query": "   "})
    assert is_error and "query" in text


def test_search_docs_no_results(conn):
    text, is_error = call_tool(conn, "search_docs", {"query": "qzxvbnmqwerty"})
    assert not is_error and text == "No results."


def test_read_doc_title_row_is_whole_page(conn):
    text, is_error = call_tool(
        conn, "read_doc", {"path": "sql/functions/search/scoring.md#Relevance_Scoring"}
    )
    assert not is_error
    assert text.startswith(
        "path: sql/functions/search/scoring.md#Relevance_Scoring\n\n# Relevance Scoring\n"
    )
    assert "\n## Scorer Functions\n" in text and "\n## Quick start\n" in text
    assert "import " not in text and "<SqlLogicTest" not in text


def test_read_doc_heading_row(conn):
    text, is_error = call_tool(
        conn,
        "read_doc",
        {"path": "sql/functions/search/scoring.md#Relevance_Scoring#Scorer_Functions"},
    )
    assert not is_error
    assert text.startswith(
        "path: sql/functions/search/scoring.md#Relevance_Scoring#Scorer_Functions\n"
        "in: Relevance Scoring\n\n## Scorer Functions\n"
    )
    assert "BM25" in text
    assert "Quick start" not in text


def test_read_doc_whole_page_doc(conn):
    text, is_error = call_tool(conn, "read_doc", {"path": "cookbook/search/autocomplete.md"})
    assert not is_error
    assert text.startswith("path: cookbook/search/autocomplete.md\n\n# Autocomplete\n")


def test_read_doc_split_page_has_no_bare_row(conn):
    text, is_error = call_tool(conn, "read_doc", {"path": "sql/functions/search/scoring.md"})
    assert is_error and "sql/functions/search/scoring.md" in text


def test_read_doc_unknown_path(conn):
    text, is_error = call_tool(conn, "read_doc", {"path": "nope/missing.md"})
    assert is_error and "nope/missing.md" in text


def test_read_doc_unknown_heading(conn):
    text, is_error = call_tool(
        conn, "read_doc", {"path": "sql/functions/search/scoring.md#Relevance_Scoring#Nope"}
    )
    assert is_error and "Nope" in text


def test_list_docs(conn):
    text, is_error = call_tool(conn, "list_docs", {"prefix": "sql/functions/search/"})
    assert not is_error
    lines = text.split("\n")
    assert all(line.startswith("sql/functions/search/") for line in lines)
    assert "sql/functions/search/scoring.md#Relevance_Scoring - Relevance Scoring" in lines
    assert not any("#Relevance_Scoring#" in line for line in lines)
    everything, _ = call_tool(conn, "list_docs", {})
    assert len(everything.split("\n")) > len(lines)


def test_list_docs_sections(conn):
    text, is_error = call_tool(
        conn, "list_docs", {"prefix": "sql/functions/search/scoring.md"}
    )
    assert not is_error
    lines = text.split("\n")
    assert lines[0] == "sql/functions/search/scoring.md#Relevance_Scoring - Relevance Scoring"
    assert "sql/functions/search/scoring.md#Relevance_Scoring#Scorer_Functions - Scorer Functions (Relevance Scoring)" in lines
    assert len(lines) > 5


def test_list_objects_by_kind(conn):
    text, is_error = call_tool(conn, "list_objects", {"kind": "index_type"})
    assert not is_error
    lines = sorted(text.split("\n"))
    assert len(lines) == 2
    assert lines[0].startswith("art - ")
    assert lines[1].startswith("inverted - ")


def test_list_objects_all_kinds_are_labelled(conn):
    text, is_error = call_tool(conn, "list_objects", {})
    assert not is_error
    lines = text.split("\n")
    assert len(lines) > 400
    kinds = {"function", "statement", "tokenizer", "type", "setting", "index_type"}
    seen = set()
    for line in lines:
        for kind in kinds:
            if f"({kind})" in line:
                seen.add(kind)
    assert seen == kinds


def test_list_objects_unknown_kind(conn):
    text, is_error = call_tool(conn, "list_objects", {"kind": "nonesuch"})
    assert is_error and "Known kinds" in text


def test_describe_object(conn):
    text, is_error = call_tool(conn, "describe_object", {"name": "ts_phrase"})
    assert not is_error
    assert text.startswith("ts_phrase(")
    assert "(function)" in text.split("\n")[0]
    assert "path: sql/functions/search/full-text.md#" in text


def test_describe_object_reports_every_kind(conn):
    text, is_error = call_tool(conn, "describe_object", {"name": "minhash"})
    assert not is_error
    headers = [
        line for line in text.split("\n") if line.endswith(("(function)", "(tokenizer)"))
    ]
    assert len(headers) == 2
    assert "\n---\n" in text


def test_describe_object_is_case_insensitive(conn):
    text, is_error = call_tool(conn, "describe_object", {"name": "TS_PHRASE"})
    assert not is_error and text.startswith("ts_phrase(")


def test_describe_object_unknown_suggests(conn):
    text, is_error = call_tool(conn, "describe_object", {"name": "to_tsvector"})
    assert is_error
    assert "No documented object named: to_tsvector" in text


def test_describe_object_unknown_lists_similar(conn):
    text, is_error = call_tool(conn, "describe_object", {"name": "phrase"})
    assert is_error
    assert "Maybe you meant:" in text
    assert "ts_phrase(" in text


def test_describe_object_empty_name(conn):
    text, is_error = call_tool(conn, "describe_object", {"name": "   "})
    assert is_error and "name" in text
