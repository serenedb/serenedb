"""MCP (Model Context Protocol) endpoint tests.

Exercises the stateless Streamable HTTP transport at /mcp: JSON-RPC
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
        "/mcp",
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
    conn.request("GET", "/mcp", headers={"Authorization": AUTH})
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
    assert names == ["search_docs", "read_doc", "list_docs"]
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


def test_search_docs_empty_query(conn):
    text, is_error = call_tool(conn, "search_docs", {"query": "   "})
    assert is_error and "query" in text


def test_search_docs_no_results(conn):
    text, is_error = call_tool(conn, "search_docs", {"query": "qzxvbnmqwerty"})
    assert not is_error and text == "No results."


def test_read_doc_page(conn):
    text, is_error = call_tool(
        conn, "read_doc", {"path": "sql/functions/search/scoring.md"}
    )
    assert not is_error
    assert text.startswith("# Relevance Scoring\npath: sql/functions/search/scoring.md\n")
    assert "\n## Scorer Functions\n" in text
    assert "import " not in text and "<SqlLogicTest" not in text


def test_read_doc_section(conn):
    text, is_error = call_tool(
        conn,
        "read_doc",
        {"path": "sql/functions/search/scoring.md", "section": "Scorer Functions"},
    )
    assert not is_error
    assert text.startswith("Scorer Functions\npath: sql/functions/search/scoring.md\n")
    assert "BM25" in text
    assert "## Quick start" not in text


def test_read_doc_unknown_path(conn):
    text, is_error = call_tool(conn, "read_doc", {"path": "nope/missing.md"})
    assert is_error and "nope/missing.md" in text


def test_read_doc_unknown_section(conn):
    text, is_error = call_tool(
        conn,
        "read_doc",
        {"path": "sql/functions/search/scoring.md", "section": "Nope"},
    )
    assert is_error and "Nope" in text


def test_list_docs(conn):
    text, is_error = call_tool(conn, "list_docs", {"prefix": "sql/functions/search/"})
    assert not is_error
    lines = text.split("\n")
    assert all(line.startswith("sql/functions/search/") for line in lines)
    assert "sql/functions/search/scoring.md - Relevance Scoring" in lines
    everything, _ = call_tool(conn, "list_docs", {})
    assert len(everything.split("\n")) > len(lines)
