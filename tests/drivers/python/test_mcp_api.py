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
TOKEN = os.environ.get("SDB_DRV_HTTP_TOKEN", "")
AUTH = (
    f"Bearer {TOKEN}"
    if TOKEN
    else "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
)


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
        "check_sql",
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


def test_read_doc_page_path_opens_its_title_row(conn):
    text, is_error = call_tool(conn, "read_doc", {"path": "sql/functions/search/scoring.md"})
    assert not is_error
    assert text.startswith("path: sql/functions/search/scoring.md#Relevance_Scoring\n")


def test_read_doc_follows_anchors_and_relative_links(conn):
    for path in ("sql/functions/search/full-text.md#ts_levenshtein",
                 "./full-text.md#ts_levenshtein"):
        text, is_error = call_tool(conn, "read_doc", {"path": path})
        assert not is_error, text
        first = text.split("\n", 1)[0]
        assert first.startswith("path: sql/functions/search/full-text.md#"), first
        assert "ts_levenshtein(" in first
    text, is_error = call_tool(
        conn, "read_doc", {"path": "sql/indexes/inverted/maintenance.md#session-settings"})
    assert not is_error and text.split("\n", 1)[0].endswith("#Session_settings")


def test_read_doc_opens_site_urls_and_paths_without_extension(conn):
    for path in ("https://serenedb.com/docs/sql/indexes#index-types",
                 "sql/indexes#index-types"):
        text, is_error = call_tool(conn, "read_doc", {"path": path})
        assert not is_error, text
        assert text.startswith("path: sql/indexes/index.md#Indexes#Index_Types\n"), text
    text, is_error = call_tool(
        conn, "read_doc", {"path": "https://duckdb.org/docs/sql/indexes"})
    assert is_error


def test_read_doc_rewrites_links_to_paths_it_accepts(conn):
    text, is_error = call_tool(
        conn, "read_doc", {"path": "sql/functions/search/full-text.md#ts_levenshtein"})
    assert not is_error
    assert "](sql/indexes/inverted/maintenance.md#session-settings)" in text
    assert "](../" not in text and "](./" not in text


def test_read_doc_cuts_a_long_page_and_lists_its_sections(conn):
    text, is_error = call_tool(conn, "read_doc", {"path": "configuration/overview.md"})
    assert not is_error
    assert "(Cut at " in text
    assert "\nconfiguration/overview.md#Configuration#" in text


def test_read_doc_unknown_path(conn):
    text, is_error = call_tool(conn, "read_doc", {"path": "nope/missing.md"})
    assert is_error and "nope/missing.md" in text


def test_read_doc_unknown_heading_falls_back_to_its_section(conn):
    text, is_error = call_tool(
        conn, "read_doc", {"path": "sql/functions/search/scoring.md#Relevance_Scoring#Nope"}
    )
    assert not is_error
    assert text.startswith("path: sql/functions/search/scoring.md#Relevance_Scoring\n")


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
    kinds = {"function", "statement", "tokenizer", "type", "setting", "index_type", "command"}
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
    text, is_error = call_tool(conn, "describe_object", {"name": "VARCHAR"})
    assert not is_error
    headers = [
        line for line in text.split("\n") if line.endswith(("(function)", "(type)"))
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


def test_describe_object_unknown_lists_the_pages_mentioning_it(conn):
    text, is_error = call_tool(conn, "describe_object", {"name": "to_tsvector"})
    assert is_error
    assert "The documentation mentions it here" in text
    assert "\n  compatibility/" in text


def test_describe_object_reports_what_only_the_server_knows(conn):
    text, is_error = call_tool(conn, "describe_object", {"name": "azure_account_name"})
    assert not is_error
    assert "The server has it, undocumented:" in text
    assert "azure_account_name VARCHAR" in text and "(setting)" in text
    text, is_error = call_tool(conn, "describe_object", {"name": "date_trunc()"})
    assert not is_error and text.startswith("date_trunc(")


def test_check_sql_plans_without_running(conn):
    text, is_error = call_tool(conn, "check_sql", {"sql": "SELECT 1 AS one"})
    assert not is_error and text.startswith("Valid. The plan:\n")
    text, is_error = call_tool(conn, "check_sql", {"sql": "SELECT ';' AS semi;"})
    assert not is_error and text.startswith("Valid.")


def test_check_sql_reports_the_server_error(conn):
    text, is_error = call_tool(conn, "check_sql", {"sql": "SELEC 1"})
    assert not is_error
    assert text.startswith("Invalid: Parser Error")
    assert "LINE 1: SELEC 1" in text


def test_check_sql_refuses_what_it_would_run(conn):
    for sql in ("SELECT 1; SELECT 2", "EXPLAIN ANALYZE SELECT 1"):
        text, is_error = call_tool(conn, "check_sql", {"sql": sql})
        assert is_error and text.startswith("check_sql:"), text


def test_search_docs_reads_questions_and_pasted_errors(conn):
    for query in ("How do I create an inverted index?",
                  "Alias: `base64` (see Catalog Error: x)",
                  "SELECT * FROM t WHERE body @@ 'fox'"):
        text, is_error = call_tool(conn, "search_docs", {"query": query})
        assert not is_error and text.startswith("[1] "), (query, text)


def test_describe_object_unknown_lists_similar(conn):
    text, is_error = call_tool(conn, "describe_object", {"name": "phrase"})
    assert is_error
    assert "Maybe you meant:" in text
    assert "ts_phrase(" in text


def test_describe_object_resolves_aliases(conn):
    text, is_error = call_tool(conn, "describe_object", {"name": "INT8"})
    assert not is_error
    assert "BIGINT (type)" in text


def test_describe_object_by_kind(conn):
    text, is_error = call_tool(
        conn, "describe_object", {"name": "VARCHAR", "kind": "type"}
    )
    assert not is_error
    headers = [
        line for line in text.split("\n") if line.endswith(("(function)", "(type)"))
    ]
    assert headers and all(line.endswith("(type)") for line in headers)


def test_describe_object_unknown_in_kind(conn):
    text, is_error = call_tool(
        conn, "describe_object", {"name": "ts_phrase", "kind": "type"}
    )
    assert is_error
    assert "No documented type named: ts_phrase" in text


def test_describe_object_empty_name(conn):
    text, is_error = call_tool(conn, "describe_object", {"name": "   "})
    assert is_error and "name" in text
