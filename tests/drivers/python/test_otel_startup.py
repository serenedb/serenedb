"""Startup checks of an ?api=otel listener against the built-in schema.

Each case runs its own serened on a fresh data directory, so the tables can be
broken before the OpenTelemetry listener first sees them.
"""

from __future__ import annotations

import http.client
import json
import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from serened import Serened, reserve_port  # type: ignore[import-untyped]

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


def _break_schema(tmp_path: Path, statement: str) -> None:
    server = _Server(tmp_path)
    try:
        server.pg.execute(statement)
    finally:
        server.close()


def _start_otel(tmp_path: Path,
                params: str = "api=otel") -> subprocess.CompletedProcess:
    with reserve_port() as pg_sock, reserve_port() as http_sock:
        listen = (f"postgres://127.0.0.1:{pg_sock.getsockname()[1]},"
                  f"http://127.0.0.1:{http_sock.getsockname()[1]}?{params}")
        return subprocess.run(
            [SERENED_BIN, str(tmp_path / "data"), f"--listen={listen}"],
            capture_output=True, text=True, timeout=60,
        )


def test_mistyped_column_refuses_startup(tmp_path: Path) -> None:
    _break_schema(
        tmp_path,
        'CREATE TABLE otel_logs ("timestamp" VARCHAR NOT NULL) '
        "WITH (storage = 'search')",
    )
    result = _start_otel(tmp_path)
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "invalid OpenTelemetry schema" in output, output
    assert "timestamp" in output, output


FIXTURES = (
    Path(__file__).resolve().parents[3] / "resources" / "otel" / "conformance"
)


class _Server:
    def __init__(self, tmp_path: Path, params: str | None = None):
        self.serened = Serened(SERENED_BIN, datadir=str(tmp_path / "data"),
                               log_path=str(tmp_path / "serened.log"),
                               http=params)
        self.serened.start()
        self.http_port = self.serened.http_port
        self.pg = psycopg.connect(self.serened.dsn(), autocommit=True)

    def post(self, path: str, body: bytes, content_type: str) -> int:
        conn = http.client.HTTPConnection("127.0.0.1", self.http_port,
                                          timeout=60)
        try:
            conn.request("POST", path, body=body, headers={
                "Content-Type": content_type,
                "Authorization": "Basic cG9zdGdyZXM6",
            })
            response = conn.getresponse()
            response.read()
            return response.status
        finally:
            conn.close()

    def count(self, table: str) -> int:
        self.pg.execute(f"VACUUM (REFRESH_TABLE) {table}")
        return self.pg.execute(f"SELECT count(*) FROM {table}").fetchone()[0]

    def close(self) -> None:
        self.pg.close()
        stopped = self.serened.graceful_stop(timeout=60)
        self.serened.stop()
        assert stopped is not None, self.serened.log.tail(0)[-2000:]


def test_schema_param_places_the_tables(tmp_path: Path) -> None:
    server = _Server(tmp_path, "api=otel&schema=telemetry")
    try:
        body = (FIXTURES / "logs/basic.json").read_bytes()
        assert server.post("/v1/logs", body, "application/json") == 200
        assert server.count("telemetry.otel_logs") > 0
        missing = server.pg.execute(
            "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename = 'otel_logs'").fetchone()[0]
        assert missing == 0
    finally:
        server.close()


def _otel_objects(server: _Server, catalog: str) -> int:
    return server.pg.execute(
        f"SELECT count(*) FROM {catalog} WHERE schemaname = 'public' "
        "AND tablename LIKE 'otel\\_%'").fetchone()[0]


def _indexes_of(server: _Server, table: str) -> list[str]:
    return [row[0] for row in server.pg.execute(
        "SELECT indexname FROM pg_indexes WHERE tablename = %s "
        "ORDER BY indexname", (table,)).fetchall()]


def test_without_create_schema_exports_wait_for_the_tables(
        tmp_path: Path) -> None:
    server = _Server(tmp_path, "api=otel&create_schema=false")
    try:
        assert _otel_objects(server, "pg_tables") == 0
        assert "Exports answer 503" in server.serened.log.tail(0)
        logs = (FIXTURES / "logs/basic.json").read_bytes()
        metrics = (FIXTURES / "metrics/mixed_batch.json").read_bytes()
        assert server.post("/v1/logs", logs, "application/json") == 503
        assert server.post("/v1/metrics", metrics, "application/json") == 503

        sql = (FIXTURES.parent / "otel_schema.sql").read_text()
        for statement in sql.split(";\n"):
            if statement.strip():
                server.pg.execute(statement)

        assert server.post("/v1/logs", logs, "application/json") == 200
        assert server.post("/v1/metrics", metrics, "application/json") == 200
        assert server.count("otel_logs") > 0
        assert server.count("otel_metrics_gauge") > 0
    finally:
        server.close()


def test_without_create_schema_mistyped_column_answers_503(
        tmp_path: Path) -> None:
    _break_schema(
        tmp_path,
        'CREATE TABLE otel_logs ("timestamp" VARCHAR NOT NULL) '
        "WITH (storage = 'search')",
    )
    server = _Server(tmp_path, "api=otel&create_schema=false")
    try:
        assert "invalid OpenTelemetry schema" in server.serened.log.tail(0)
        body = (FIXTURES / "logs/basic.json").read_bytes()
        assert server.post("/v1/logs", body, "application/json") == 503
        assert _otel_objects(server, "pg_tables") == 1
    finally:
        server.close()


def test_without_create_schema_missing_database_refuses_startup(
        tmp_path: Path) -> None:
    result = _start_otel(tmp_path, "api=otel&db=telemetry&create_schema=false")
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "database 'telemetry' does not exist" in output, output


def test_missing_table_is_created_with_its_index(tmp_path: Path) -> None:
    server = _Server(tmp_path, "api=otel")
    try:
        assert _otel_objects(server, "pg_tables") == 7
        assert _otel_objects(server, "pg_indexes") == 7
        server.pg.execute("DROP INDEX otel_logs_idx")
        server.pg.execute("DROP TABLE otel_metrics_summary")
    finally:
        server.close()

    server = _Server(tmp_path, "api=otel")
    try:
        assert _otel_objects(server, "pg_tables") == 7
        assert _indexes_of(server, "otel_metrics_summary") == [
            "otel_metrics_summary_idx"]
        assert _indexes_of(server, "otel_logs") == []
    finally:
        server.close()


def test_own_index_survives_restart(tmp_path: Path) -> None:
    server = _Server(tmp_path, "api=otel")
    try:
        def export(bodies: list[str]) -> bytes:
            return json.dumps({"resourceLogs": [{
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": "svc"}}]},
                "scopeLogs": [{"logRecords": [{
                    "timeUnixNano": str(1_700_000_000_000_000_000 + i),
                    "body": {"stringValue": body},
                } for i, body in enumerate(bodies)]}],
            }]}).encode()

        assert server.post("/v1/logs", export([
            "upstream downstream_remote_disconnect",
            "connection reset by peer",
        ]), "application/json") == 200
        assert server.count("otel_logs") == 2

        server.pg.execute(
            "CREATE TEXT SEARCH DICTIONARY otel_words AS "
            "split_by_non_alpha(case := 'lower', break := 'alnum') "
            "WITH (frequency, position, norm)")
        server.pg.execute(
            "CREATE OR REPLACE INDEX otel_logs_idx ON otel_logs USING inverted "
            '("timestamp", body otel_words, service_name)')
    finally:
        server.close()

    server = _Server(tmp_path, "api=otel")
    try:
        assert _indexes_of(server, "otel_logs") == ["otel_logs_idx"]
        definition = server.pg.execute(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'otel_logs_idx'"
        ).fetchone()[0]
        assert "otel_words" in definition, definition
        assert server.post("/v1/logs", export([
            "local_disconnect", "remote peer closed",
        ]), "application/json") == 200
        assert server.count("otel_logs") == 4

        def matches(query: str) -> list[str]:
            return [row[0] for row in server.pg.execute(
                "SELECT body FROM otel_logs WHERE body @@ %s ORDER BY body",
                (query,)).fetchall()]

        assert matches("remote") == [
            "remote peer closed", "upstream downstream_remote_disconnect"]
        assert matches("disconnect") == [
            "local_disconnect", "upstream downstream_remote_disconnect"]
    finally:
        server.close()


OWN_LOGS_SCHEMA = (
    "CREATE TEXT SEARCH DICTIONARY otel_words AS "
    "split_by_non_alpha(case := 'lower', break := 'alnum') "
    "WITH (frequency, position, norm)",
    "CREATE TEXT SEARCH DICTIONARY otel_keyword AS keyword()",
    """CREATE TABLE otel_logs (
        "timestamp"         TIMESTAMP_NS NOT NULL,
        observed_timestamp  TIMESTAMP_NS,
        trace_id            VARCHAR,
        span_id             VARCHAR,
        trace_flags         INTEGER,
        severity_text       VARCHAR,
        severity_number     SMALLINT,
        service_name        VARCHAR,
        event_name          VARCHAR,
        body                VARCHAR USING COMPRESSION zstd,
        resource_schema_url VARCHAR,
        scope_schema_url    VARCHAR,
        scope_name          VARCHAR,
        scope_version       VARCHAR,
        resource_attributes JSON,
        scope_attributes    JSON,
        log_attributes      JSON
    ) WITH (storage = 'search')""",
    """CREATE INDEX otel_logs_idx ON otel_logs USING inverted (
        "timestamp", body otel_words, severity_text, service_name, trace_id,
        (log_attributes ->> 'error.type') otel_keyword
    )""",
)


def test_own_schema_with_its_own_tokenizer_and_expression_index(
        tmp_path: Path) -> None:
    def export(records: list[tuple[str, str | None]]) -> bytes:
        return json.dumps({"resourceLogs": [{"scopeLogs": [{"logRecords": [{
            "timeUnixNano": str(1_700_000_000_000_000_000 + i),
            "body": {"stringValue": body},
            "attributes": [] if error is None else [
                {"key": "error.type", "value": {"stringValue": error}}],
        } for i, (body, error) in enumerate(records)]}]}]}).encode()

    def bodies(server: _Server, where: str) -> list[str]:
        return [row[0] for row in server.pg.execute(
            f"SELECT body FROM otel_logs WHERE {where} ORDER BY body"
        ).fetchall()]

    server = _Server(tmp_path, "api=otel&create_schema=false")
    try:
        assert "Exports answer 503" in server.serened.log.tail(0)
        for statement in OWN_LOGS_SCHEMA:
            server.pg.execute(statement)
        sql = (FIXTURES.parent / "otel_schema.sql").read_text()
        for statement in sql.split(";\n"):
            if statement.strip():
                server.pg.execute(statement)
        assert _otel_objects(server, "pg_tables") == 7

        assert server.post("/v1/logs", export([
            ("upstream reset: downstream_remote_disconnect",
             "downstream_remote_disconnect"),
            ("remote peer closed the stream", "remote"),
            ("inventory lookup timed out", "timeout"),
            ("order placed", None),
        ]), "application/json") == 200
        assert server.count("otel_logs") == 4
    finally:
        server.close()

    restart = (tmp_path / "serened.log").stat().st_size
    server = _Server(tmp_path, "api=otel&create_schema=false")
    try:
        assert "Exports answer 503" not in server.serened.log.tail(restart)
        definition = server.pg.execute(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'otel_logs_idx'"
        ).fetchone()[0]
        assert "otel_words" in definition, definition
        assert "otel_keyword" in definition, definition
        assert server.post("/v1/logs", export([
            ("payment gateway timed out", "timeout"),
        ]), "application/json") == 200
        assert server.count("otel_logs") == 5

        assert bodies(server, "body @@ 'remote'") == [
            "remote peer closed the stream",
            "upstream reset: downstream_remote_disconnect"]
        assert bodies(server, "body @@ 'disconnect'") == [
            "upstream reset: downstream_remote_disconnect"]
        assert bodies(
            server,
            "body @@ phraseto_tsquery('downstream_remote_disconnect')") == [
            "upstream reset: downstream_remote_disconnect"]
        assert bodies(
            server,
            "(log_attributes ->> 'error.type') @@ "
            "'downstream_remote_disconnect'") == [
            "upstream reset: downstream_remote_disconnect"]
        assert bodies(
            server, "(log_attributes ->> 'error.type') @@ 'remote'") == [
            "remote peer closed the stream"]
        assert server.pg.execute(
            "SELECT log_attributes ->> 'error.type' AS error_type, count(*) "
            "FROM otel_logs WHERE log_attributes ->> 'error.type' IS NOT NULL "
            "GROUP BY ALL ORDER BY 2 DESC, 1").fetchall() == [
            ("timeout", 2), ("downstream_remote_disconnect", 1),
            ("remote", 1)]
        assert server.pg.execute(
            "SELECT DISTINCT compression FROM pragma_storage_info('otel_logs') "
            "WHERE column_name = 'body'").fetchall() == [("ZSTD",)]
    finally:
        server.close()


def test_single_worker_thread_does_not_deadlock(tmp_path: Path) -> None:
    server = _Server(tmp_path, "api=otel&api=es")
    try:
        server.pg.execute("SET GLOBAL threads = 1")
        assert str(server.pg.execute(
            "SELECT current_setting('threads')").fetchone()[0]) == "1"
        for fixture, path in (("logs/basic.json", "/v1/logs"),
                              ("traces/upstream.json", "/v1/traces"),
                              ("metrics/mixed_batch.json", "/v1/metrics")):
            body = (FIXTURES / fixture).read_bytes()
            assert server.post(path, body, "application/json") == 200, path
        assert server.count("otel_logs") > 0

        mapping = json.dumps({"mappings": {"properties": {
            "n": {"type": "integer"}}}}).encode()
        conn = http.client.HTTPConnection("127.0.0.1", server.http_port,
                                          timeout=60)
        conn.request("PUT", "/single", body=mapping, headers={
            "Content-Type": "application/json",
            "Authorization": "Basic cG9zdGdyZXM6"})
        assert conn.getresponse().status == 200
        conn.close()
        bulk = "".join('{"index":{}}\n{"n":%d}\n' % i for i in range(1000))
        assert server.post("/single/_bulk", bulk.encode(),
                           "application/x-ndjson") == 200
        assert server.count("es.single") == 1000
    finally:
        server.close()


def test_failed_metrics_export_writes_no_table(tmp_path: Path) -> None:
    server = _Server(tmp_path, "api=otel")
    try:
        server.pg.execute("DROP TABLE otel_metrics_summary")
        conn = http.client.HTTPConnection("127.0.0.1", server.http_port,
                                          timeout=60)
        headers = {"Content-Type": "application/json",
                   "Authorization": "Basic cG9zdGdyZXM6"}
        body = (FIXTURES / "metrics/mixed_batch.json").read_bytes()
        conn.request("POST", "/v1/metrics", body=body, headers=headers)
        response = conn.getresponse()
        response.read()
        assert response.status == 503
        for table in ("otel_metrics_gauge", "otel_metrics_sum",
                      "otel_metrics_histogram"):
            assert server.count(table) == 0, table

        body = (FIXTURES / "logs/basic.json").read_bytes()
        conn.request("POST", "/v1/logs", body=body, headers=headers)
        response = conn.getresponse()
        response.read()
        assert response.status == 200
        conn.close()
        assert server.count("otel_logs") > 0
    finally:
        server.close()


def test_metrics_export_inserts_only_its_kinds(tmp_path: Path) -> None:
    server = _Server(tmp_path, "api=otel")
    try:
        server.pg.execute("DROP TABLE otel_metrics_summary")
        export = json.loads((FIXTURES / "metrics/mixed_batch.json").read_text())
        for resource in export["resourceMetrics"]:
            for scope in resource["scopeMetrics"]:
                scope["metrics"] = [
                    metric for metric in scope["metrics"] if "gauge" in metric]
        conn = http.client.HTTPConnection("127.0.0.1", server.http_port,
                                          timeout=60)
        conn.request("POST", "/v1/metrics", body=json.dumps(export),
                     headers={"Content-Type": "application/json",
                              "Authorization": "Basic cG9zdGdyZXM6"})
        response = conn.getresponse()
        response.read()
        conn.close()
        assert response.status == 200
        assert server.count("otel_metrics_gauge") > 0
    finally:
        server.close()


def _logs_export(resources: int, scopes: int, records: int) -> bytes:
    return json.dumps({"resourceLogs": [{
        "resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": f"svc{r}"}}]},
        "scopeLogs": [{
            "scope": {"name": f"scope{s}"},
            "logRecords": [{
                "timeUnixNano": str(1_700_000_000_000_000_000 + i),
                "body": {"stringValue": f"svc{r}/scope{s}/{i}"},
            } for i in range(records)],
        } for s in range(scopes)],
    } for r in range(resources)]}).encode()


def _metrics_export(resources: int, metrics: int, points: int) -> bytes:
    def metric(r: int, m: int) -> dict:
        data = {"dataPoints": [{
            "timeUnixNano": str(1_700_000_000_000_000_000 + p),
            "asDouble": float(p),
        } for p in range(points)]}
        if m % 2:
            data["aggregationTemporality"] = 2
            return {"name": f"svc{r}/sum{m}", "sum": data}
        return {"name": f"svc{r}/gauge{m}", "gauge": data}

    return json.dumps({"resourceMetrics": [{
        "resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": f"svc{r}"}}]},
        "scopeMetrics": [{"scope": {"name": "scope"},
                          "metrics": [metric(r, m) for m in range(metrics)]}],
    } for r in range(resources)]}).encode()


def test_large_export_is_split_without_losing_rows(tmp_path: Path) -> None:
    server = _Server(tmp_path, "api=otel")
    try:
        body = _logs_export(resources=3, scopes=2, records=12_000)
        assert server.post("/v1/logs", body, "application/json") == 200
        assert server.count("otel_logs") == 72_000
        assert server.pg.execute(
            "SELECT count(DISTINCT body) FROM otel_logs").fetchone()[0] == 72_000
        assert server.pg.execute(
            "SELECT count(*) FROM otel_logs WHERE body NOT LIKE "
            "service_name || '/' || scope_name || '/%'").fetchone()[0] == 0

        body = _metrics_export(resources=2, metrics=40, points=1_000)
        assert server.post("/v1/metrics", body, "application/json") == 200
        for table in ("otel_metrics_gauge", "otel_metrics_sum"):
            assert server.count(table) == 40_000, table
            assert server.pg.execute(
                f"SELECT count(*) FROM {table} WHERE metric_name NOT LIKE "
                "service_name || '/%'").fetchone()[0] == 0, table
    finally:
        server.close()
