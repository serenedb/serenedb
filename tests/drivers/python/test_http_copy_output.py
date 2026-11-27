from __future__ import annotations

import base64
import http.client
import os
import socket
import struct
import time

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
def http_conn():
    conn = http.client.HTTPConnection(HOST, PORT, timeout=30)
    yield conn
    conn.close()


def copy(conn: http.client.HTTPConnection, sql: str) -> tuple[int, bytes]:
    conn.request("POST", "/test/copy", body=sql.encode(),
                 headers={"Authorization": AUTH, "Content-Type": "text/plain"})
    response = conn.getresponse()
    return response.status, response.read()


def test_every_format_writes_into_the_output(http_conn):
    assert copy(http_conn, "COPY (SELECT 1 AS a, 'x,y' AS b) TO STDOUT "
                           "(FORMAT csv, HEADER)") == (200, b'a,b\n1,"x,y"\n')
    assert copy(http_conn, "COPY (SELECT 1 AS a, 'x' AS b) TO STDOUT "
                           "(FORMAT json)") == (200, b'{"a":1,"b":"x"}\n')
    assert copy(http_conn, "COPY (SELECT 1 AS a, NULL::TEXT AS b) TO STDOUT"
                ) == (200, b"1\t\\N\n")
    status, binary = copy(http_conn, "COPY (SELECT 1 AS a) TO STDOUT "
                                     "(FORMAT binary)")
    assert status == 200
    assert binary.startswith(b"PGCOPY\n\xff\r\n\x00")
    assert binary.endswith(b"\xff\xff")
    status, parquet = copy(http_conn, "COPY (SELECT 1 AS a) TO STDOUT "
                                      "(FORMAT parquet)")
    assert status == 200
    assert parquet.startswith(b"PAR1") and parquet.endswith(b"PAR1")


def test_every_request_runs_in_a_session_of_its_own(http_conn):
    sockets = []
    assert copy(http_conn, "SET application_name = 'drv_http_copy_leak'") == (
        200, b"")
    sockets.append(http_conn.sock)
    assert copy(http_conn, "COPY (SELECT current_setting('application_name')) "
                           "TO STDOUT") != (200, b"drv_http_copy_leak\n")
    sockets.append(http_conn.sock)
    assert copy(http_conn, "CREATE TEMPORARY TABLE drv_http_copy_temp AS "
                           "SELECT 1 AS a") == (200, b"")
    sockets.append(http_conn.sock)
    status, error = copy(http_conn, "COPY drv_http_copy_temp TO STDOUT")
    sockets.append(http_conn.sock)
    assert status == 400
    assert b"drv_http_copy_temp" in error
    assert all(s is sockets[0] for s in sockets)


def _running(conn: psycopg.Connection, marker: str) -> int:
    return conn.execute(
        "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' "
        "AND query LIKE %s AND pid <> pg_backend_pid()",
        (f"%{marker}%",)).fetchone()[0]


@pytest.mark.parametrize("leave", ["close", "half-close", "reset"])
@pytest.mark.parametrize("query", [
    "SELECT pg_sleep(600) AS {marker}",
    "SELECT sum(i) AS {marker} FROM range(9000000000000000000) t(i)",
])
def test_a_client_that_goes_away_cancels_its_query(leave, query):
    marker = f"drv_http_copy_cancel_{leave.replace('-', '_')}"
    sql = f"COPY ({query.format(marker=marker)}) TO STDOUT".encode()
    request = (f"POST /test/copy HTTP/1.1\r\nHost: {HOST}\r\n"
               f"Authorization: {AUTH}\r\n"
               f"Content-Length: {len(sql)}\r\n\r\n").encode() + sql
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        client = socket.create_connection((HOST, PORT), timeout=30)
        try:
            client.sendall(request)
            deadline = time.monotonic() + 30
            while _running(conn, marker) == 0:
                assert time.monotonic() < deadline, "the query never started"
                time.sleep(0.05)
            left = time.monotonic()
            if leave == "half-close":
                client.shutdown(socket.SHUT_WR)
            elif leave == "reset":
                client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                  struct.pack("ii", 1, 0))
                client.close()
            else:
                client.close()
            while _running(conn, marker) != 0:
                assert time.monotonic() - left < 10, \
                    "the query outlived its client"
                time.sleep(0.01)
        finally:
            client.close()
