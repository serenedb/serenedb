from __future__ import annotations

import base64
import http.client
import os
import socket

import psycopg
import pytest
from spec_loader import conn_kwargs

HOST = os.environ.get("SDB_DRV_HOST", "localhost")
PORT = int(os.environ.get("SDB_DRV_HTTP_PORT", "9200"))
USER = os.environ.get("SDB_DRV_USER", "postgres")
PASSWORD = os.environ.get("SDB_DRV_PASSWORD", "")
TOKEN = os.environ.get("SDB_DRV_HTTP_TOKEN", "")

SUPERUSER_AUTH = (
    f"Bearer {TOKEN}"
    if TOKEN
    else "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
)
SUPERUSER_NAME = "postgres" if TOKEN else USER

ROLE = "drv_http_session_role"
ROLE_PASSWORD = "drv-http-session-pw"
ROLE_AUTH = "Basic " + base64.b64encode(f"{ROLE}:{ROLE_PASSWORD}".encode()).decode()


def _reachable() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=2):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _reachable(), reason=f"no HTTP endpoint at {HOST}:{PORT}"
)


def _drop_role(conn: psycopg.Connection, database: str) -> None:
    exists = conn.execute(
        "SELECT count(*) FROM pg_roles WHERE rolname = %s", (ROLE,)
    ).fetchone()[0]
    if exists:
        conn.execute(f'REVOKE CONNECT ON DATABASE "{database}" FROM {ROLE}')
        conn.execute(f"DROP ROLE {ROLE}")


@pytest.fixture()
def role():
    database = conn_kwargs()["dbname"]
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        _drop_role(conn, database)
        conn.execute(f"CREATE ROLE {ROLE} LOGIN PASSWORD '{ROLE_PASSWORD}'")
        conn.execute(f'GRANT CONNECT ON DATABASE "{database}" TO {ROLE}')
    yield ROLE
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        _drop_role(conn, database)


def session_user(conn: http.client.HTTPConnection, auth: str) -> tuple[int, str]:
    conn.request("GET", "/_test/session_user", headers={"Authorization": auth})
    response = conn.getresponse()
    body = response.read().decode()
    return response.status, body


def test_each_request_runs_as_the_role_it_authenticated_as(role):
    conn = http.client.HTTPConnection(HOST, PORT, timeout=30)
    try:
        answers = []
        sockets = []
        for auth in (SUPERUSER_AUTH, ROLE_AUTH, SUPERUSER_AUTH, ROLE_AUTH, ROLE_AUTH):
            answers.append(session_user(conn, auth))
            sockets.append(conn.sock)
        assert answers == [
            (200, SUPERUSER_NAME),
            (200, role),
            (200, SUPERUSER_NAME),
            (200, role),
            (200, role),
        ]
        assert all(s is sockets[0] for s in sockets)
    finally:
        conn.close()


def test_a_rejected_request_does_not_change_the_session_role(role):
    conn = http.client.HTTPConnection(HOST, PORT, timeout=30)
    try:
        assert session_user(conn, ROLE_AUTH) == (200, role)
        wrong = "Basic " + base64.b64encode(f"{ROLE}:wrong".encode()).decode()
        status, _ = session_user(conn, wrong)
        assert status == 401
        assert session_user(conn, SUPERUSER_AUTH) == (200, SUPERUSER_NAME)
        assert session_user(conn, ROLE_AUTH) == (200, role)
    finally:
        conn.close()
