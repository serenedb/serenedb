from __future__ import annotations

import base64
import hashlib
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


@pytest.fixture()
def http_conn():
    conn = http.client.HTTPConnection(HOST, PORT, timeout=30)
    yield conn
    conn.close()


def session_user(conn: http.client.HTTPConnection, auth: str) -> tuple[int, str]:
    conn.request("GET", "/_test/session_user", headers={"Authorization": auth})
    response = conn.getresponse()
    body = response.read().decode()
    return response.status, body


def test_each_request_runs_as_the_role_it_authenticated_as(role, http_conn):
    answers = []
    sockets = []
    for auth in (SUPERUSER_AUTH, ROLE_AUTH, SUPERUSER_AUTH, ROLE_AUTH, ROLE_AUTH):
        answers.append(session_user(http_conn, auth))
        sockets.append(http_conn.sock)
    assert answers == [
        (200, SUPERUSER_NAME),
        (200, role),
        (200, SUPERUSER_NAME),
        (200, role),
        (200, role),
    ]
    assert all(s is sockets[0] for s in sockets)


def test_is_superuser_follows_the_role_a_request_runs_as(role, http_conn):
    answers = []
    for auth in (SUPERUSER_AUTH, ROLE_AUTH, SUPERUSER_AUTH):
        http_conn.request("GET", "/_test/is_superuser", headers={"Authorization": auth})
        response = http_conn.getresponse()
        answers.append((response.status, response.read().decode()))
    assert answers == [(200, "on"), (200, "off"), (200, "on")]


@pytest.mark.skipif(not TOKEN, reason="the server has no bearer token")
def test_the_bearer_token_is_masked_in_settings():
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        setting = conn.execute(
            "SELECT setting FROM sdb_flags WHERE name = 'auth_bearer_token'"
        ).fetchone()[0]
    assert setting == "***"


def test_a_rejected_request_does_not_change_the_session_role(role, http_conn):
    wrong = "Basic " + base64.b64encode(f"{ROLE}:wrong".encode()).decode()
    answers = []
    for auth in (ROLE_AUTH, wrong, SUPERUSER_AUTH, ROLE_AUTH):
        status, body = session_user(http_conn, auth)
        answers.append((status, body) if status == 200 else (status,))
    assert answers == [
        (200, role),
        (401,),
        (200, SUPERUSER_NAME),
        (200, role),
    ]


PASSWORDLESS = "drv_http_session_nopw"


@pytest.fixture()
def passwordless_role():
    database = conn_kwargs()["dbname"]
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        conn.execute(f"DROP ROLE IF EXISTS {PASSWORDLESS}")
        conn.execute(f"CREATE ROLE {PASSWORDLESS} LOGIN")
        conn.execute(f'GRANT CONNECT ON DATABASE "{database}" TO {PASSWORDLESS}')
    yield PASSWORDLESS
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        conn.execute(f'REVOKE CONNECT ON DATABASE "{database}" FROM {PASSWORDLESS}')
        conn.execute(f"DROP ROLE {PASSWORDLESS}")


EXPIRED = "drv_http_session_expired"
EXPIRED_PASSWORD = "drv-http-session-expired-pw"


@pytest.fixture()
def expired_role():
    database = conn_kwargs()["dbname"]
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        conn.execute(f"DROP ROLE IF EXISTS {EXPIRED}")
        conn.execute(
            f"CREATE ROLE {EXPIRED} LOGIN PASSWORD '{EXPIRED_PASSWORD}' "
            "VALID UNTIL '2000-01-01 00:00:00+00'"
        )
        conn.execute(f'GRANT CONNECT ON DATABASE "{database}" TO {EXPIRED}')
    yield EXPIRED
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        conn.execute(f'REVOKE CONNECT ON DATABASE "{database}" FROM {EXPIRED}')
        conn.execute(f"DROP ROLE {EXPIRED}")


def test_a_password_past_its_valid_until_is_refused(expired_role, http_conn):
    auth = "Basic " + base64.b64encode(
        f"{expired_role}:{EXPIRED_PASSWORD}".encode()
    ).decode()
    assert session_user(http_conn, auth)[0] == 401
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        conn.execute(f"ALTER ROLE {expired_role} VALID UNTIL 'infinity'")
    assert session_user(http_conn, auth) == (200, expired_role)


def test_a_passwordless_role_is_not_trusted(passwordless_role, http_conn):
    for password in ("", "anything"):
        auth = "Basic " + base64.b64encode(
            f"{passwordless_role}:{password}".encode()
        ).decode()
        status, _ = session_user(http_conn, auth)
        assert status == 401
    empty_user = "Basic " + base64.b64encode(b":").decode()
    status, _ = session_user(http_conn, empty_user)
    assert status == 401


MD5_ROLE = "drv_http_session_md5"
MD5_PASSWORD = "drv-http-session-md5-pw"


@pytest.fixture()
def md5_role():
    database = conn_kwargs()["dbname"]
    stored = "md5" + hashlib.md5(f"{MD5_PASSWORD}{MD5_ROLE}".encode()).hexdigest()
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        conn.execute(f"DROP ROLE IF EXISTS {MD5_ROLE}")
        conn.execute(f"CREATE ROLE {MD5_ROLE} LOGIN PASSWORD '{stored}'")
        conn.execute(f'GRANT CONNECT ON DATABASE "{database}" TO {MD5_ROLE}')
    yield MD5_ROLE
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        conn.execute(f'REVOKE CONNECT ON DATABASE "{database}" FROM {MD5_ROLE}')
        conn.execute(f"DROP ROLE {MD5_ROLE}")


def test_an_md5_stored_password_authenticates(md5_role, http_conn):
    for password, want in ((MD5_PASSWORD, (200, md5_role)), ("wrong", (401,))):
        auth = "Basic " + base64.b64encode(
            f"{md5_role}:{password}".encode()
        ).decode()
        status, body = session_user(http_conn, auth)
        assert ((status, body) if status == 200 else (status,)) == want


LIMITED = "drv_http_session_limited"
LIMITED_PASSWORD = "drv-http-session-limited-pw"


@pytest.fixture()
def limited_role():
    database = conn_kwargs()["dbname"]
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        conn.execute(f"DROP ROLE IF EXISTS {LIMITED}")
        conn.execute(
            f"CREATE ROLE {LIMITED} LOGIN PASSWORD '{LIMITED_PASSWORD}' "
            "CONNECTION LIMIT 1"
        )
        conn.execute(f'GRANT CONNECT ON DATABASE "{database}" TO {LIMITED}')
    yield LIMITED
    with psycopg.connect(**conn_kwargs(), autocommit=True) as conn:
        conn.execute(f'REVOKE CONNECT ON DATABASE "{database}" FROM {LIMITED}')
        conn.execute(f"DROP ROLE {LIMITED}")


def test_http_sessions_count_toward_the_connection_limit(limited_role):
    auth = "Basic " + base64.b64encode(
        f"{limited_role}:{LIMITED_PASSWORD}".encode()
    ).decode()
    first = http.client.HTTPConnection(HOST, PORT, timeout=30)
    second = http.client.HTTPConnection(HOST, PORT, timeout=30)
    try:
        assert session_user(first, auth) == (200, limited_role)
        assert session_user(second, auth)[0] != 200
    finally:
        first.close()
        second.close()
