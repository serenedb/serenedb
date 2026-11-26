"""A Basic login to the HTTP APIs is decided like a psql login: by the client
authentication rules for the peer, the role and the database, against the
role's stored password, its LOGIN and its VALID UNTIL."""

from __future__ import annotations

import base64
import hashlib
import http.client
import os
from pathlib import Path

import psycopg
import pytest
from serened import Serened  # type: ignore[import-untyped]

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)

PASSWORD = "drv-login-pw"


def _md5(role: str) -> str:
    return "md5" + hashlib.md5((PASSWORD + role).encode()).hexdigest()


ROLES = {
    "drv_login_scram": f"LOGIN PASSWORD '{PASSWORD}'",
    "drv_login_md5": f"LOGIN PASSWORD '{_md5('drv_login_md5')}'",
    "drv_login_md5_rule": f"LOGIN PASSWORD '{PASSWORD}'",
    "drv_login_md5_secret": f"LOGIN PASSWORD '{_md5('drv_login_md5_secret')}'",
    "drv_login_password": f"LOGIN PASSWORD '{PASSWORD}'",
    "drv_login_reject": f"LOGIN PASSWORD '{PASSWORD}'",
    "drv_login_trust": "LOGIN",
    "drv_login_nopassword": "LOGIN",
    "drv_login_expired": f"LOGIN PASSWORD '{PASSWORD}' VALID UNTIL '2000-01-01'",
    "drv_login_expired_trust": f"LOGIN PASSWORD '{PASSWORD}' VALID UNTIL '2000-01-01'",
    "drv_login_nologin": f"NOLOGIN PASSWORD '{PASSWORD}'",
    "drv_login_ldap": f"LOGIN PASSWORD '{PASSWORD}'",
    "drv_login_unlisted": f"LOGIN PASSWORD '{PASSWORD}'",
}

RULES = """
host all drv_login_scram 127.0.0.1/32 scram-sha-256
host all drv_login_md5 127.0.0.1/32 md5
host all drv_login_md5_rule 127.0.0.1/32 md5
host all drv_login_md5_secret 127.0.0.1/32 scram-sha-256
host all drv_login_password 127.0.0.1/32 password
host all drv_login_reject 127.0.0.1/32 reject
host all drv_login_trust 127.0.0.1/32 trust
host all drv_login_nopassword 127.0.0.1/32 scram-sha-256
host all drv_login_missing 127.0.0.1/32 trust
host all drv_login_expired 127.0.0.1/32 scram-sha-256
host all drv_login_expired_trust 127.0.0.1/32 trust
host all drv_login_nologin 127.0.0.1/32 trust
host all drv_login_ldap 127.0.0.1/32 ldap ldapserver=nope
"""


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    root = tmp_path_factory.mktemp("http_login")
    serened = Serened(SERENED_BIN, datadir_root=str(root),
                      log_path=str(root / "serened.log"), http="api=test")
    serened.start()
    try:
        with psycopg.connect(serened.dsn(), autocommit=True) as conn:
            for role, options in ROLES.items():
                conn.execute(f"CREATE ROLE {role} {options}")
                conn.execute(f"GRANT CONNECT ON DATABASE postgres TO {role}")
            conn.execute(f"SET hba = $rules${RULES}$rules$")
        yield serened
    finally:
        serened.stop()


def _session_user_as(server: Serened, authorization: str) -> tuple[int, str]:
    conn = http.client.HTTPConnection("127.0.0.1", server.http_port, timeout=30)
    try:
        conn.request("GET", "/_test/session_user",
                     headers={"Authorization": authorization})
        response = conn.getresponse()
        return response.status, response.read().decode()
    finally:
        conn.close()


def _session_user(server: Serened, role: str, password: str) -> tuple[int, str]:
    credentials = base64.b64encode(f"{role}:{password}".encode()).decode()
    return _session_user_as(server, f"Basic {credentials}")


def _psql_login(server: Serened, role: str, password: str) -> bool:
    try:
        psycopg.connect(host="127.0.0.1", port=server.port, dbname="postgres",
                        user=role, password=password, connect_timeout=10,
                        sslmode="disable").close()
    except psycopg.OperationalError:
        return False
    return True


@pytest.mark.parametrize("role, password, allowed", [
    ("drv_login_scram", PASSWORD, True),
    ("drv_login_scram", "wrong", False),
    ("drv_login_md5", PASSWORD, True),
    ("drv_login_md5", "wrong", False),
    ("drv_login_md5_rule", PASSWORD, True),
    ("drv_login_md5_secret", PASSWORD, False),
    ("drv_login_reject", PASSWORD, False),
    ("drv_login_trust", "anything", True),
    ("drv_login_nopassword", "", False),
    ("drv_login_missing", PASSWORD, False),
    ("drv_login_expired", PASSWORD, False),
    ("drv_login_expired_trust", PASSWORD, True),
    ("drv_login_nologin", PASSWORD, False),
    ("drv_login_ldap", PASSWORD, False),
    ("drv_login_unlisted", PASSWORD, False),
    ("postgres", "", True),
])
def test_a_basic_login_is_decided_as_a_psql_login(server, role, password,
                                                  allowed):
    assert _psql_login(server, role, password) == allowed
    expected = (200, role) if allowed else (401, '{"error":"unauthorized"}')
    assert _session_user(server, role, password) == expected


def test_a_password_rule_checks_the_password(server):
    assert _session_user(server, "drv_login_password", PASSWORD) == (
        200, "drv_login_password")
    assert _session_user(server, "drv_login_password", "wrong")[0] == 401


@pytest.mark.parametrize("authorization", [
    "",
    "Digest drv_login_scram",
    "Basic !!!",
    "Basic " + base64.b64encode(b"drv_login_scram").decode(),
    "Basic " + base64.b64encode(f":{PASSWORD}".encode()).decode(),
])
def test_a_malformed_authorization_is_refused(server, authorization):
    assert _session_user_as(server, authorization) == (
        401, '{"error":"unauthorized"}')


def test_an_api_without_logins_answers_anyone_and_runs_no_sql(tmp_path):
    serened = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                      log_path=str(tmp_path / "serened.log"),
                      http=("api=test-open", "api=test"))
    serened.start()
    try:
        open_port, test_port = serened.http_ports

        def get(port: int, path: str) -> tuple[int, str]:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
            try:
                conn.request("GET", path)
                response = conn.getresponse()
                return response.status, response.read().decode()
            finally:
                conn.close()

        assert get(open_port, "/_test/ping")[0] == 200
        assert get(test_port, "/_test/ping")[0] == 401
        status, body = get(open_port, "/_test/session_user")
        assert status == 500 and "postgres" not in body, body
    finally:
        serened.stop()
