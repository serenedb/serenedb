"""A server directory on an object store.

The snapshot and the segments live in the bucket; the log and the temporary
files stay on a local disk. `LOCK` in the bucket names the log directory that
owns the server until a checkpointed stop removes it, so a server starts from
another log directory only after such a stop. The server signs its requests
with the credentials of the AWS default chain, which it fetches again before
they expire.
"""

from __future__ import annotations

import datetime
import http.server
import json
import os
import subprocess
import threading
import time
from pathlib import Path

import psycopg
import pytest
from fake_s3 import FakeS3
from serened import reserve_port  # type: ignore[import-untyped]

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


def _environment(fake: FakeS3, extra: dict[str, str]) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items()
           if not key.startswith("AWS_") and not key.startswith("DUCKDB_S3_")}
    env.update({
        "AWS_REGION": "us-east-1",
        "AWS_EC2_METADATA_DISABLED": "true",
        "DUCKDB_S3_ENDPOINT": fake.endpoint,
        "DUCKDB_S3_USE_SSL": "false",
        "DUCKDB_S3_URL_STYLE": "path",
    })
    env.update(extra)
    return env


class _Server:
    def __init__(self, url: str, node: Path, env: dict[str, str],
                 *args: str) -> None:
        self.reserved = reserve_port()
        self.port = self.reserved.getsockname()[1]
        node.mkdir(parents=True, exist_ok=True)
        self.log = node / f"serened-{self.port}.log"
        with self.log.open("w") as out:
            self.proc = subprocess.Popen(
                [SERENED_BIN, url, f"--log_directory={node / 'logs'}",
                 f"--temp_directory={node / 'temp'}",
                 f"--listen=postgres://127.0.0.1:{self.port}", *args],
                stdout=out, stderr=subprocess.STDOUT, env=env)

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(host="127.0.0.1", port=self.port,
                               user="postgres", dbname="postgres",
                               autocommit=True)

    def wait_ready(self, timeout: float = 60.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                return False
            try:
                self.connect().close()
                return True
            except psycopg.OperationalError:
                time.sleep(0.2)
        return False

    def stop(self) -> int:
        if self.proc.poll() is None:
            self.proc.terminate()
        code = self.proc.wait(timeout=60)
        self.reserved.close()
        return code

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=60)
        self.reserved.close()

    def output(self) -> str:
        return self.log.read_text()


def _count(server: _Server) -> int:
    with server.connect() as conn:
        return conn.execute("SELECT count(*) FROM t").fetchone()[0]


def test_a_server_moves_between_log_directories_after_a_checkpointed_stop(
        tmp_path: Path) -> None:
    with FakeS3() as fake:
        prefix = f"moves-{tmp_path.name}"
        url = f"s3://bucket/{prefix}"
        env = _environment(fake, {"AWS_ACCESS_KEY_ID": "key",
                                  "AWS_SECRET_ACCESS_KEY": "secret"})

        first = _Server(url, tmp_path / "a", env, "--checkpoint_at_stop")
        try:
            assert first.wait_ready(), first.output()
            assert f"{prefix}/LOCK" in fake.keys("bucket", prefix)
            with first.connect() as conn:
                conn.execute("CREATE TABLE t (a INT)")
                conn.execute("INSERT INTO t SELECT i FROM range(1000) r(i)")
                conn.execute("CHECKPOINT")
                conn.execute("INSERT INTO t VALUES (-1)")
        finally:
            assert first.stop() == 0, first.output()
        assert f"{prefix}/LOCK" not in fake.keys("bucket", prefix)
        assert sorted(p.name for p in (tmp_path / "a" / "logs").iterdir()) == ["LOCK"]

        second = _Server(url, tmp_path / "b", env, "--require_existing")
        try:
            assert second.wait_ready(), second.output()
            assert _count(second) == 1001
            with second.connect() as conn:
                conn.execute("INSERT INTO t VALUES (-2)")
        finally:
            second.kill()
        assert f"{prefix}/LOCK" in fake.keys("bucket", prefix)

        refused = _Server(url, tmp_path / "a", env, "--require_existing")
        try:
            assert not refused.wait_ready(timeout=30)
            assert refused.proc.wait(timeout=30) != 0
            assert "belongs to the log directory" in refused.output()
        finally:
            refused.kill()

        again = _Server(url, tmp_path / "b", env, "--require_existing")
        try:
            assert again.wait_ready(), again.output()
            assert _count(again) == 1002
        finally:
            assert again.stop() == 0, again.output()


def test_an_object_store_server_directory_needs_local_directories(
        tmp_path: Path) -> None:
    result = subprocess.run(
        [SERENED_BIN, "s3://bucket/nowhere", "--listen=postgres://127.0.0.1:1"],
        capture_output=True, text=True, timeout=60)
    assert result.returncode != 0, result
    assert "--log_directory has to name a local directory" in \
        result.stdout + result.stderr, result


class _Credentials(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        owner = self.server.owner
        with owner.lock:
            owner.authorizations.append(self.headers.get("Authorization"))
            body = json.dumps(owner.answer).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


class _CredentialsEndpoint:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.authorizations: list[str | None] = []
        self.answer: dict[str, str] = {}
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0),
                                                      _Credentials)
        self.server.owner = self
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}/creds"

    def serve(self, token: str, lifetime: datetime.timedelta) -> None:
        expiration = datetime.datetime.now(datetime.UTC) + lifetime
        with self.lock:
            self.answer = {
                "AccessKeyId": f"key-{token}",
                "SecretAccessKey": f"secret-{token}",
                "Token": token,
                "Expiration": expiration.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def test_container_credentials_are_fetched_again_before_they_expire(
        tmp_path: Path) -> None:
    endpoint = _CredentialsEndpoint()
    token_file = tmp_path / "token"
    token_file.write_text("pod-token\n")
    endpoint.serve("first", datetime.timedelta(minutes=5, seconds=4))
    try:
        with FakeS3() as fake:
            fake.session_tokens = {"first"}
            env = _environment(fake, {
                "AWS_CONTAINER_CREDENTIALS_FULL_URI": endpoint.url,
                "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE": str(token_file),
            })
            url = f"s3://bucket/creds-{tmp_path.name}"
            server = _Server(url, tmp_path / "a", env)
            try:
                assert server.wait_ready(), server.output()
                with server.connect() as conn:
                    conn.execute("CREATE TABLE t (a INT)")
                    conn.execute("INSERT INTO t VALUES (1)")
                    conn.execute("CHECKPOINT")
                fetched = len(endpoint.authorizations)
                endpoint.serve("second", datetime.timedelta(hours=1))
                fake.session_tokens = {"second"}
                time.sleep(5)
                with server.connect() as conn:
                    conn.execute("INSERT INTO t VALUES (2)")
                    conn.execute("CHECKPOINT")
                assert len(endpoint.authorizations) == fetched + 1
                assert _count(server) == 2
            finally:
                assert server.stop() == 0, server.output()
    finally:
        endpoint.close()
    assert set(endpoint.authorizations) == {"pod-token"}
