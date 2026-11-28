"""A server directory on an object store.

The snapshot and the segments live in the bucket; the WAL and the temporary
files stay on a local disk. `OWNER` in the bucket names the WAL directory that
owns the server until a handover stop removes it, so a server starts from
another WAL directory only after such a stop. The WAL and temporary
directories keep an `OWNER` of their own, locked while the server runs; the
copies of evicted segment blocks stay in the temporary directory for the next
start of the same server, and its spill files go at every start. The
server signs its requests with the credentials of the AWS default chain, which
it fetches again before they expire. A backup copies its snapshot inside the
store, and a copy of the prefix with its label restores in another prefix.
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
                [SERENED_BIN, url, f"--wal_directory={node / 'wal'}",
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


def _wal_owner(node: Path) -> tuple[bytes, bytes]:
    owner = (node / "wal" / "OWNER").read_bytes()
    assert len(owner) == 32, owner
    return owner[:16], owner[16:]


def test_a_server_moves_between_wal_directories_after_a_handover_stop(
        tmp_path: Path) -> None:
    with FakeS3() as fake:
        prefix = f"moves-{tmp_path.name}"
        url = f"s3://bucket/{prefix}"
        env = _environment(fake, {"AWS_ACCESS_KEY_ID": "key",
                                  "AWS_SECRET_ACCESS_KEY": "secret"})

        first = _Server(url, tmp_path / "a", env)
        try:
            assert first.wait_ready(), first.output()
            _, first_wal = _wal_owner(tmp_path / "a")
            assert fake.objects[("bucket", f"{prefix}/OWNER")].data == first_wal
            with first.connect() as conn:
                conn.execute("CREATE TABLE t (a INT)")
                conn.execute("INSERT INTO t SELECT i FROM range(1000) r(i)")
                conn.execute("CHECKPOINT")
                conn.execute("INSERT INTO t VALUES (-1)")
                conn.execute("SET sdb_faults = 'handover_at_stop'")
        finally:
            assert first.stop() == 0, first.output()
        assert f"{prefix}/OWNER" not in fake.keys("bucket", prefix)
        assert sorted(p.name for p in (tmp_path / "a" / "wal").iterdir()) == ["OWNER"]

        second = _Server(url, tmp_path / "b", env, "--require_existing")
        try:
            assert second.wait_ready(), second.output()
            assert _count(second) == 1001
            with second.connect() as conn:
                conn.execute("INSERT INTO t VALUES (-2)")
        finally:
            second.kill()
        first_server, _ = _wal_owner(tmp_path / "a")
        second_server, second_wal = _wal_owner(tmp_path / "b")
        assert second_server == first_server
        assert fake.objects[("bucket", f"{prefix}/OWNER")].data == second_wal

        refused = _Server(url, tmp_path / "a", env, "--require_existing")
        try:
            assert not refused.wait_ready(timeout=30)
            assert refused.proc.wait(timeout=30) != 0
            assert "belongs to the WAL directory" in refused.output()
        finally:
            refused.kill()

        again = _Server(url, tmp_path / "b", env, "--require_existing")
        try:
            assert again.wait_ready(), again.output()
            assert _count(again) == 1002
        finally:
            assert again.stop() == 0, again.output()


def test_a_lost_wal_directory_is_abandoned_by_name(tmp_path: Path) -> None:
    with FakeS3() as fake:
        url = f"s3://bucket/lost-{tmp_path.name}"
        env = _environment(fake, {"AWS_ACCESS_KEY_ID": "key",
                                  "AWS_SECRET_ACCESS_KEY": "secret"})

        lost = _Server(url, tmp_path / "a", env)
        try:
            assert lost.wait_ready(), lost.output()
            with lost.connect() as conn:
                conn.execute("CREATE TABLE t (a INT)")
                conn.execute("INSERT INTO t VALUES (1)")
                conn.execute("CHECKPOINT")
                conn.execute("INSERT INTO t VALUES (2)")
        finally:
            lost.kill()
        lost_id = _wal_owner(tmp_path / "a")[1].hex()

        wrong = _Server(url, tmp_path / "b", env, "--abandon_wal=0123")
        try:
            assert not wrong.wait_ready(timeout=30)
            assert wrong.proc.wait(timeout=30) != 0
            assert f"--abandon_wal={lost_id}" in wrong.output()
        finally:
            wrong.kill()

        taken = _Server(url, tmp_path / "b", env, f"--abandon_wal={lost_id}")
        try:
            assert taken.wait_ready(), taken.output()
            assert _count(taken) == 1
            with taken.connect() as conn:
                conn.execute("INSERT INTO t VALUES (3)")
        finally:
            taken.kill()
        assert fake.objects[("bucket", f"lost-{tmp_path.name}/OWNER")].data == \
            _wal_owner(tmp_path / "b")[1]

        again = _Server(url, tmp_path / "b", env)
        try:
            assert again.wait_ready(), again.output()
            assert _count(again) == 2
        finally:
            assert again.stop() == 0, again.output()


def test_a_wal_directory_of_another_server_is_refused(tmp_path: Path) -> None:
    with FakeS3() as fake:
        env = _environment(fake, {"AWS_ACCESS_KEY_ID": "key",
                                  "AWS_SECRET_ACCESS_KEY": "secret"})
        first = _Server(f"s3://bucket/first-{tmp_path.name}", tmp_path / "a",
                        env)
        try:
            assert first.wait_ready(), first.output()
            with first.connect() as conn:
                conn.execute("CREATE TABLE t (a INT)")
                conn.execute("INSERT INTO t VALUES (1)")
        finally:
            first.kill()
        wal = tmp_path / "a" / "wal"
        before = {p.name: p.read_bytes() for p in wal.iterdir()}

        other = _Server(f"s3://bucket/other-{tmp_path.name}", tmp_path / "a",
                        env)
        try:
            assert not other.wait_ready(timeout=30)
            assert other.proc.wait(timeout=30) != 0
            assert f"the WAL directory '{wal}' belongs to the server " \
                f"{_wal_owner(tmp_path / 'a')[0].hex()}" in other.output()
        finally:
            other.kill()
        assert {p.name: p.read_bytes() for p in wal.iterdir()} == before


def test_a_temporary_directory_keeps_the_copies_of_its_server_only(
        tmp_path: Path) -> None:
    with FakeS3() as fake:
        env = _environment(fake, {"AWS_ACCESS_KEY_ID": "key",
                                  "AWS_SECRET_ACCESS_KEY": "secret"})
        url = f"s3://bucket/temp-{tmp_path.name}"
        temp = tmp_path / "a" / "temp"
        server = _Server(url, tmp_path / "a", env)
        try:
            assert server.wait_ready(), server.output()
            owner = (temp / "OWNER").read_bytes()
            assert owner == _wal_owner(tmp_path / "a")[0]
            busy = _Server(f"s3://bucket/busy-{tmp_path.name}",
                           tmp_path / "b", env, f"--temp_directory={temp}")
            try:
                assert not busy.wait_ready(timeout=30)
                assert busy.proc.wait(timeout=30) != 0
                assert str(temp) in busy.output()
                assert "is in use by another running server" in busy.output()
            finally:
                busy.kill()
        finally:
            assert server.stop() == 0, server.output()
        copy = temp / "external_file_cache" / ("ab" * 16)
        copy.parent.mkdir(exist_ok=True)
        copy.write_bytes(b"\0" * 64)
        spill = temp / f"sdb_temp_{os.getpid()}_0_storage_x.tmp"
        spill.write_bytes(b"spilled")
        (temp / "notes").write_text("mine")

        again = _Server(url, tmp_path / "a", env)
        try:
            assert again.wait_ready(), again.output()
        finally:
            assert again.stop() == 0, again.output()
        assert not spill.exists()
        assert copy.read_bytes() == b"\0" * 64
        assert (temp / "notes").read_text() == "mine"

        spill.write_bytes(b"spilled")
        other = _Server(f"s3://bucket/other-{tmp_path.name}", tmp_path / "c",
                        env, f"--temp_directory={temp}")
        try:
            assert other.wait_ready(), other.output()
            assert not spill.exists()
            assert not copy.parent.exists()
            assert (temp / "notes").read_text() == "mine"
            assert (temp / "OWNER").read_bytes() == _wal_owner(tmp_path / "c")[0]
        finally:
            assert other.stop() == 0, other.output()


def _segment_bytes(fake: FakeS3, prefix: str) -> int:
    """The bytes of segments the logged GETs read."""
    total = 0
    for entry in fake.log or []:
        if not entry["key"].startswith(f"{prefix}/segments/"):
            continue
        ranged = entry["range"]
        if ranged is None:
            total += len(fake.objects[("bucket", entry["key"])].data)
            continue
        first, last = ranged.removeprefix("bytes=").split("-")
        total += int(last) - int(first) + 1
    return total


def test_a_restarted_server_reads_evicted_blocks_from_its_temporary_directory(
        tmp_path: Path) -> None:
    with FakeS3() as fake:
        env = _environment(fake, {"AWS_ACCESS_KEY_ID": "key",
                                  "AWS_SECRET_ACCESS_KEY": "secret"})
        prefix = f"copies-{tmp_path.name}"
        url = f"s3://bucket/{prefix}"
        scan = "SELECT sum(length(body)) FROM t"
        first = _Server(url, tmp_path / "a", env)
        try:
            assert first.wait_ready(), first.output()
            with first.connect() as conn:
                conn.execute("CREATE TABLE t (id INT, body TEXT) "
                             "WITH (compaction_interval = 0)")
                conn.execute(
                    "INSERT INTO t SELECT i, concat_ws(' ', md5(i::VARCHAR), "
                    "md5((i + 1000000)::VARCHAR)) FROM range(1000000) r(i)")
                conn.execute("CHECKPOINT")
                expected = conn.execute(scan).fetchone()[0]
                # what the lower limit evicts keeps a copy
                conn.execute("SET GLOBAL memory_limit = '16MiB'")
                conn.execute("RESET GLOBAL memory_limit")
        finally:
            assert first.stop() == 0, first.output()
        copies = tmp_path / "a" / "temp" / "external_file_cache"
        assert any(copies.iterdir())
        stored = sum(len(fake.objects[("bucket", key)].data)
                     for key in fake.keys("bucket", f"{prefix}/segments/"))

        again = _Server(url, tmp_path / "a", env)
        try:
            assert again.wait_ready(), again.output()
            with again.connect() as conn:
                fake.log = []
                assert conn.execute(scan).fetchone()[0] == expected
                fetched = _segment_bytes(fake, prefix)
                fake.log = None
                read_back = conn.execute(
                    "SELECT value FROM sdb_metrics "
                    "WHERE metric = 'external_file_cache_spill_reads'").fetchone()[0]
        finally:
            assert again.stop() == 0, again.output()
        assert read_back > 0
        assert fetched < stored / 2, (fetched, stored)


def test_a_backup_on_an_object_store_restores_in_another_prefix(
        tmp_path: Path) -> None:
    with FakeS3() as fake:
        source_prefix = f"source-{tmp_path.name}"
        copy_prefix = f"copy-{tmp_path.name}"
        env = _environment(fake, {"AWS_ACCESS_KEY_ID": "key",
                                  "AWS_SECRET_ACCESS_KEY": "secret"})
        source = _Server(f"s3://bucket/{source_prefix}", tmp_path / "a", env)
        try:
            assert source.wait_ready(), source.output()
            with source.connect() as conn:
                conn.execute("CREATE TABLE t (a INT)")
                conn.execute("INSERT INTO t SELECT i FROM range(1000) r(i)")
                conn.execute("VACUUM (FLUSH_ALL)")
            with source.connect() as backup:
                copies = fake.requests["COPY"]
                backup.execute("SELECT pg_backup_start('s3')")
                assert fake.requests["COPY"] == copies + 1
                assert len(fake.keys("bucket", f"{source_prefix}/backups/p")) == 1
                with source.connect() as conn:
                    conn.execute("INSERT INTO t VALUES (-1)")
                    conn.execute("VACUUM (FLUSH_ALL)")
                    conn.execute("CHECKPOINT")
                for key in fake.keys("bucket", f"{source_prefix}/"):
                    fake.replace("bucket", copy_prefix + key[len(source_prefix):],
                                 fake.objects[("bucket", key)].data)
                (label,) = backup.execute(
                    "SELECT labelfile FROM pg_backup_stop()").fetchone()
            assert not fake.keys("bucket", f"{source_prefix}/backups/p")
            assert _count(source) == 1001
        finally:
            assert source.stop() == 0, source.output()
        fake.replace("bucket", f"{copy_prefix}/backup_label", label.encode())

        restored = _Server(f"s3://bucket/{copy_prefix}", tmp_path / "b", env,
                           "--require_existing")
        try:
            assert restored.wait_ready(), restored.output()
            assert _count(restored) == 1000
        finally:
            assert restored.stop() == 0, restored.output()
        keys = fake.keys("bucket", f"{copy_prefix}/")
        assert f"{copy_prefix}/backup_label" not in keys
        assert f"{copy_prefix}/backup_label.old" in keys
        assert not fake.keys("bucket", f"{copy_prefix}/backups/p")
        assert fake.objects[("bucket", f"{copy_prefix}/OWNER")].data == \
            _wal_owner(tmp_path / "b")[1]


def test_an_object_store_server_directory_needs_local_directories(
        tmp_path: Path) -> None:
    result = subprocess.run(
        [SERENED_BIN, "s3://bucket/nowhere", "--listen=postgres://127.0.0.1:1"],
        capture_output=True, text=True, timeout=60)
    assert result.returncode != 0, result
    assert "--wal_directory has to name a local directory" in \
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
