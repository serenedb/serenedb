"""Search index files in a tablespace: a local directory, an S3 bucket or an
Azure container, reached with the secret whose scope covers the location.
"""

from __future__ import annotations

import collections
import contextlib
import os
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pytest
from fake_azure import FakeAzure
from fake_s3 import FakeS3

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class _Server:
    def __init__(self, datadir: Path) -> None:
        self.port = _free_port()
        self.log = datadir.parent / f"serened-{self.port}.log"
        with self.log.open("w") as out:
            self.proc = subprocess.Popen(
                [SERENED_BIN, str(datadir),
                 f"--listen=postgres://127.0.0.1:{self.port}"],
                stdout=out, stderr=subprocess.STDOUT)

    def connect(self, dbname: str = "postgres",
                user: str = "postgres") -> psycopg.Connection:
        return psycopg.connect(host="127.0.0.1", port=self.port,
                               user=user, dbname=dbname, autocommit=True)

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

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=60)

    def output(self) -> str:
        return self.log.read_text()


CREATE_INDEX = ("CREATE INDEX docs_body ON docs USING inverted(body words) "
                "TABLESPACE idx")


def _create_tablespace(server: _Server, location: str) -> None:
    with server.connect() as conn:
        conn.execute(f"CREATE TABLESPACE idx LOCATION '{location}'")


def _create_index(server: _Server, tablespace: bool = True) -> None:
    with server.connect() as conn:
        conn.execute(
            "CREATE TEXT SEARCH DICTIONARY words AS "
            "split_by_non_alpha(case := 'lower')")
        conn.execute("CREATE TABLE docs(id INTEGER, body TEXT)")
        conn.execute(
            "INSERT INTO docs VALUES (1, 'quick brown fox'), (2, 'lazy dog')")
        conn.execute(CREATE_INDEX if tablespace else
                     CREATE_INDEX.removesuffix(" TABLESPACE idx"))


def _matches(server: _Server) -> list[int]:
    with server.connect() as conn:
        rows = conn.execute(
            "SELECT id FROM docs_body WHERE body @@ 'fox' ORDER BY id"
        ).fetchall()
    return [row[0] for row in rows]


def _refused(datadir: Path) -> str:
    server = _Server(datadir)
    try:
        assert not server.wait_ready(timeout=30)
        assert server.proc.wait(timeout=30) != 0
        return server.output()
    finally:
        server.stop()


def test_index_files_in_a_local_tablespace(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    location = tmp_path / "indexes"

    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        _create_tablespace(server, str(location))
        _create_index(server)
        assert _matches(server) == [1]
    finally:
        server.stop()
    assert any(path.is_file() for path in (location / "engine_v1").rglob("*"))
    assert not any(
        path.is_dir() and any(path.iterdir())
        for database in (datadir / "engine_v1").glob("[0-9]*")
        for path in database.glob("[0-9]*"))

    again = _Server(datadir)
    try:
        assert again.wait_ready(), again.output()
        assert _matches(again) == [1]
        with again.connect() as conn:
            conn.execute("DROP INDEX docs_body")
            conn.execute("DROP TABLESPACE idx")
    finally:
        again.stop()
    assert not (location / "engine_v1").exists()


def test_moved_data_directory_keeps_its_indexes(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        _create_index(server, tablespace=False)
    finally:
        server.stop()

    moved = tmp_path / "moved"
    datadir.rename(moved)
    again = _Server(moved)
    try:
        assert again.wait_ready(), again.output()
        assert _matches(again) == [1]
    finally:
        again.stop()


def _oids(conn: psycopg.Connection, names: list[str]) -> dict[str, int]:
    return dict(conn.execute(
        "SELECT relname, oid FROM pg_class WHERE relname = ANY(%s::text[])",
        (names,)).fetchall())


def _placement(conn: psycopg.Connection, names: list[str]) -> dict[str, str]:
    return dict(conn.execute(
        "SELECT c.relname, coalesce(t.spcname, '') FROM pg_class c "
        "LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace "
        "WHERE c.relname = ANY(%s::text[])", (names,)).fetchall())


def test_a_database_in_a_tablespace_keeps_its_objects_there(
        tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    home = tmp_path / "home"
    other = tmp_path / "other"
    names = ["docs", "notes_home", "notes_other", "in_default"]

    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute(f"CREATE TABLESPACE home LOCATION '{home}'")
            conn.execute(f"CREATE TABLESPACE other LOCATION '{other}'")
            conn.execute("CREATE DATABASE placed TABLESPACE home")
            database = conn.execute(
                "SELECT oid FROM pg_database WHERE datname = 'placed'"
            ).fetchone()[0]
        with server.connect("placed") as conn:
            conn.execute("CREATE TEXT SEARCH DICTIONARY words AS "
                         "split_text(case := 'lower')")
            conn.execute("CREATE TABLE docs (id INT, body TEXT) "
                         "WITH (storage = 'search')")
            conn.execute("CREATE TABLE notes (id INT, body TEXT)")
            conn.execute("CREATE INDEX notes_home ON notes "
                         "USING inverted(body words)")
            conn.execute("CREATE INDEX notes_other ON notes "
                         "USING inverted(body words) TABLESPACE other")
            conn.execute("CREATE TABLE in_default (id INT) "
                         "WITH (storage = 'search') TABLESPACE pg_default")
            conn.execute("INSERT INTO docs VALUES (1, 'quick brown fox'), "
                         "(2, 'lazy dog')")
            conn.execute("INSERT INTO notes VALUES (1, 'red fox'), "
                         "(2, 'blue whale')")
            conn.execute("VACUUM (REFRESH_TABLE) docs")
            conn.execute("VACUUM (REFRESH_TABLE) notes")
            assert _placement(conn, names) == {
                "docs": "", "notes_home": "", "notes_other": "other",
                "in_default": "pg_default"}
            oids = _oids(conn, names)
    finally:
        server.stop()

    in_home = home / "engine_v1" / str(database)
    assert (in_home / "data.db").is_file()
    assert (in_home / str(oids["docs"])).is_dir()
    assert (in_home / str(oids["notes_home"])).is_dir()
    assert (other / "engine_v1" / str(database)
            / str(oids["notes_other"])).is_dir()
    assert (datadir / "engine_v1" / str(database)
            / str(oids["in_default"])).is_dir()

    again = _Server(datadir)
    try:
        assert again.wait_ready(), again.output()
        with again.connect("placed") as conn:
            assert conn.execute(
                "SELECT id FROM docs WHERE body = 'lazy dog'").fetchall() == [(2,)]
            assert conn.execute(
                "SELECT id FROM notes_home WHERE body @@ 'fox' ORDER BY id"
            ).fetchall() == [(1,)]
            assert conn.execute(
                "SELECT id FROM notes_other WHERE body @@ 'whale' ORDER BY id"
            ).fetchall() == [(2,)]
        with again.connect() as conn:
            with pytest.raises(psycopg.Error, match="is not empty"):
                conn.execute("DROP TABLESPACE home")
            conn.execute("DROP DATABASE placed")
            conn.execute("DROP TABLESPACE home")
            conn.execute("DROP TABLESPACE other")
    finally:
        again.stop()
    assert not (home / "engine_v1").exists()
    assert not (other / "engine_v1").exists()
    assert not (datadir / "engine_v1" / str(database)).exists()


def test_the_tablespace_of_its_database_needs_no_grant(tmp_path: Path) -> None:
    server = _Server(tmp_path / "data")
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute(f"CREATE TABLESPACE home LOCATION '{tmp_path / 'home'}'")
            conn.execute(
                f"CREATE TABLESPACE other LOCATION '{tmp_path / 'other'}'")
            conn.execute("CREATE ROLE app LOGIN PASSWORD 'app_pw'")
            conn.execute("CREATE DATABASE appdb TABLESPACE home")
        with server.connect("appdb") as conn:
            conn.execute("GRANT ALL ON SCHEMA public TO app")
        with psycopg.connect(host="127.0.0.1", port=server.port, user="app",
                             password="app_pw", dbname="appdb",
                             autocommit=True) as conn:
            conn.execute("CREATE TABLE implicit (id INT) "
                         "WITH (storage = 'search')")
            conn.execute("CREATE TABLE explicit (id INT) "
                         "WITH (storage = 'search') TABLESPACE home")
            for tablespace in ("other", "pg_default"):
                with pytest.raises(psycopg.errors.InsufficientPrivilege,
                                   match=f"permission denied for tablespace "
                                         f"{tablespace}"):
                    conn.execute("CREATE TABLE elsewhere (id INT) "
                                 "WITH (storage = 'search') "
                                 f"TABLESPACE {tablespace}")
            assert _placement(conn, ["implicit", "explicit"]) == {
                "implicit": "", "explicit": ""}
    finally:
        server.stop()


def test_attach_cannot_place_a_database_by_tablespace_oid(
        tmp_path: Path) -> None:
    home = tmp_path / "home"
    server = _Server(tmp_path / "data")
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute(f"CREATE TABLESPACE home LOCATION '{home}'")
            oid = conn.execute(
                "SELECT oid FROM pg_tablespace WHERE spcname = 'home'"
            ).fetchone()[0]
            conn.execute("CREATE ROLE creator CREATEDB")
            conn.execute("SET ROLE creator")
            with pytest.raises(psycopg.errors.InsufficientPrivilege,
                               match="permission denied for tablespace home"):
                conn.execute("CREATE DATABASE placed TABLESPACE home")
            with pytest.raises(psycopg.Error, match="tablespace_oid"):
                conn.execute(
                    f"ATTACH '' AS attached (TYPE serenedb, tablespace_oid {oid})")
            conn.execute("RESET ROLE")
            assert conn.execute(
                "SELECT count(*) FROM pg_database WHERE datname = 'attached'"
            ).fetchone()[0] == 0
    finally:
        server.stop()
    assert not list((home / "engine_v1").glob("[0-9]*"))


MINIO = ("MINIO_HOST", "MINIO_PORT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
         "MINIO_BUCKET")


@dataclass
class _Store:
    endpoint: str
    key_id: str
    secret: str
    bucket: str
    fake: FakeS3 | None = None


@pytest.fixture
def s3_store():
    if all(os.environ.get(name) for name in MINIO):
        env = {name: os.environ[name] for name in MINIO}
        yield _Store(f"{env['MINIO_HOST']}:{env['MINIO_PORT']}",
                     env["MINIO_ACCESS_KEY"], env["MINIO_SECRET_KEY"],
                     env["MINIO_BUCKET"])
        return
    with FakeS3() as fake:
        yield _Store(fake.endpoint, "key", "secret", "bucket", fake)


def _index_files(fake: FakeS3, prefix: str) -> list[str]:
    return [key for key in fake.keys("bucket", prefix)
            if not key.endswith((".storage", "/engine_v1/tablespace"))]


def _create_secret(server: _Server, store: _Store, location: str,
                   tablespace: str | None = None) -> None:
    with server.connect() as conn:
        conn.execute(
            "CREATE PERSISTENT SECRET index_store (TYPE s3, "
            f"KEY_ID '{store.key_id}', SECRET '{store.secret}', "
            f"ENDPOINT '{store.endpoint}', URL_STYLE 'path', USE_SSL false, "
            f"SCOPE '{location}')")
    _create_tablespace(server, tablespace or location)


def test_index_files_on_s3(tmp_path: Path, s3_store: _Store) -> None:
    prefix = f"tablespace-{tmp_path.name}"
    location = f"s3://{s3_store.bucket}/{prefix}"
    datadir = tmp_path / "data"

    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        _create_secret(server, s3_store, location)
        _create_index(server)
        assert _matches(server) == [1]
        with server.connect() as conn:
            conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
            conn.execute("VACUUM (REFRESH_TABLE) docs")
            conn.execute("CREATE TABLE notes(id INTEGER, body TEXT) "
                         "WITH (storage = 'search') TABLESPACE idx")
            conn.execute("INSERT INTO notes VALUES (1, 'kept on s3')")
            conn.execute("CREATE DATABASE placed TABLESPACE idx")
        assert _matches(server) == [1, 3]
    finally:
        server.stop()
    assert len(list((datadir / "engine_v1").glob("*/data.db"))) == 2, list(
        datadir.rglob("*"))

    again = _Server(datadir)
    try:
        assert again.wait_ready(), again.output()
        assert _matches(again) == [1, 3]
        with again.connect() as conn:
            assert conn.execute("SELECT body FROM notes").fetchall() == [
                ("kept on s3",)]
            with pytest.raises(psycopg.Error, match="is not empty"):
                conn.execute("DROP TABLESPACE idx")
            conn.execute("DROP DATABASE placed")
            conn.execute("DROP TABLE notes")
            conn.execute("DROP INDEX docs_body")
            conn.execute("DROP TABLESPACE idx")
    finally:
        again.stop()
    if s3_store.fake:
        assert not s3_store.fake.keys(s3_store.bucket, f"{prefix}/"), \
            s3_store.fake.keys(s3_store.bucket)


AZURITE = ("AZURITE_CONNECTION_STRING", "AZURITE_BLOB_CONTAINER")


@dataclass
class _Container:
    connection_string: str
    name: str
    fake: FakeAzure | None = None


@pytest.fixture
def azure_container():
    if all(os.environ.get(name) for name in AZURITE):
        yield _Container(os.environ["AZURITE_CONNECTION_STRING"],
                         os.environ["AZURITE_BLOB_CONTAINER"])
        return
    with FakeAzure() as fake:
        yield _Container(fake.connection_string, "indexes", fake)


def test_index_files_on_azure(tmp_path: Path, azure_container: _Container) -> None:
    location = f"az://{azure_container.name}/cluster-{tmp_path.name}"
    datadir = tmp_path / "data"
    fake = azure_container.fake
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute(
                "CREATE PERSISTENT SECRET index_store (TYPE azure, "
                f"CONNECTION_STRING '{azure_container.connection_string}', "
                f"SCOPE '{location}')")
        _create_tablespace(server, location)
        _create_index(server)
        assert _matches(server) == [1]
        with server.connect() as conn:
            conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
            conn.execute("VACUUM (REFRESH_TABLE) docs")
        assert _matches(server) == [1, 3]
    finally:
        server.stop()
    if fake:
        assert fake.requests["COPY"] > 0, fake.requests

    again = _Server(datadir)
    try:
        assert again.wait_ready(), again.output()
        assert _matches(again) == [1, 3]
        with again.connect() as conn:
            conn.execute("DROP INDEX docs_body")
            conn.execute("DROP TABLESPACE idx")
    finally:
        again.stop()
    if fake:
        prefix = f"cluster-{tmp_path.name}/"
        assert not fake.names(azure_container.name, prefix), fake.names(
            azure_container.name)


def _large_table(server: _Server, store: _Store, location: str) -> None:
    _create_secret(server, store, location)
    with server.connect() as conn:
        conn.execute(
            "CREATE TEXT SEARCH DICTIONARY words AS "
            "split_by_non_alpha(case := 'lower')")
        conn.execute("CREATE TABLE docs(id INTEGER, body TEXT)")
        conn.execute(
            "INSERT INTO docs SELECT i, concat_ws(' ', 'fox', md5(i::VARCHAR), "
            "md5((i + 1000000)::VARCHAR), md5((i + 2000000)::VARCHAR)) "
            "FROM range(300000) t(i)")


def _count_fox(server: _Server) -> int:
    with server.connect() as conn:
        return conn.execute(
            "SELECT count(*) FROM docs_body WHERE body @@ 'fox'").fetchone()[0]


def test_large_index_files_upload_parts_at_once(tmp_path: Path) -> None:
    location = f"s3://bucket/parts-{tmp_path.name}"
    with FakeS3(latency=0.05) as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _large_table(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                         location)
            with server.connect() as conn:
                conn.execute(
                    CREATE_INDEX)
            assert _count_fox(server) == 300000
        finally:
            server.stop()
    assert fake.requests["PUT_PART"] > 2, fake.requests
    assert fake.max_parts_in_flight > 1, fake.requests
    assert fake.requests["CHECKSUM"] > fake.requests["COMPLETE_UPLOAD"] > 0, fake.requests


def test_a_trickling_read_fails_by_its_deadline(tmp_path: Path) -> None:
    location = f"s3://bucket/trickle-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            with server.connect() as conn:
                conn.execute("SET GLOBAL http_timeout = 2")
                conn.execute("SET GLOBAL http_retries = 1")
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower')")
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT)")
                conn.execute(
                    "INSERT INTO docs SELECT i, concat_ws(' ', 'fox', md5(i::VARCHAR)) "
                    "FROM range(30000) t(i)")
                conn.execute(
                    CREATE_INDEX)
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                fake.trickle = 2048
                began = time.monotonic()
                with pytest.raises(psycopg.Error):
                    conn.execute(
                        "SELECT count(*) FROM docs_body WHERE body @@ 'fox'").fetchall()
                assert time.monotonic() - began < 30
        finally:
            fake.trickle = 0
            server.stop()


def test_reads_refuse_an_object_replaced_after_its_upload(tmp_path: Path) -> None:
    location = f"s3://bucket/replaced-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
            assert _matches(server) == [1]
            terms = [key for key in fake.keys("bucket", f"replaced-{tmp_path.name}/")
                     if key.endswith(".idx")]
            assert terms, fake.keys("bucket")
            for key in terms:
                with fake.lock:
                    size = len(fake.objects[("bucket", key)].data)
                fake.replace("bucket", key, bytes(size))
            with server.connect() as conn:
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                with pytest.raises(psycopg.Error, match="changed after it was opened"):
                    conn.execute(
                        "SELECT id FROM docs_body WHERE body @@ 'fox'").fetchall()
        finally:
            server.stop()


def test_a_failed_part_aborts_its_upload(tmp_path: Path) -> None:
    location = f"s3://bucket/failed-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _large_table(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                         location)
            fake.failing_parts.add(2)
            with server.connect() as conn:
                conn.execute("SET GLOBAL http_retries = 0")
                with pytest.raises(psycopg.Error):
                    conn.execute(
                        CREATE_INDEX)
            assert fake.requests["ABORT_UPLOAD"] > 0, fake.requests
            assert not fake.uploads
            fake.failing_parts.clear()
            with server.connect() as conn:
                conn.execute(
                    CREATE_INDEX)
            assert _count_fox(server) == 300000
        finally:
            server.stop()


def test_failed_commit_is_retried(tmp_path: Path) -> None:
    location = f"s3://bucket/retried-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
            fake.copy_error = "AccessDenied"
            with server.connect() as conn:
                conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
                with pytest.raises(psycopg.Error):
                    conn.execute("VACUUM (REFRESH_TABLE) docs")
                with pytest.raises(psycopg.Error,
                                   match="cannot commit its last refresh"):
                    conn.execute("CHECKPOINT")
            assert _matches(server) == [1]
            fake.copy_error = None
            with server.connect() as conn:
                conn.execute("VACUUM (REFRESH_TABLE) docs")
                conn.execute("CHECKPOINT")
            assert _matches(server) == [1, 3]
        finally:
            server.stop()


def test_throttled_requests_are_retried(tmp_path: Path) -> None:
    location = f"s3://bucket/throttled-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            fake.throttle = 2
            _create_index(server)
            with server.connect() as conn:
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
            fake.throttle = 2
            assert _matches(server) == [1]
        finally:
            server.stop()
    assert fake.requests["THROTTLED"] == 4, fake.requests


def test_a_location_is_kept_normalized(tmp_path: Path) -> None:
    location = f"s3://bucket/nested-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location, f"s3://bucket//nested-{tmp_path.name}/")
            with server.connect() as conn:
                assert conn.execute(
                    "SELECT pg_tablespace_location(oid) FROM pg_tablespace "
                    "WHERE spcname = 'idx'").fetchall() == [(location,)]
            _create_index(server)
            with server.connect() as conn:
                conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
                conn.execute("VACUUM (REFRESH_TABLE) docs")
            assert _matches(server) == [1, 3]
        finally:
            server.stop()
    assert fake.objects and all(
        key.startswith(f"nested-{tmp_path.name}/") for _, key in fake.objects), \
        sorted(fake.objects)


def test_a_lost_upload_completion_fails_its_statement_and_leaves_no_object(
        tmp_path: Path) -> None:
    location = f"s3://bucket/lost-{tmp_path.name}"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _large_table(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                         location)
            fake.drop["COMPLETE_UPLOAD"] = 1
            with server.connect() as conn:
                with pytest.raises(psycopg.Error, match="multipart upload"):
                    conn.execute(
                        CREATE_INDEX)
                conn.execute(
                    CREATE_INDEX)
            assert _count_fox(server) == 300000
        finally:
            server.stop()
        assert fake.dropped, fake.requests
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _count_fox(again) == 300000
        finally:
            again.stop()
        assert not set(fake.dropped) & set(fake.keys("bucket")), fake.dropped


def test_a_lost_copy_response_is_retried(tmp_path: Path) -> None:
    location = f"s3://bucket/copy-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
            fake.drop["COPY"] = 1
            with server.connect() as conn:
                conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
                conn.execute("VACUUM (REFRESH_TABLE) docs")
            assert _matches(server) == [1, 3]
        finally:
            server.stop()
    assert fake.dropped, fake.requests


def test_a_store_down_at_startup_refuses_the_start(tmp_path: Path) -> None:
    location = f"s3://bucket/down-{tmp_path.name}"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
        finally:
            server.stop()
    output = _refused(datadir)
    assert f"/bucket/down-{tmp_path.name}/" in output and "Could not connect" in output, output


def test_a_crash_while_creating_an_index_leaves_no_objects(tmp_path: Path) -> None:
    location = f"s3://bucket/crash-{tmp_path.name}"
    prefix = f"crash-{tmp_path.name}/"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower')")
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT)")
                conn.execute(
                    "INSERT INTO docs VALUES (1, 'quick brown fox'), (2, 'lazy dog')")
                try:
                    conn.execute("SET sdb_faults = 'crash_before_finish_creation'")
                except psycopg.Error:
                    pytest.skip("built without fault injection")
                with pytest.raises(psycopg.OperationalError):
                    conn.execute(
                        CREATE_INDEX)
            server.proc.wait(timeout=60)
            assert _index_files(fake, prefix), fake.keys("bucket")
        finally:
            server.stop()
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            with again.connect() as conn:
                with pytest.raises(psycopg.Error):
                    conn.execute("SELECT * FROM docs_body")
        finally:
            again.stop()
        assert not _index_files(fake, prefix), fake.keys("bucket")


def _kill_while_stalled(server: _Server, fake: FakeS3, statement) -> None:
    def run() -> None:
        with contextlib.suppress(psycopg.Error), server.connect() as conn:
            statement(conn)

    worker = threading.Thread(target=run)
    worker.start()
    try:
        assert fake.stalled.wait(120), fake.requests
        server.proc.kill()
        server.proc.wait(timeout=60)
    finally:
        fake.release.set()
        worker.join(timeout=60)


def _crash_while_committing(tmp_path: Path, kind: str, key: str) -> None:
    location = f"s3://bucket/commit-{tmp_path.name}"
    prefix = f"commit-{tmp_path.name}/"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
            fake.stall(kind, key)

            def refresh(conn: psycopg.Connection) -> None:
                conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
                conn.execute("VACUUM (REFRESH_TABLE) docs")

            _kill_while_stalled(server, fake, refresh)
        finally:
            server.stop()
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _matches(again) == [1, 3]
        finally:
            again.stop()
        assert not [name for name in fake.keys("bucket", prefix)
                    if "pending_segments_" in name], fake.keys("bucket", prefix)


def test_a_crash_before_the_commit_copy_keeps_the_last_commit(
        tmp_path: Path) -> None:
    _crash_while_committing(tmp_path, "COPY", "/segments_")


def test_a_crash_between_the_commit_copy_and_its_delete_keeps_the_new_commit(
        tmp_path: Path) -> None:
    _crash_while_committing(tmp_path, "DELETE", "pending_segments_")


def test_a_crash_during_an_upload_leaves_no_index_files(tmp_path: Path) -> None:
    location = f"s3://bucket/upload-{tmp_path.name}"
    prefix = f"upload-{tmp_path.name}/"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _large_table(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                         location)
            fake.stall("PUT_PART", prefix)
            _kill_while_stalled(server, fake,
                                lambda conn: conn.execute(CREATE_INDEX))
        finally:
            server.stop()
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert not _index_files(fake, prefix), fake.keys("bucket", prefix)
            with again.connect() as conn:
                with pytest.raises(psycopg.Error):
                    conn.execute("SELECT * FROM docs_body")
                conn.execute(CREATE_INDEX)
            assert _count_fox(again) == 300000
        finally:
            again.stop()


def test_an_orphan_object_is_removed_when_its_index_opens(tmp_path: Path) -> None:
    location = f"s3://bucket/orphan-{tmp_path.name}"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
        finally:
            server.stop()
        commits = [key for key in fake.keys("bucket", f"orphan-{tmp_path.name}/")
                   if key.rsplit("/", 1)[-1].startswith("segments_")]
        assert commits, fake.keys("bucket")
        orphan = commits[0].rsplit("/", 1)[0] + "/_99.doc"
        fake.replace("bucket", orphan, b"written by an upload no commit names")
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _matches(again) == [1]
        finally:
            again.stop()
        assert orphan not in fake.keys("bucket"), fake.keys("bucket")


def test_an_orphan_object_of_a_search_table_is_removed_after_a_restart(
        tmp_path: Path) -> None:
    location = f"s3://bucket/orphan-table-{tmp_path.name}"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            with server.connect() as conn:
                conn.execute("CREATE TABLE notes (id INT, body TEXT) "
                             "WITH (storage = 'search') TABLESPACE idx")
                conn.execute("INSERT INTO notes VALUES (1, 'kept on s3')")
                conn.execute("VACUUM (REFRESH_TABLE) notes")
        finally:
            server.stop()
        commits = [key for key in fake.keys("bucket", f"orphan-table-{tmp_path.name}/")
                   if key.rsplit("/", 1)[-1].startswith("segments_")]
        assert commits, fake.keys("bucket")
        orphan = commits[0].rsplit("/", 1)[0] + "/_99.doc"
        fake.replace("bucket", orphan, b"written by an upload no commit names")
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            with again.connect() as conn:
                assert conn.execute("SELECT body FROM notes").fetchall() == [
                    ("kept on s3",)]
            assert _eventually(lambda: orphan not in fake.keys("bucket")), \
                fake.keys("bucket")
        finally:
            again.stop()


def test_a_partitioned_copy_to_s3_checks_its_target_with_one_listing(tmp_path: Path) -> None:
    target = f"s3://bucket/copied-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute(
                    "CREATE PERSISTENT SECRET copy_store (TYPE s3, KEY_ID 'key', SECRET 'secret', "
                    f"ENDPOINT '{fake.endpoint}', URL_STYLE 'path', USE_SSL false, SCOPE '{target}')")
                conn.execute(f"COPY (SELECT i % 10 AS p, i FROM range(1000) t(i)) TO '{target}' "
                             "(FORMAT csv, PARTITION_BY (p))")
                written = fake.keys("bucket", f"copied-{tmp_path.name}/")
                assert len(written) == 10, written
                listings = fake.requests["LIST"]
                conn.execute(f"COPY (SELECT i % 5 AS p, i FROM range(100) t(i)) TO '{target}' "
                             "(FORMAT csv, PARTITION_BY (p), OVERWRITE)")
                assert fake.requests["LIST"] - listings == 1, fake.requests
                rewritten = fake.keys("bucket", f"copied-{tmp_path.name}/")
                assert [key.split("/")[1] for key in rewritten] == [f"p={i}" for i in range(5)], rewritten
                with pytest.raises(psycopg.Error, match="is not empty"):
                    conn.execute(f"COPY (SELECT 1 AS p, 2 AS i) TO '{target}' (FORMAT csv, PARTITION_BY (p))")
        finally:
            server.stop()


@pytest.mark.parametrize("kind", ["s3", "azure"])
def test_a_restart_lists_each_directory_once(tmp_path: Path, kind: str) -> None:
    prefix = f"listed-{tmp_path.name}"
    datadir = tmp_path / "data"
    with FakeS3() if kind == "s3" else FakeAzure() as fake:
        if kind == "s3":
            bucket, location = "bucket", f"s3://bucket/{prefix}"
            keys = fake.keys
        else:
            bucket, location = "indexes", f"az://indexes/{prefix}"
            keys = fake.names
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            if kind == "s3":
                _create_secret(server, _Store(fake.endpoint, "key", "secret", bucket),
                               location)
            else:
                with server.connect() as conn:
                    conn.execute(
                        "CREATE PERSISTENT SECRET index_store (TYPE azure, "
                        f"CONNECTION_STRING '{fake.connection_string}', "
                        f"SCOPE '{location}')")
                _create_tablespace(server, location)
            _create_index(server)
        finally:
            server.stop()
        markers = [key for key in keys(bucket, f"{prefix}/") if key.endswith(".storage")]
        assert len(markers) == 1, keys(bucket)
        index = markers[0].removesuffix(".storage")
        database = index.rsplit("/", 1)[0]
        for i in range(6000):
            fake.replace(bucket, f"{database}/archive/{i}", b"")
        orphan = f"{index}/_99.doc"
        fake.replace(bucket, orphan, b"written by an upload no commit names")
        listings = fake.requests["LIST"]
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _matches(again) == [1]
            assert _eventually(lambda: orphan not in keys(bucket, f"{index}/")), \
                keys(bucket, f"{index}/")
            assert fake.requests["LIST"] - listings == 3, fake.requests
            objects = len(keys(bucket, f"{prefix}/"))
            page = 1000 if kind == "s3" else 5000
            listings = fake.requests["LIST"]
            with again.connect() as conn:
                assert conn.execute("SELECT pg_tablespace_size('idx')").fetchone()[0] > 0
            assert fake.requests["LIST"] - listings == -(-objects // page), fake.requests
        finally:
            again.stop()


def test_restored_objects_are_read_by_the_etags_they_are_listed_with(
        tmp_path: Path) -> None:
    location = f"s3://bucket/restored-{tmp_path.name}"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
            assert _matches(server) == [1]
        finally:
            server.stop()
        with fake.lock:
            for (_, key), obj in fake.objects.items():
                if key.startswith(f"restored-{tmp_path.name}/"):
                    obj.etag = '"copied-' + obj.etag.strip('"') + '"'
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _matches(again) == [1]
        finally:
            again.stop()


def _token_secret(store: _Store, location: str, token: str) -> str:
    return ("CREATE OR REPLACE PERSISTENT SECRET index_store (TYPE s3, "
            f"KEY_ID '{store.key_id}', SECRET '{store.secret}', "
            f"SESSION_TOKEN '{token}', ENDPOINT '{store.endpoint}', "
            f"URL_STYLE 'path', USE_SSL false, SCOPE '{location}')")


def _eventually(check, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return True
        time.sleep(0.1)
    return check()


def test_a_rotated_secret_reaches_files_opened_before_it(tmp_path: Path) -> None:
    location = f"s3://bucket/token-{tmp_path.name}"
    with FakeS3() as fake:
        fake.session_tokens = {"first"}
        store = _Store(fake.endpoint, "key", "secret", "bucket", fake)
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute(_token_secret(store, location, "first"))
            _create_tablespace(server, location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower')")
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT)")
                conn.execute(
                    "INSERT INTO docs SELECT i, concat_ws(' ', 'fox', "
                    "md5(i::VARCHAR)) FROM range(20000) t(i)")
                conn.execute(
                    "CREATE INDEX docs_body ON docs USING inverted(body words) "
                    "WITH (refresh_interval = 100, compaction_interval = 0) "
                    "TABLESPACE idx")
            assert _count_fox(server) == 20000

            fake.session_tokens = {"first", "second"}
            with server.connect() as conn:
                conn.execute(_token_secret(store, location, "second"))
            fake.session_tokens = {"second"}

            with server.connect() as conn:
                conn.execute(
                    "INSERT INTO docs SELECT i, 'fox' FROM range(20000, 20100) t(i)")
            assert _eventually(lambda: _count_fox(server) == 20100), \
                server.output()

            with server.connect() as conn:
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
            assert _count_fox(server) == 20100, server.output()
            assert fake.stats()["requests"].get("EXPIRED", 0) > 0
        finally:
            server.stop()


def test_an_expired_token_holds_refreshes_until_a_restart(tmp_path: Path) -> None:
    location = f"s3://bucket/expired-{tmp_path.name}"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        fake.session_tokens = {"first"}
        store = _Store(fake.endpoint, "key", "secret", "bucket", fake)
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute(_token_secret(store, location, "first"))
            _create_tablespace(server, location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower')")
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT)")
                conn.execute(
                    "INSERT INTO docs SELECT i, 'fox' FROM range(1000) t(i)")
                conn.execute(
                    "CREATE INDEX docs_body ON docs USING inverted(body words) "
                    "WITH (refresh_interval = 100, compaction_interval = 0) "
                    "TABLESPACE idx")
            assert _count_fox(server) == 1000

            fake.session_tokens = {"second"}
            with server.connect() as conn:
                conn.execute(
                    "INSERT INTO docs SELECT i, 'fox' FROM range(1000, 1100) t(i)")
            assert _eventually(
                lambda: fake.stats()["requests"].get("EXPIRED", 0) > 0)
            with server.connect() as conn:
                conn.execute(_token_secret(store, location, "second"))
            time.sleep(1)
            assert server.proc.poll() is None, server.output()
            assert _count_fox(server) == 1000
        finally:
            server.stop()

        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _count_fox(again) == 1100
        finally:
            again.stop()


def _sas_secret(fake: FakeAzure, location: str, signature: str) -> str:
    return ("CREATE OR REPLACE PERSISTENT SECRET index_store (TYPE azure, "
            f"CONNECTION_STRING '{fake.sas_connection_string(signature)}', "
            f"SCOPE '{location}')")


def test_a_rotated_azure_secret_reaches_files_opened_before_it(
        tmp_path: Path) -> None:
    location = f"az://indexes/token-{tmp_path.name}"
    with FakeAzure() as fake:
        fake.sas_signatures = {"first"}
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute(_sas_secret(fake, location, "first"))
            _create_tablespace(server, location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower')")
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT)")
                conn.execute(
                    "INSERT INTO docs SELECT i, concat_ws(' ', 'fox', "
                    "md5(i::VARCHAR)) FROM range(20000) t(i)")
                conn.execute(
                    "CREATE INDEX docs_body ON docs USING inverted(body words) "
                    "WITH (refresh_interval = 100, compaction_interval = 0) "
                    "TABLESPACE idx")
            assert _count_fox(server) == 20000

            fake.sas_signatures = {"first", "second"}
            with server.connect() as conn:
                conn.execute(_sas_secret(fake, location, "second"))
            fake.sas_signatures = {"second"}

            with server.connect() as conn:
                conn.execute(
                    "INSERT INTO docs SELECT i, 'fox' FROM range(20000, 20100) t(i)")
            assert _eventually(lambda: _count_fox(server) == 20100), \
                server.output()

            with server.connect() as conn:
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
            assert _count_fox(server) == 20100, server.output()
            assert fake.requests["FORBIDDEN"] > 0, fake.requests
        finally:
            server.stop()


def _pending_uploads(conn: psycopg.Connection) -> dict[str, int]:
    return dict(conn.execute(
        "SELECT metric, value FROM sdb_metrics "
        "WHERE relation_id IS NULL AND metric LIKE 'index_upload%' "
        "ORDER BY metric").fetchall())


def test_pending_uploads_are_reported(tmp_path: Path) -> None:
    location = f"s3://bucket/pending-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _large_table(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                         location)
            errors: list[Exception] = []

            def build() -> None:
                try:
                    with server.connect() as conn:
                        conn.execute(
                            CREATE_INDEX)
                except Exception as e:  # noqa: BLE001
                    errors.append(e)

            fake.latency = 0.2
            builder = threading.Thread(target=build)
            builder.start()
            seen = []
            with server.connect() as conn:
                while builder.is_alive():
                    seen.append(_pending_uploads(conn))
                    time.sleep(0.05)
                builder.join()
                fake.latency = 0
                deadline = time.monotonic() + 30
                after = _pending_uploads(conn)
                while any(after.values()) and time.monotonic() < deadline:
                    time.sleep(0.1)
                    after = _pending_uploads(conn)
        finally:
            fake.latency = 0
            server.stop()
    assert not errors, errors
    assert any(rows["index_uploads_pending"] > 0 and rows["index_upload_pending_bytes"] > 0
               for rows in seen), seen[:5]
    assert after == {"index_upload_pending_bytes": 0, "index_uploads_pending": 0}, after


def test_store_requests_are_logged(tmp_path: Path) -> None:
    location = f"s3://bucket/logged-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
            with server.connect() as conn:
                conn.execute("SET GLOBAL logging_storage = 'memory'")
                conn.execute("CALL enable_logging('HTTP')")
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                fake.reset()
                assert _matches(server) == [1]
                logged = dict(conn.execute(
                    "SELECT request.type, count(*) FROM sdb_logs_parsed('HTTP') "
                    "WHERE request.url LIKE %s GROUP BY ALL",
                    (f"%/logged-{tmp_path.name}/%",)).fetchall())
        finally:
            server.stop()
    assert logged.get("GET") == fake.requests["GET"] > 0, (logged, fake.requests)


def _one_round(log: list[dict], extension: str) -> list[dict]:
    reads = [entry for entry in log if entry["key"].endswith(extension)]
    assert reads, log
    assert max(entry["began"] for entry in reads) < min(
        entry["ended"] for entry in reads), reads
    return reads


def test_cold_top_k_reads_postings_and_columns_in_one_round(
        tmp_path: Path) -> None:
    location = f"s3://bucket/rounds-{tmp_path.name}"
    segments = 4
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret",
                                          "bucket"), location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower') "
                    "WITH (frequency, position, norm)")
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT)")
                conn.execute(
                    "CREATE INDEX docs_body ON docs USING inverted(id, body words) "
                    "WITH (compaction_interval = 0) TABLESPACE idx")
                for segment in range(segments):
                    conn.execute(
                        "INSERT INTO docs SELECT i, CASE i % 10 WHEN 0 THEN 'alpha' "
                        "WHEN 1 THEN 'beta' ELSE 'gamma' END "
                        f"FROM range({segment * 1000}, {(segment + 1) * 1000}) t(i)")
                    conn.execute("VACUUM (REFRESH_TABLE) docs")
                conn.execute("SET GLOBAL threads = 2")
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                fake.latency = 0.1
                fake.log = []
                rows = conn.execute(
                    "SELECT id FROM docs_body WHERE body @@ to_tsquery('alpha beta') "
                    "ORDER BY BM25(docs_body.tableoid) DESC LIMIT 1000").fetchall()
                log, fake.log = fake.log, None
        finally:
            server.stop()
    assert sorted(row[0] for row in rows) == [
        i for i in range(segments * 1000) if i % 10 < 2]
    assert len(_one_round(log, ".doc")) <= segments
    assert len(_one_round(log, ".col")) == segments


def test_cold_count_reads_its_postings_in_one_round(tmp_path: Path) -> None:
    location = f"s3://bucket/counts-{tmp_path.name}"
    segments = 4
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret",
                                          "bucket"), location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower') "
                    "WITH (frequency, position, norm)")
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT)")
                conn.execute(
                    "CREATE INDEX docs_body ON docs USING inverted(id, body words) "
                    "WITH (compaction_interval = 0) TABLESPACE idx")
                for segment in range(segments):
                    conn.execute(
                        "INSERT INTO docs SELECT i, CASE i % 10 WHEN 0 THEN 'alpha beta' "
                        "WHEN 1 THEN 'beta alpha' ELSE 'gamma' END "
                        f"FROM range({segment * 1000}, {(segment + 1) * 1000}) t(i)")
                    conn.execute("VACUUM (REFRESH_TABLE) docs")
                conn.execute("SET GLOBAL threads = 2")
                fake.latency = 0.1

                def cold_count(query: str) -> tuple[int, list[dict]]:
                    conn.execute("SET GLOBAL enable_external_file_cache = false")
                    conn.execute("SET GLOBAL enable_external_file_cache = true")
                    fake.log = []
                    count = conn.execute(
                        "SELECT count(*) FROM docs_body WHERE body @@ to_tsquery(%s)",
                        (query,)).fetchone()[0]
                    log, fake.log = fake.log, None
                    return count, log

                term, term_log = cold_count("alpha")
                phrase, phrase_log = cold_count('"alpha beta"')
        finally:
            server.stop()
    assert term == segments * 200
    assert not [entry for entry in term_log
                if entry["key"].endswith((".doc", ".pos"))], term_log
    assert phrase == segments * 100
    by_segment = collections.defaultdict(list)
    for entry in phrase_log:
        name = entry["key"].rsplit("/", 1)[-1]
        if name.endswith((".doc", ".pos")):
            by_segment[name.split(".")[0]].append(entry)
    assert len(by_segment) == segments, phrase_log
    for reads in by_segment.values():
        assert {entry["key"].rsplit(".", 1)[-1] for entry in reads} == {"doc", "pos"}, reads
        assert max(entry["began"] for entry in reads) < min(
            entry["ended"] for entry in reads), reads


def test_an_index_is_created_again_where_it_was_dropped(tmp_path: Path) -> None:
    location = f"s3://bucket/recreated-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
            assert _matches(server) == [1]
            with server.connect() as conn:
                conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
                conn.execute("VACUUM (REFRESH_TABLE) docs")
            assert _matches(server) == [1, 3]
            with server.connect() as conn:
                conn.execute("DROP INDEX docs_body")
                conn.execute(CREATE_INDEX)
            assert _matches(server) == [1, 3]
        finally:
            server.stop()
