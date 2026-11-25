"""Search index files in a tablespace: a local directory, an S3 bucket or an
Azure container, reached with the secret whose scope covers the location.
"""

from __future__ import annotations

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
from serened import reserve_port  # type: ignore[import-untyped]

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


class _Server:
    def __init__(self, datadir: Path) -> None:
        self.reserved = reserve_port()
        self.port = self.reserved.getsockname()[1]
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
        self.reserved.close()

    def output(self) -> str:
        return self.log.read_text()


CREATE_INDEX = "CREATE INDEX docs_body ON docs USING inverted(body words)"


def _create_tablespace(server: _Server, location: str) -> None:
    with server.connect() as conn:
        conn.execute(f"CREATE TABLESPACE idx LOCATION '{location}'")


def _create_index(server: _Server, tablespace: bool = True) -> None:
    with server.connect() as conn:
        conn.execute(
            "CREATE TEXT SEARCH DICTIONARY words AS "
            "split_by_non_alpha(case := 'lower')")
        conn.execute("CREATE TABLE docs(id INTEGER, body TEXT)"
                     + (" TABLESPACE idx" if tablespace else ""))
        conn.execute(CREATE_INDEX)
        conn.execute(
            "INSERT INTO docs VALUES (1, 'quick brown fox'), (2, 'lazy dog')")
        conn.execute("VACUUM (FLUSH_TABLE) docs")


def _create_view_index(conn: psycopg.Connection, source: Path) -> None:
    conn.execute(
        "COPY (SELECT 1 AS id, 'quick brown fox' AS body "
        f"UNION ALL SELECT 2, 'lazy dog') TO '{source}' (FORMAT PARQUET)")
    conn.execute(
        f"CREATE VIEW notes_view AS SELECT * FROM read_parquet('{source}')")
    conn.execute("CREATE INDEX notes ON notes_view USING inverted(id, body) "
                 "TABLESPACE idx")


def _matches(server: _Server) -> list[int]:
    with server.connect() as conn:
        rows = conn.execute(
            "SELECT id FROM docs_body WHERE body @@ 'fox' ORDER BY id"
        ).fetchall()
    return [row[0] for row in rows]


def _directory(location: Path) -> Path:
    [directory] = location.iterdir()
    return directory / "engine_v1"


def _refused(datadir: Path) -> str:
    server = _Server(datadir)
    try:
        assert not server.wait_ready(timeout=30)
        assert server.proc.wait(timeout=30) != 0
        return server.output()
    finally:
        server.stop()


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


def _files(conn: psycopg.Connection, names: list[str]) -> int:
    return conn.execute(
        "SELECT coalesce(sum(m.value), 0) FROM sdb_metrics m "
        "JOIN pg_class c ON c.oid = m.relation_id "
        "WHERE m.metric = 'num_files' AND c.relname = ANY(%s::text[])",
        (names,)).fetchone()[0]


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
    source = tmp_path / "notes.parquet"
    names = ["docs", "notes", "notes_home", "notes_other", "in_default"]

    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute(f"CREATE TABLESPACE home LOCATION '{home}'")
            conn.execute(f"CREATE TABLESPACE other LOCATION '{other}'")
            conn.execute("CREATE DATABASE placed TABLESPACE home")
        with server.connect("placed") as conn:
            conn.execute("CREATE TEXT SEARCH DICTIONARY words AS "
                         "split_text(case := 'lower')")
            conn.execute("CREATE TABLE docs (id INT, body TEXT)")
            conn.execute("CREATE TABLE notes (id INT, body TEXT)")
            conn.execute("CREATE INDEX notes_home ON notes "
                         "USING inverted(body words)")
            conn.execute("INSERT INTO docs VALUES (1, 'quick brown fox'), "
                         "(2, 'lazy dog')")
            conn.execute("INSERT INTO notes VALUES (1, 'red fox'), "
                         "(2, 'blue whale')")
            conn.execute("COPY (SELECT id, body FROM notes ORDER BY id) "
                         f"TO '{source}' (FORMAT PARQUET)")
            conn.execute("CREATE VIEW notes_view AS "
                         f"SELECT * FROM read_parquet('{source}')")
            conn.execute("CREATE INDEX notes_other ON notes_view "
                         "USING inverted(id, body words) TABLESPACE other")
            conn.execute("CREATE TABLE in_default (id INT) "
                         "TABLESPACE pg_default")
            conn.execute("INSERT INTO in_default VALUES (1)")
            conn.execute("VACUUM (FLUSH_TABLE) docs")
            conn.execute("VACUUM (FLUSH_TABLE) notes")
            conn.execute("VACUUM (FLUSH_TABLE) in_default")
            assert _placement(conn, names) == {
                "docs": "", "notes": "", "notes_home": "",
                "notes_other": "other", "in_default": "pg_default"}
            in_home = _directory(home) / "store"
            in_other = _directory(other) / "store"
            in_default = datadir / "engine_v1" / "store"
            for store, placed in ((in_home, ["docs", "notes"]),
                                  (in_other, ["notes_other"]),
                                  (in_default, ["in_default"])):
                for name in placed:
                    assert _files(conn, [name]) > 0, name
                assert len(list(store.iterdir())) == _files(conn, placed), store
    finally:
        server.stop()

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
    assert not any(in_home.iterdir())
    assert not any(in_other.iterdir())
    assert not any(in_default.iterdir())


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
            conn.execute("CREATE TABLE implicit (id INT)")
            conn.execute("CREATE TABLE explicit (id INT) TABLESPACE home")
            for tablespace in ("other", "pg_default"):
                with pytest.raises(psycopg.errors.InsufficientPrivilege,
                                   match=f"permission denied for tablespace "
                                         f"{tablespace}"):
                    conn.execute("CREATE TABLE elsewhere (id INT) "
                                 f"TABLESPACE {tablespace}")
            assert _placement(conn, ["implicit", "explicit"]) == {
                "implicit": "", "explicit": ""}
    finally:
        server.stop()


def test_a_database_in_a_tablespace_needs_create_on_it(
        tmp_path: Path) -> None:
    home = tmp_path / "home"
    server = _Server(tmp_path / "data")
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute(f"CREATE TABLESPACE home LOCATION '{home}'")
            conn.execute("CREATE ROLE creator CREATEDB")
            conn.execute("SET ROLE creator")
            with pytest.raises(psycopg.errors.InsufficientPrivilege,
                               match="permission denied for tablespace home"):
                conn.execute("CREATE DATABASE placed TABLESPACE home")
    finally:
        server.stop()
    assert not list(home.glob("*/engine_v1/store/*"))


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
            if not key.endswith("/")]


def _create_secret(server: _Server, store: _Store, location: str) -> None:
    with server.connect() as conn:
        conn.execute(
            "CREATE PERSISTENT SECRET index_store (TYPE s3, "
            f"KEY_ID '{store.key_id}', SECRET '{store.secret}', "
            f"ENDPOINT '{store.endpoint}', URL_STYLE 'path', USE_SSL false, "
            f"SCOPE '{location}')")
    _create_tablespace(server, location)


def test_a_store_that_never_accepts_fails_within_the_connect_timeout(
        tmp_path: Path) -> None:
    hole = socket.socket()
    hole.bind(("127.0.0.1", 0))
    hole.listen(0)
    port = hole.getsockname()[1]
    queued = []
    for _ in range(4):
        filler = socket.socket()
        filler.setblocking(False)
        filler.connect_ex(("127.0.0.1", port))
        queued.append(filler)
    location = f"s3://bucket/hole-{tmp_path.name}"
    server = _Server(tmp_path / "data")
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute(
                "CREATE PERSISTENT SECRET hole_store (TYPE s3, KEY_ID 'key', "
                f"SECRET 'secret', ENDPOINT '127.0.0.1:{port}', URL_STYLE 'path', "
                f"USE_SSL false, SCOPE '{location}')")
            began = time.monotonic()
            with pytest.raises(psycopg.Error):
                conn.execute(f"CREATE TABLESPACE hole LOCATION '{location}'")
            elapsed = time.monotonic() - began
    finally:
        server.stop()
        for filler in queued:
            filler.close()
        hole.close()
    assert elapsed < 20, elapsed


def test_a_request_that_never_connects_is_retried_whatever_its_method(
        tmp_path: Path) -> None:
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute(
                    "CREATE PERSISTENT SECRET closing_store (TYPE s3, KEY_ID 'key', "
                    f"SECRET 'secret', ENDPOINT '{fake.endpoint}', URL_STYLE 'path', "
                    "USE_SSL false, SCOPE 's3://bucket')")
                conn.execute("SET GLOBAL logging_storage = 'memory'")
                conn.execute("CALL enable_logging('HTTP')")
                fake.keep_alive = False
                fake.close_after("HEAD", 1)
                with pytest.raises(psycopg.Error):
                    conn.execute(
                        "COPY (SELECT i, repeat('x', 100) AS s FROM range(400000) t(i)) "
                        "TO 's3://bucket/closing/big.csv' (FORMAT csv)")
                attempts = conn.execute(
                    "SELECT count(*) FROM sdb_logs_parsed('HTTP') "
                    "WHERE request.type = 'POST' AND request.url LIKE %s",
                    ("%uploads%",)).fetchone()[0]
        finally:
            server.stop()
    assert fake.requests["HEAD"] == 1 and fake.requests["CREATE_UPLOAD"] == 0, fake.requests
    assert attempts == 4, attempts


@pytest.mark.parametrize("kind", ["s3", "azure"])
def test_a_tablespace_asks_for_nothing_above_its_location(tmp_path: Path,
                                                          kind: str) -> None:
    prefix = f"above-{tmp_path.name}/deep/er/ts"
    with FakeS3() if kind == "s3" else FakeAzure() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                if kind == "s3":
                    bucket, keys = "bucket", fake.keys
                    conn.execute(
                        "CREATE PERSISTENT SECRET wide (TYPE s3, KEY_ID 'key', "
                        f"SECRET 'secret', ENDPOINT '{fake.endpoint}', "
                        "URL_STYLE 'path', USE_SSL false, SCOPE 's3://bucket')")
                else:
                    bucket, keys = "indexes", fake.names
                    conn.execute(
                        "CREATE PERSISTENT SECRET wide (TYPE azure, "
                        f"CONNECTION_STRING '{fake.connection_string}', "
                        "SCOPE 'az://indexes')")
                scheme = "s3" if kind == "s3" else "az"
                trace: list[dict] = []
                if kind == "s3":
                    fake.trace = trace
                conn.execute(
                    f"CREATE TABLESPACE idx LOCATION '{scheme}://{bucket}/{prefix}'")
                conn.execute("CREATE TABLE notes(id INTEGER) TABLESPACE idx")
                conn.execute("INSERT INTO notes VALUES (1)")
                conn.execute("VACUUM (FLUSH_TABLE) notes")
        finally:
            server.stop()
        assert not [entry for entry in trace
                    if not entry["key"].startswith(prefix)], trace
        stored = keys(bucket)
        assert stored and not [key for key in stored
                               if not key.startswith(prefix)], stored


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
            conn.execute("VACUUM (FLUSH_TABLE) docs")
            conn.execute("CREATE TABLE notes(id INTEGER, body TEXT) "
                         "TABLESPACE idx")
            conn.execute("INSERT INTO notes VALUES (1, 'kept on s3')")
            conn.execute("CREATE DATABASE placed TABLESPACE idx")
        assert _matches(server) == [1, 3]
    finally:
        server.stop()
    assert not any((datadir / "engine_v1" / "store").iterdir()), list(
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
            conn.execute("DROP TABLE docs")
            conn.execute("DROP TABLESPACE idx")
    finally:
        again.stop()
    if s3_store.fake:
        assert not _index_files(s3_store.fake, f"{prefix}/"), \
            s3_store.fake.keys(s3_store.bucket)


def _create_azure_secret(server: _Server, fake: FakeAzure, location: str) -> None:
    with server.connect() as conn:
        conn.execute(
            "CREATE PERSISTENT SECRET index_store (TYPE azure, "
            f"CONNECTION_STRING '{fake.connection_string}', "
            f"SCOPE '{location}')")
    _create_tablespace(server, location)


def _fill_docs(server: _Server, rows: int) -> None:
    with server.connect() as conn:
        conn.execute(
            "CREATE TEXT SEARCH DICTIONARY words AS "
            "split_by_non_alpha(case := 'lower')")
        conn.execute("CREATE TABLE docs(id INTEGER, body TEXT) "
                     "WITH (compaction_interval = 0) TABLESPACE idx")
        conn.execute("SET threads = 1")
        conn.execute(
            "INSERT INTO docs SELECT i, concat_ws(' ', 'fox', md5(i::VARCHAR), "
            "md5((i + 1000000)::VARCHAR), md5((i + 2000000)::VARCHAR)) "
            f"FROM range({rows}) t(i)")
        conn.execute("VACUUM (FLUSH_TABLE) docs")


def _large_table(server: _Server, store: _Store, location: str) -> None:
    _create_secret(server, store, location)
    _fill_docs(server, 300000)


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
    assert fake.max_upload_parts_in_flight > 1, fake.requests
    assert fake.requests["CHECKSUM"] > fake.requests["COMPLETE_UPLOAD"] > 0, fake.requests
    assert fake.requests["UNSIGNED_PAYLOAD"] >= fake.requests["PUT_PART"], fake.requests
    assert fake.requests["SIGNED_PAYLOAD"] == 0, fake.requests


def test_blob_blocks_are_staged_from_cache_blocks_and_read_back(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    with FakeAzure() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute("SET GLOBAL azure_write_block_size = 1000000")
            _create_azure_secret(server, fake, f"az://indexes/blocks-{tmp_path.name}")
            _fill_docs(server, 20000)
            with server.connect() as conn:
                conn.execute(CREATE_INDEX)
        finally:
            server.stop()
        with fake.lock:
            blocks = sorted(len(block) for blob in fake.blobs.values()
                            for block in blob.blocks.values())
        assert blocks[-1] > 1000000, blocks
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _count_fox(again) == 20000
        finally:
            again.stop()


def test_one_part_in_flight_sends_each_part_from_the_write_that_fills_it(
        tmp_path: Path) -> None:
    location = f"s3://bucket/serial-{tmp_path.name}"
    with FakeS3(latency=0.05) as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute("SET GLOBAL s3_uploader_parts_in_flight = 1")
            _large_table(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                         location)
            with server.connect() as conn:
                conn.execute(CREATE_INDEX)
            assert _count_fox(server) == 300000
        finally:
            server.stop()
    assert fake.requests["PUT_PART"] > 2, fake.requests
    assert fake.max_upload_parts_in_flight == 1, fake.requests


def test_one_block_in_flight_stages_each_block_from_the_write_that_fills_it(
        tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    with FakeAzure() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute("SET GLOBAL azure_write_block_size = 1000000")
                conn.execute("SET GLOBAL azure_write_blocks_in_flight = 1")
            _create_azure_secret(server, fake, f"az://indexes/serial-{tmp_path.name}")
            _fill_docs(server, 20000)
            with server.connect() as conn:
                conn.execute(CREATE_INDEX)
            assert _count_fox(server) == 20000
        finally:
            server.stop()
    assert fake.requests["PUT_BLOCK"] > 1, fake.requests


@pytest.mark.faults
@pytest.mark.parametrize("kind", ["s3", "azure"])
def test_a_write_out_of_cache_blocks_uploads_every_byte(tmp_path: Path, kind: str) -> None:
    datadir = tmp_path / "data"
    with FakeS3() if kind == "s3" else FakeAzure() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            if kind == "s3":
                _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                               f"s3://bucket/short-{tmp_path.name}")
            else:
                _create_azure_secret(server, fake, f"az://indexes/short-{tmp_path.name}")
            _fill_docs(server, 20000)
            with server.connect() as conn:
                conn.execute("SET sdb_faults = 'remote_write_cache_block'")
                conn.execute(CREATE_INDEX)
                conn.execute("RESET sdb_faults")
        finally:
            server.stop()
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _count_fox(again) == 20000
        finally:
            again.stop()


def test_uploads_without_a_checksum_send_their_body_hash(tmp_path: Path) -> None:
    location = f"s3://bucket/hashed-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute("SET GLOBAL s3_upload_checksum = false")
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
            assert _matches(server) == [1]
        finally:
            server.stop()
    assert fake.requests["SIGNED_PAYLOAD"] > 0, fake.requests
    assert fake.requests["UNSIGNED_PAYLOAD"] == 0, fake.requests


@pytest.mark.parametrize("kind", ["s3", "azure"])
def test_a_trickling_read_fails_by_its_deadline(tmp_path: Path, kind: str) -> None:
    with FakeS3() if kind == "s3" else FakeAzure() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            if kind == "s3":
                _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                               f"s3://bucket/trickle-{tmp_path.name}")
            else:
                _create_azure_secret(server, fake, f"az://indexes/trickle-{tmp_path.name}")
            with server.connect() as conn:
                conn.execute("SET GLOBAL http_timeout = 2")
                conn.execute("SET GLOBAL http_retries = 1")
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower')")
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT) TABLESPACE idx")
                conn.execute(CREATE_INDEX)
                conn.execute(
                    "INSERT INTO docs SELECT i, concat_ws(' ', 'fox', md5(i::VARCHAR)) "
                    "FROM range(30000) t(i)")
                conn.execute("VACUUM (FLUSH_TABLE) docs")
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


@pytest.mark.parametrize("kind", ["s3", "azure"])
def test_a_read_uses_the_first_block_of_a_request_while_the_rest_arrives(
        tmp_path: Path, kind: str) -> None:
    query = "SELECT id FROM docs_body WHERE body @@ 'common' LIMIT 5"
    block = 131072
    with FakeS3() if kind == "s3" else FakeAzure() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            if kind == "s3":
                _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                               f"s3://bucket/arrival-{tmp_path.name}")
            else:
                _create_azure_secret(server, fake, f"az://indexes/arrival-{tmp_path.name}")
            with server.connect() as conn:
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower')")
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT) "
                             "WITH (compaction_interval = 0) TABLESPACE idx")
                conn.execute(
                    "CREATE INDEX docs_body ON docs USING inverted(id, body words)")
                conn.execute("SET threads = 1")
                conn.execute(
                    "INSERT INTO docs SELECT i, concat_ws(' ', "
                    "CASE WHEN i % 3 <> 0 THEN 'common' END, 'w' || (hash(i) % 5000)) "
                    "FROM range(600000) t(i)")
                conn.execute("VACUUM (FLUSH_TABLE) docs")
                expected = conn.execute(query).fetchall()
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                fake.hold_read(".doc", block)
                rows = conn.execute(query).fetchall()
                held = fake.read_held.is_set()
        finally:
            fake.read_resumed.set()
            server.stop()
    assert len(rows) == 5 and rows == expected
    assert held


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
            columns = [key for key in fake.keys("bucket", f"replaced-{tmp_path.name}/")
                       if key.endswith(".col")]
            assert columns, fake.keys("bucket")
            for key in columns:
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


def test_a_flush_opens_the_files_it_wrote_without_a_head(tmp_path: Path) -> None:
    location = f"s3://bucket/written-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
            assert _matches(server) == [1]
            fake.trace = []
            with server.connect() as conn:
                conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
                conn.execute("VACUUM (FLUSH_TABLE) docs")
            assert _matches(server) == [1, 3]
            trace, fake.trace = fake.trace, None
        finally:
            server.stop()
    written = {entry["key"] for entry in trace if entry["method"] == "PUT"}
    assert written, trace
    assert not [entry for entry in trace
                if entry["method"] == "HEAD" and entry["key"] in written], trace


def test_a_checkpoint_asks_the_store_for_nothing(tmp_path: Path) -> None:
    location = f"s3://bucket/checkpoint-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
            with server.connect() as conn:
                conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
                fake.reset()
                conn.execute("CHECKPOINT")
                requests = fake.stats()["requests"]
            assert _matches(server) == [1, 3]
        finally:
            server.stop()
    assert not requests, requests


def test_reads_refuse_a_blob_replaced_after_its_upload(tmp_path: Path) -> None:
    location = f"az://indexes/replaced-{tmp_path.name}"
    with FakeAzure() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute(
                    "CREATE PERSISTENT SECRET index_store (TYPE azure, "
                    f"CONNECTION_STRING '{fake.connection_string}', "
                    f"SCOPE '{location}')")
            _create_tablespace(server, location)
            _create_index(server)
            assert _matches(server) == [1]
            columns = [name for name in fake.names("indexes", f"replaced-{tmp_path.name}/")
                       if name.endswith(".col")]
            assert columns, fake.names("indexes")
            for name in columns:
                with fake.lock:
                    size = len(fake.blobs[("indexes", name)].data)
                fake.replace("indexes", name, bytes(size))
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


def test_a_failed_flush_keeps_its_rows_and_is_retried(tmp_path: Path) -> None:
    location = f"s3://bucket/retried-{tmp_path.name}"
    prefix = f"retried-{tmp_path.name}/"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
            stored = _index_files(fake, prefix)
            fake.put_error = "AccessDenied"
            with server.connect() as conn:
                conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
                with pytest.raises(psycopg.Error, match="AccessDenied"):
                    conn.execute("VACUUM (FLUSH_TABLE) docs")
                conn.execute("CHECKPOINT")
            assert _matches(server) == [1, 3]
            assert _index_files(fake, prefix) == stored
            fake.put_error = None
            with server.connect() as conn:
                conn.execute("VACUUM (FLUSH_TABLE) docs")
            assert _matches(server) == [1, 3]
            assert len(_index_files(fake, prefix)) > len(stored)
        finally:
            server.stop()
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _matches(again) == [1, 3]
        finally:
            again.stop()


def test_a_view_index_its_store_refuses_fails_and_can_be_created_again(
        tmp_path: Path) -> None:
    location = f"s3://bucket/refused-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            fake.put_error = "AccessDenied"
            with server.connect() as conn:
                with pytest.raises(psycopg.Error,
                                   match="failed to flush index 'notes'"):
                    _create_view_index(conn, tmp_path / "notes.parquet")
            fake.put_error = None
            with server.connect() as conn:
                conn.execute("CREATE INDEX notes ON notes_view "
                             "USING inverted(id, body) TABLESPACE idx")
                assert conn.execute(
                    "SELECT id, body FROM notes ORDER BY id").fetchall() == [
                        (1, "quick brown fox"), (2, "lazy dog")]
        finally:
            server.stop()


def test_a_failed_copy_to_s3_leaves_no_object(tmp_path: Path) -> None:
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute(
                    "CREATE SECRET copies (TYPE s3, KEY_ID 'key', SECRET 'secret', "
                    f"ENDPOINT '{fake.endpoint}', URL_STYLE 'path', USE_SSL false, "
                    "SCOPE 's3://bucket/copies')")
                with pytest.raises(psycopg.Error, match="boom"):
                    conn.execute(
                        "COPY (SELECT CASE WHEN i = 30000 THEN error('boom') "
                        "ELSE repeat('x', 1000) END AS v FROM range(40000) t(i)) "
                        "TO 's3://bucket/copies/failed.csv' (FORMAT csv)")
                assert conn.execute(
                    "SELECT count(*) FROM glob('s3://bucket/copies/*')"
                ).fetchone() == (0,), dict(fake.requests)
            assert fake.keys("bucket", "copies/") == [], dict(fake.requests)
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


@pytest.mark.parametrize("kind", ["s3", "azure"])
def test_clusters_sharing_a_location_keep_apart(tmp_path: Path,
                                                kind: str) -> None:
    with FakeS3() if kind == "s3" else FakeAzure() as fake:
        if kind == "s3":
            bucket, location = "bucket", f"s3://bucket/shared-{tmp_path.name}"
            secret = (f"TYPE s3, KEY_ID 'key', SECRET 'secret', "
                      f"ENDPOINT '{fake.endpoint}', URL_STYLE 'path', USE_SSL false")
            keys = fake.keys
        else:
            bucket, location = "indexes", f"az://indexes/shared-{tmp_path.name}"
            secret = f"TYPE azure, CONNECTION_STRING '{fake.connection_string}'"
            keys = fake.names
        prefix = f"shared-{tmp_path.name}/"

        def files() -> list[str]:
            return [key for key in keys(bucket, prefix) if not key.endswith("/")]

        def clusters() -> set[str]:
            return {key.split("/")[1] for key in files()}

        def rows(server: _Server, table: str) -> list[tuple[int]]:
            with server.connect() as conn:
                return conn.execute(
                    f"SELECT id FROM {table} ORDER BY id").fetchall()

        first = _Server(tmp_path / "first")
        other = _Server(tmp_path / "other")
        try:
            assert first.wait_ready(), first.output()
            assert other.wait_ready(), other.output()
            for server in (first, other):
                with server.connect() as conn:
                    conn.execute(f"CREATE PERSISTENT SECRET store ({secret}, "
                                 f"SCOPE '{location}')")
            with first.connect() as conn:
                conn.execute(f"CREATE TABLESPACE gone LOCATION '{location}'")
                conn.execute("CREATE TABLE dropped (id INT) "
                             " TABLESPACE gone")
                conn.execute("INSERT INTO dropped VALUES (1)")
                conn.execute("VACUUM (FLUSH_TABLE) dropped")
                conn.execute("DROP TABLE dropped")
                conn.execute("DROP TABLESPACE gone")
                conn.execute(f"CREATE TABLESPACE kept LOCATION '{location}'")
                conn.execute("CREATE TABLE kept_rows (id INT) "
                             " TABLESPACE kept")
                conn.execute("INSERT INTO kept_rows VALUES (1), (2)")
                conn.execute("VACUUM (FLUSH_TABLE) kept_rows")
                with pytest.raises(psycopg.errors.ObjectInUse,
                                   match="is already used by tablespace"):
                    conn.execute(
                        f"CREATE TABLESPACE shared LOCATION '{location}'")
            with other.connect() as conn:
                conn.execute(f"CREATE TABLESPACE theirs LOCATION '{location}'")
                conn.execute("CREATE TABLE theirs_rows (id INT) "
                             " TABLESPACE theirs")
                conn.execute("INSERT INTO theirs_rows VALUES (3)")
                conn.execute("VACUUM (FLUSH_TABLE) theirs_rows")
            assert rows(other, "theirs_rows") == [(3,)]
            assert len(clusters()) == 2, keys(bucket)
            with other.connect() as conn:
                conn.execute("DROP TABLE theirs_rows")
                conn.execute("DROP TABLESPACE theirs")
        finally:
            first.stop()
            other.stop()
        assert len(clusters()) == 1, keys(bucket)

        for datadir in ("other", "first"):
            again = _Server(tmp_path / datadir)
            try:
                assert again.wait_ready(), again.output()
            finally:
                again.stop()
        again = _Server(tmp_path / "first")
        try:
            assert again.wait_ready(), again.output()
            assert rows(again, "kept_rows") == [(1,), (2,)]
            with again.connect() as conn:
                conn.execute("DROP TABLE kept_rows")
                conn.execute("DROP TABLESPACE kept")
        finally:
            again.stop()
        assert not files(), keys(bucket)


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


@pytest.mark.parametrize("kind", ["s3", "azure"])
def test_a_lost_marker_response_is_retried(tmp_path: Path, kind: str) -> None:
    with FakeS3() if kind == "s3" else FakeAzure() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            fake.drop["PUT" if kind == "s3" else "PUT_BLOCK_LIST"] = 1
            if kind == "s3":
                _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                               f"s3://bucket/marker-{tmp_path.name}")
            else:
                _create_azure_secret(server, fake, f"az://indexes/marker-{tmp_path.name}")
            with server.connect() as conn:
                conn.execute("CREATE TABLE notes(id INTEGER, body TEXT) "
                             " TABLESPACE idx")
                conn.execute("INSERT INTO notes VALUES (1, 'kept on s3')")
                conn.execute("VACUUM (FLUSH_TABLE) notes")
                assert conn.execute("SELECT body FROM notes").fetchall() == [
                    ("kept on s3",)]
        finally:
            server.stop()
    assert len(fake.dropped) == 1 and fake.dropped[0].endswith("/"), \
        fake.dropped


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


@pytest.mark.parametrize("kind", ["table", "index"])
def test_a_missing_storage_refuses_the_start(tmp_path: Path, kind: str) -> None:
    prefix = f"missing-{tmp_path.name}/"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           f"s3://bucket/{prefix}")
            if kind == "table":
                with server.connect() as conn:
                    conn.execute("CREATE TABLE notes(id INTEGER, body TEXT) "
                                 "TABLESPACE idx")
            else:
                with server.connect() as conn:
                    _create_view_index(conn, tmp_path / "notes.parquet")
        finally:
            server.stop()
        with fake.lock:
            for key in [key for _, key in fake.objects if key.startswith(prefix)]:
                del fake.objects[("bucket", key)]
        output = _refused(datadir)
    assert f"/bucket/{prefix}" in output and "of notes" in output, output
    assert "is missing" in output, output


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
                try:
                    conn.execute("SET sdb_faults = 'crash_before_finish_creation'")
                except psycopg.Error:
                    pytest.skip("built without fault injection")
                with pytest.raises(psycopg.OperationalError):
                    _create_view_index(conn, tmp_path / "notes.parquet")
            server.proc.wait(timeout=60)
            assert _index_files(fake, prefix), fake.keys("bucket")
        finally:
            server.stop()
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            with again.connect() as conn:
                with pytest.raises(psycopg.Error):
                    conn.execute("SELECT * FROM notes")
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


def test_a_crash_during_a_flush_keeps_its_rows_and_leaves_no_files(
        tmp_path: Path) -> None:
    location = f"s3://bucket/flush-{tmp_path.name}"
    prefix = f"flush-{tmp_path.name}/"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            _create_index(server)
            stored = _index_files(fake, prefix)
            fake.stall("PUT", ".idx")

            def flush(conn: psycopg.Connection) -> None:
                conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
                conn.execute("VACUUM (FLUSH_TABLE) docs")

            _kill_while_stalled(server, fake, flush)
        finally:
            server.stop()
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _matches(again) == [1, 3]
            assert _eventually(lambda: _index_files(fake, prefix) == stored), \
                fake.keys("bucket", prefix)
        finally:
            again.stop()


def test_a_crash_during_an_index_build_leaves_the_files_of_its_table(
        tmp_path: Path) -> None:
    location = f"s3://bucket/upload-{tmp_path.name}"
    prefix = f"upload-{tmp_path.name}/"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _large_table(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                         location)
            stored = _index_files(fake, prefix)
            fake.stall("PUT_PART", prefix)
            _kill_while_stalled(server, fake,
                                lambda conn: conn.execute(CREATE_INDEX))
        finally:
            server.stop()
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _eventually(lambda: _index_files(fake, prefix) == stored), \
                fake.keys("bucket", prefix)
            with again.connect() as conn:
                with pytest.raises(psycopg.Error):
                    conn.execute("SELECT * FROM docs_body")
                conn.execute(CREATE_INDEX)
            assert _count_fox(again) == 300000
        finally:
            again.stop()


@pytest.mark.parametrize("kind", ["table", "index"])
def test_an_orphan_object_is_removed_after_a_crash(tmp_path: Path,
                                                   kind: str) -> None:
    location = f"s3://bucket/orphan-{tmp_path.name}"
    prefix = f"orphan-{tmp_path.name}/"
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            with server.connect() as conn:
                if kind == "table":
                    conn.execute("CREATE TABLE notes (id INT, body TEXT) "
                                 "TABLESPACE idx")
                    conn.execute("INSERT INTO notes VALUES (1, 'quick brown fox')")
                    conn.execute("VACUUM (FLUSH_TABLE) notes")
                else:
                    _create_view_index(conn, tmp_path / "notes.parquet")
            server.proc.kill()
        finally:
            server.stop()
        stored = _index_files(fake, prefix)
        assert stored, fake.keys("bucket")
        orphan = stored[0].rsplit("/", 1)[0] + "/00000000000000ff.doc"
        fake.replace("bucket", orphan, b"written by an upload no segment names")
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            with again.connect() as conn:
                assert conn.execute(
                    "SELECT id FROM notes WHERE body = 'quick brown fox'"
                ).fetchall() == [(1,)]
            assert _eventually(lambda: _index_files(fake, prefix) == stored), \
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


def test_a_copy_to_a_dfs_file_reads_back(tmp_path: Path) -> None:
    target = f"abfss://files/copied-{tmp_path.name}/numbers.csv"
    with FakeAzure() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute("SET GLOBAL azure_write_block_size = 262144")
                conn.execute("CREATE PERSISTENT SECRET dfs_store (TYPE azure, "
                             f"CONNECTION_STRING '{fake.connection_string}', SCOPE 'abfss://files')")
                conn.execute(f"COPY (SELECT i FROM range(100000) t(i)) TO '{target}' (FORMAT csv, HEADER)")
                assert conn.execute(f"SELECT count(*), sum(i) FROM read_csv('{target}')").fetchone() == (
                    100000, 4999950000)
        finally:
            server.stop()
    assert fake.requests["APPEND"] > 1 and fake.requests["FLUSH"] > 0, fake.requests


def test_a_table_in_a_dfs_tablespace_survives_a_restart(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    with FakeAzure() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _create_azure_secret(server, fake, f"abfss://indexes/dfs-{tmp_path.name}")
            _fill_docs(server, 2000)
            with server.connect() as conn:
                conn.execute(CREATE_INDEX)
            assert _count_fox(server) == 2000
        finally:
            server.stop()
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _count_fox(again) == 2000
            with again.connect() as conn:
                conn.execute("DROP TABLE docs")
                conn.execute("DROP TABLESPACE idx")
        finally:
            again.stop()
        assert not [name for name in fake.names("indexes", f"dfs-{tmp_path.name}/")
                    if not name.endswith("/")], fake.names("indexes")


@pytest.mark.parametrize("kind", ["s3", "azure"])
def test_a_restart_lists_each_directory_one_level_deep(tmp_path: Path, kind: str) -> None:
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
            server.proc.kill()
        finally:
            server.stop()
        markers = [key for key in keys(bucket, f"{prefix}/") if key.endswith("/")]
        assert len(markers) == 4, keys(bucket)
        store = max(markers, key=lambda key: key.count("/")).removesuffix("/")
        for i in range(6000):
            fake.replace(bucket, f"{store}/archive/{i}", b"")
        orphan = f"{store}/00000000000000ff.doc"
        fake.replace(bucket, orphan, b"written by an upload no segment names")
        listings = fake.requests["LIST"]
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            assert _matches(again) == [1]
            assert _eventually(lambda: orphan not in keys(bucket, f"{store}/")), \
                keys(bucket, f"{store}/")
            assert fake.requests["LIST"] - listings == 1, fake.requests
            objects = len(keys(bucket, f"{store}/"))
            page = 1000 if kind == "s3" else 5000
            listings = fake.requests["LIST"]
            with again.connect() as conn:
                assert conn.execute("SELECT pg_tablespace_size('idx')").fetchone()[0] > 0
            assert fake.requests["LIST"] - listings == -(-objects // page), fake.requests
        finally:
            again.stop()


def test_a_restart_opens_its_tables_at_once(tmp_path: Path) -> None:
    each = 3
    datadir = tmp_path / "data"
    with FakeS3() as fake:
        server = _Server(datadir)
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           f"s3://bucket/opened-{tmp_path.name}")
            with server.connect() as conn:
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower')")
                for i in range(each):
                    conn.execute(f"CREATE TABLE docs{i}(id INTEGER, body TEXT) "
                                 "TABLESPACE idx")
                    conn.execute(f"CREATE INDEX docs{i}_body ON docs{i} "
                                 "USING inverted(body words)")
                    conn.execute(f"INSERT INTO docs{i} VALUES ({i}, 'quick brown fox')")
                    conn.execute(f"VACUUM (FLUSH_TABLE) docs{i}")
                    conn.execute(f"CREATE TABLE notes{i}(id INTEGER, body TEXT) "
                                 "TABLESPACE idx")
                    conn.execute(f"INSERT INTO notes{i} VALUES ({i}, 'kept on s3')")
                    conn.execute(f"VACUUM (FLUSH_TABLE) notes{i}")
        finally:
            server.stop()
        fake.latency = 0.5
        fake.log = []
        fake.trace = []
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            log, fake.log = fake.log, None
            trace, fake.trace = fake.trace, None
            fake.latency = 0
            with again.connect() as conn:
                for i in range(each):
                    assert conn.execute(
                        f"SELECT id FROM docs{i}_body WHERE body @@ 'fox'"
                    ).fetchall() == [(i,)]
                    assert conn.execute(
                        f"SELECT id, body FROM notes{i}"
                    ).fetchall() == [(i, "kept on s3")]
        finally:
            again.stop()
    assert not [entry for entry in log if entry["key"].endswith("/")], log
    assert not [entry for entry in trace if entry["method"] == "LIST"], trace
    checks = [entry for entry in trace
              if entry["method"] == "HEAD" and entry["key"].endswith("/")]
    assert len(checks) == 1, trace
    opens: dict[str, dict] = {}
    for entry in sorted(log, key=lambda entry: entry["began"]):
        opens.setdefault(entry["key"].rsplit("/", 1)[1].split(".", 1)[0], entry)
    assert len(opens) == 2 * each, log
    assert max(entry["began"] for entry in opens.values()) < min(
        entry["ended"] for entry in opens.values()), opens


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
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT) "
                             "WITH (compaction_interval = 0) TABLESPACE idx")
                conn.execute(CREATE_INDEX)
                conn.execute(
                    "INSERT INTO docs SELECT i, concat_ws(' ', 'fox', "
                    "md5(i::VARCHAR)) FROM range(20000) t(i)")
                conn.execute("VACUUM (FLUSH_TABLE) docs")
            assert _count_fox(server) == 20000

            fake.session_tokens = {"first", "second"}
            with server.connect() as conn:
                conn.execute(_token_secret(store, location, "second"))
            fake.session_tokens = {"second"}

            with server.connect() as conn:
                conn.execute(
                    "INSERT INTO docs SELECT i, 'fox' FROM range(20000, 20100) t(i)")
                conn.execute("VACUUM (FLUSH_TABLE) docs")
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
            assert _count_fox(server) == 20100, server.output()
            assert fake.stats()["requests"].get("EXPIRED", 0) > 0
        finally:
            server.stop()


SECRET_LOOKUP_FAULT = "pause_secret_lookup_outside_sessions"


@pytest.mark.faults
def test_a_secret_replaced_while_a_removal_outside_sessions_reads_it(
        tmp_path: Path) -> None:
    location = f"s3://bucket/replaced-{tmp_path.name}"
    fault = SECRET_LOOKUP_FAULT
    with FakeS3() as fake:
        store = _Store(fake.endpoint, "key", "secret", "bucket")
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, store, location)
            with server.connect() as conn:
                conn.execute("CREATE TABLE gone(id INTEGER) "
                             "WITH (compaction_interval = 0) TABLESPACE idx")
                conn.execute("INSERT INTO gone SELECT i FROM range(1000) t(i)")
                conn.execute("VACUUM (FLUSH_TABLE) gone")
                written = {key for key in fake.keys("bucket", f"replaced-{tmp_path.name}/")
                           if "/store/" in key and not key.endswith("/")}
                assert written, fake.keys("bucket")
                conn.execute(f"SET sdb_faults = '{fault}'")
                conn.execute("DROP TABLE gone")

                def parked() -> bool:
                    try:
                        conn.execute(f"SET sdb_faults = '-{fault}:parked'")
                        return True
                    except psycopg.Error:
                        return False

                assert _eventually(parked), server.output()
                conn.execute(
                    "CREATE OR REPLACE PERSISTENT SECRET index_store (TYPE s3, "
                    f"KEY_ID '{store.key_id}', SECRET '{store.secret}', "
                    f"ENDPOINT '{store.endpoint}', URL_STYLE 'path', USE_SSL false, "
                    f"SCOPE '{location}')")
                for _ in range(3):
                    with server.connect() as other:
                        other.execute("SELECT 1")
                time.sleep(0.5)
                conn.execute(f"SET sdb_faults = '{fault}:release'")
            assert _eventually(lambda: not written & set(
                fake.keys("bucket", f"replaced-{tmp_path.name}/"))), server.output()
            with server.connect() as conn:
                assert conn.execute("SELECT 1").fetchone() == (1,)
        finally:
            server.stop()


def test_a_restart_opens_its_tables_with_the_replaced_secret(
        tmp_path: Path) -> None:
    location = f"s3://bucket/restarted-{tmp_path.name}"
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
                conn.execute("CREATE TABLE gone(id INTEGER) TABLESPACE idx")
                conn.execute("INSERT INTO gone VALUES (1)")
                conn.execute("VACUUM (FLUSH_TABLE) gone")
                conn.execute("CREATE TABLE notes(id INTEGER, body TEXT) "
                             "TABLESPACE idx")
                conn.execute("INSERT INTO notes VALUES (1, 'kept on s3')")
                conn.execute("VACUUM (FLUSH_TABLE) notes")
                conn.execute("DROP TABLE gone")
                fake.session_tokens = {"first", "second"}
                conn.execute(_token_secret(store, location, "second"))
        finally:
            server.stop()
        fake.session_tokens = {"second"}
        fake.reset()
        again = _Server(datadir)
        try:
            assert again.wait_ready(), again.output()
            with again.connect() as conn:
                assert conn.execute("SELECT body FROM notes").fetchall() == [
                    ("kept on s3",)]
        finally:
            again.stop()
        assert fake.stats()["requests"].get("EXPIRED", 0) == 0, fake.requests


def test_a_flush_with_an_expired_token_works_once_the_secret_is_replaced(
        tmp_path: Path) -> None:
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
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT) "
                             "WITH (compaction_interval = 0) TABLESPACE idx")
                conn.execute(CREATE_INDEX)
                conn.execute(
                    "INSERT INTO docs SELECT i, 'fox' FROM range(1000) t(i)")
                conn.execute("VACUUM (FLUSH_TABLE) docs")
            assert _count_fox(server) == 1000

            fake.session_tokens = {"second"}
            with server.connect() as conn:
                conn.execute(
                    "INSERT INTO docs SELECT i, 'fox' FROM range(1000, 1100) t(i)")
                with pytest.raises(psycopg.Error, match="ExpiredToken"):
                    conn.execute("VACUUM (FLUSH_TABLE) docs")
            assert fake.stats()["requests"].get("EXPIRED", 0) > 0
            assert _count_fox(server) == 1100
            with server.connect() as conn:
                conn.execute(_token_secret(store, location, "second"))
                conn.execute("VACUUM (FLUSH_TABLE) docs")
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
            assert _count_fox(server) == 1100
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
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT) "
                             "WITH (compaction_interval = 0) TABLESPACE idx")
                conn.execute(CREATE_INDEX)
                conn.execute(
                    "INSERT INTO docs SELECT i, concat_ws(' ', 'fox', "
                    "md5(i::VARCHAR)) FROM range(20000) t(i)")
                conn.execute("VACUUM (FLUSH_TABLE) docs")
            assert _count_fox(server) == 20000

            fake.sas_signatures = {"first", "second"}
            with server.connect() as conn:
                conn.execute(_sas_secret(fake, location, "second"))
            fake.sas_signatures = {"second"}

            with server.connect() as conn:
                conn.execute(
                    "INSERT INTO docs SELECT i, 'fox' FROM range(20000, 20100) t(i)")
                conn.execute("VACUUM (FLUSH_TABLE) docs")
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
            assert _count_fox(server) == 20100, server.output()
            assert fake.requests["FORBIDDEN"] > 0, fake.requests
        finally:
            server.stop()


@pytest.mark.parametrize("target,kind", [
    ("az://copies/{}/rows.csv", "PUT_BLOCK"),
    ("az://copies/{}/rows.csv", "PUT_BLOCK_LIST"),
    ("abfss://files/{}/rows.csv", "APPEND"),
    ("abfss://files/{}/rows.csv", "FLUSH"),
])
def test_a_rotated_azure_secret_reaches_a_write_in_flight(
        tmp_path: Path, target: str, kind: str) -> None:
    target = target.format(f"rotated-{tmp_path.name}")
    scope = target.rsplit("/", 2)[0]
    with FakeAzure() as fake:
        fake.sas_signatures = {"first"}
        fake.unheld["PUT_BLOCK_LIST"] = 1
        server = _Server(tmp_path / "data")
        release = threading.Event()
        try:
            assert server.wait_ready(), server.output()
            with server.connect() as conn:
                conn.execute(_sas_secret(fake, scope, "first"))
            fake.hold[kind] = release
            errors: list[BaseException] = []

            def copy() -> None:
                try:
                    with server.connect() as conn:
                        conn.execute(f"COPY (SELECT i FROM range(100000) t(i)) TO '{target}' "
                                     "(FORMAT csv, HEADER)")
                except BaseException as error:
                    errors.append(error)

            writer = threading.Thread(target=copy)
            writer.start()
            assert _eventually(lambda: fake.requests["HELD"] > 0), server.output()
            fake.sas_signatures = {"first", "second"}
            with server.connect() as conn:
                conn.execute(_sas_secret(fake, scope, "second"))
            fake.sas_signatures = {"second"}
            del fake.hold[kind]
            release.set()
            writer.join(60)
            assert not writer.is_alive() and not errors, (errors, server.output())
            with server.connect() as conn:
                assert conn.execute(
                    f"SELECT count(*), sum(i) FROM read_csv('{target}')").fetchone() == (
                        100000, 4999950000)
            assert fake.requests["FORBIDDEN"] > 0, fake.requests
        finally:
            release.set()
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
                conn.execute("VACUUM (FLUSH_TABLE) docs")
            assert _matches(server) == [1, 3]
            with server.connect() as conn:
                conn.execute("DROP INDEX docs_body")
                conn.execute(CREATE_INDEX)
            assert _matches(server) == [1, 3]
        finally:
            server.stop()
