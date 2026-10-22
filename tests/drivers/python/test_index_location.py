"""Where search index files live: --index_location.

A cluster keeps its index files in the location it was first started with,
the data directory by default. A later start with another location, or
without the one it was created with, is refused instead of booting with its
indexes missing.
"""

from __future__ import annotations

import collections
import os
import socket
import subprocess
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
    def __init__(self, datadir: Path, *flags: str) -> None:
        self.port = _free_port()
        self.log = datadir.parent / f"serened-{self.port}.log"
        with self.log.open("w") as out:
            self.proc = subprocess.Popen(
                [SERENED_BIN, str(datadir),
                 f"--listen=postgres://127.0.0.1:{self.port}", *flags],
                stdout=out, stderr=subprocess.STDOUT)

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

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=60)

    def output(self) -> str:
        return self.log.read_text()


def _create_index(server: _Server) -> None:
    with server.connect() as conn:
        conn.execute(
            "CREATE TEXT SEARCH DICTIONARY words AS "
            "split_by_non_alpha(case := 'lower')")
        conn.execute("CREATE TABLE docs(id INTEGER, body TEXT)")
        conn.execute(
            "INSERT INTO docs VALUES (1, 'quick brown fox'), (2, 'lazy dog')")
        conn.execute("CREATE INDEX docs_body ON docs USING inverted(body words)")


def _matches(server: _Server) -> list[int]:
    with server.connect() as conn:
        rows = conn.execute(
            "SELECT id FROM docs_body WHERE body @@ 'fox' ORDER BY id"
        ).fetchall()
    return [row[0] for row in rows]


def _refused(datadir: Path, *flags: str) -> str:
    server = _Server(datadir, *flags)
    try:
        assert not server.wait_ready(timeout=30)
        assert server.proc.wait(timeout=30) != 0
        return server.output()
    finally:
        server.stop()


def test_index_files_in_their_own_directory(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    location = tmp_path / "indexes"
    flag = f"--index_location={location}"

    server = _Server(datadir, flag)
    try:
        assert server.wait_ready(), server.output()
        _create_index(server)
        assert _matches(server) == [1]
    finally:
        server.stop()
    assert any(path.is_file() for path in location.rglob("*"))
    assert not any(
        path.is_dir() and any(path.iterdir())
        for database in (datadir / "engine_v1").glob("[0-9]*")
        for path in database.glob("[0-9]*"))

    again = _Server(datadir, flag)
    try:
        assert again.wait_ready(), again.output()
        assert _matches(again) == [1]
    finally:
        again.stop()

    output = _refused(datadir)
    assert f"live in '{location}'" in output
    assert f"--index_location={location}" in output

    output = _refused(datadir, f"--index_location={tmp_path / 'elsewhere'}")
    assert f"live in '{location}'" in output


def test_existing_cluster_keeps_the_data_directory(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        _create_index(server)
    finally:
        server.stop()

    output = _refused(datadir, f"--index_location={tmp_path / 'indexes'}")
    assert "start it without --index_location" in output

    again = _Server(datadir)
    try:
        assert again.wait_ready(), again.output()
        assert _matches(again) == [1]
    finally:
        again.stop()


@pytest.mark.parametrize("flags", [(), ("--index_location=indexes",)],
                         ids=["default", "relative"])
def test_moved_data_directory_keeps_its_indexes(
        tmp_path: Path, flags: tuple[str, ...]) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir, *flags)
    try:
        assert server.wait_ready(), server.output()
        _create_index(server)
    finally:
        server.stop()

    moved = tmp_path / "moved"
    datadir.rename(moved)
    again = _Server(moved, *flags)
    try:
        assert again.wait_ready(), again.output()
        assert _matches(again) == [1]
    finally:
        again.stop()


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


def _create_secret(server: _Server, store: _Store, location: str) -> None:
    with server.connect() as conn:
        conn.execute(
            "CREATE PERSISTENT SECRET index_store (TYPE s3, "
            f"KEY_ID '{store.key_id}', SECRET '{store.secret}', "
            f"ENDPOINT '{store.endpoint}', URL_STYLE 'path', USE_SSL false, "
            f"SCOPE '{location}')")


def test_index_files_on_s3(tmp_path: Path, s3_store: _Store) -> None:
    location = f"s3://{s3_store.bucket}/index-location-{tmp_path.name}"
    datadir = tmp_path / "data"
    flag = f"--index_location={location}"

    server = _Server(datadir, flag)
    try:
        assert server.wait_ready(), server.output()
        _create_secret(server, s3_store, location)
        _create_index(server)
        assert _matches(server) == [1]
        with server.connect() as conn:
            conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
            conn.execute("VACUUM (REFRESH_TABLE) docs")
        assert _matches(server) == [1, 3]
    finally:
        server.stop()

    again = _Server(datadir, flag)
    try:
        assert again.wait_ready(), again.output()
        assert _matches(again) == [1, 3]
        with again.connect() as conn:
            conn.execute("DROP INDEX docs_body")
    finally:
        again.stop()
    if s3_store.fake:
        assert not [key for key in s3_store.fake.keys(s3_store.bucket)
                    if not key.endswith(".storage")], s3_store.fake.keys(s3_store.bucket)


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
    flag = f"--index_location={location}"
    fake = azure_container.fake
    server = _Server(datadir, flag)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute(
                "CREATE PERSISTENT SECRET index_store (TYPE azure, "
                f"CONNECTION_STRING '{azure_container.connection_string}', "
                f"SCOPE '{location}')")
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

    again = _Server(datadir, flag)
    try:
        assert again.wait_ready(), again.output()
        assert _matches(again) == [1, 3]
        with again.connect() as conn:
            conn.execute("DROP INDEX docs_body")
    finally:
        again.stop()
    if fake:
        prefix = f"cluster-{tmp_path.name}/"
        assert not [name for name in fake.names(azure_container.name, prefix)
                    if not name.endswith(".storage")], fake.names(azure_container.name)


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
        server = _Server(tmp_path / "data", f"--index_location={location}")
        try:
            assert server.wait_ready(), server.output()
            _large_table(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                         location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE INDEX docs_body ON docs USING inverted(body words)")
            assert _count_fox(server) == 300000
        finally:
            server.stop()
    assert fake.requests["PUT_PART"] > 2, fake.requests
    assert fake.max_parts_in_flight > 1, fake.requests
    assert fake.requests["CHECKSUM"] > fake.requests["COMPLETE_UPLOAD"] > 0, fake.requests


def test_a_trickling_read_fails_by_its_deadline(tmp_path: Path) -> None:
    location = f"s3://bucket/trickle-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data", f"--index_location={location}")
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
                    "CREATE INDEX docs_body ON docs USING inverted(body words)")
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
        server = _Server(tmp_path / "data", f"--index_location={location}")
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
        server = _Server(tmp_path / "data", f"--index_location={location}")
        try:
            assert server.wait_ready(), server.output()
            _large_table(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                         location)
            fake.failing_parts.add(2)
            with server.connect() as conn:
                conn.execute("SET GLOBAL http_retries = 0")
                with pytest.raises(psycopg.Error):
                    conn.execute(
                        "CREATE INDEX docs_body ON docs USING inverted(body words)")
            assert fake.requests["ABORT_UPLOAD"] > 0, fake.requests
            assert not fake.uploads
            fake.failing_parts.clear()
            with server.connect() as conn:
                conn.execute(
                    "CREATE INDEX docs_body ON docs USING inverted(body words)")
            assert _count_fox(server) == 300000
        finally:
            server.stop()


COPY_FAULT = "object_store_copy_fails"


def test_failed_commit_is_retried(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir, "--index_location=sim://bucket/retried")
    try:
        if not server.wait_ready():
            if "sim://" in server.output():
                pytest.skip("the simulated object store exists in dev builds")
            pytest.fail(server.output())
        _create_index(server)
        with server.connect() as conn:
            try:
                conn.execute(f"SET sdb_faults = '{COPY_FAULT}'")
            except psycopg.Error:
                pytest.skip("built without fault injection")
            conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
            with pytest.raises(psycopg.Error):
                conn.execute("VACUUM (REFRESH_TABLE) docs")
        assert _matches(server) == [1]
        with server.connect() as conn:
            conn.execute(f"SET sdb_faults = '-{COPY_FAULT}'")
            conn.execute("VACUUM (REFRESH_TABLE) docs")
        assert _matches(server) == [1, 3]
    finally:
        server.stop()


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
        server = _Server(tmp_path / "data", f"--index_location={location}")
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
                    "WITH (compaction_interval = 0)")
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
        server = _Server(tmp_path / "data", f"--index_location={location}")
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
                    "WITH (compaction_interval = 0)")
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


def test_index_files_on_an_object_store(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir, "--index_location=sim://bucket/cluster")
    try:
        if not server.wait_ready():
            if "sim://" in server.output():
                pytest.skip("the simulated object store exists in dev builds")
            pytest.fail(server.output())
        _create_index(server)
        assert _matches(server) == [1]
        with server.connect() as conn:
            conn.execute("INSERT INTO docs VALUES (3, 'fox again')")
            conn.execute("VACUUM (REFRESH_TABLE) docs")
        assert _matches(server) == [1, 3]
        with server.connect() as conn:
            conn.execute("DROP INDEX docs_body")
            conn.execute(
                "CREATE INDEX docs_body ON docs USING inverted(body words)")
        assert _matches(server) == [1, 3]
    finally:
        server.stop()
