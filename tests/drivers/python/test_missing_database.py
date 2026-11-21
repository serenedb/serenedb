"""What boot does with a data directory that lost files.

A table or index whose files are missing refuses to start, since coming
back without its data would turn a lost file into silent data loss. A missing
catalog log beside the index files refuses to start too, and so does a data
directory of an earlier layout, which boot leaves as it found it.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

import psycopg
import pytest
from serened import listens_on, reserve_port  # type: ignore[import-untyped]

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


class _Server:
    def __init__(self, datadir: Path, *flags: str) -> None:
        self.reserved = reserve_port()
        self.port = self.reserved.getsockname()[1]
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
            if listens_on(self.proc.pid, [self.port]):
                try:
                    self.connect().close()
                    return True
                except psycopg.OperationalError:
                    pass
            time.sleep(0.2)
        return False

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=60)
        self.reserved.close()

    def output(self) -> str:
        return self.log.read_text()


def test_missing_catalog_log(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute("CREATE TABLE kept(a INTEGER)")
            conn.execute("INSERT INTO kept VALUES (1)")
            conn.execute("VACUUM (FLUSH_TABLE) kept")
    finally:
        server.stop()
    store = datadir / "engine_v1" / "store"
    files = sorted(store.iterdir())
    assert files
    shutil.rmtree(datadir / "engine_v1" / "wal")

    refused = _Server(datadir)
    try:
        assert not refused.wait_ready(timeout=30)
        assert refused.proc.wait(timeout=30) != 0
        assert "holds no catalog log" in refused.output()
    finally:
        refused.stop()
    assert sorted(store.iterdir()) == files


def test_missing_segment_file(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute(
                "CREATE TABLE docs(id BIGINT PRIMARY KEY, body TEXT) ")
            conn.execute("INSERT INTO docs VALUES (1, 'kept')")
            conn.execute("VACUUM (FLUSH_TABLE) docs")
            oid = conn.execute(
                "SELECT table_oid FROM sdb_tables() "
                "WHERE table_name = 'docs'").fetchone()[0]
    finally:
        server.stop()
    lost = min((datadir / "engine_v1" / "store").iterdir())
    lost.unlink()

    refused = _Server(datadir)
    try:
        assert not refused.wait_ready(timeout=30)
        assert refused.proc.wait(timeout=30) != 0
        assert f"cannot open the files of relation {oid}" in refused.output()
        assert str(lost) in refused.output()
    finally:
        refused.stop()


def _contents(root: Path) -> dict[str, bytes]:
    return {str(path.relative_to(root)): path.read_bytes()
            for path in sorted(root.rglob("*")) if path.is_file()}


def test_a_data_directory_of_an_earlier_layout_is_refused_untouched(
        tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with server.connect() as conn:
            conn.execute("CREATE TABLE docs(id BIGINT PRIMARY KEY, body TEXT)")
            conn.execute("INSERT INTO docs VALUES (1, 'kept')")
            conn.execute("VACUUM (FLUSH_TABLE) docs")
    finally:
        server.stop()
    (datadir / "engine_v1" / "wal" / "snapshot").write_bytes(b"")
    engine = _contents(datadir / "engine_v1")

    refused = _Server(datadir)
    try:
        assert not refused.wait_ready(timeout=30)
        assert refused.proc.wait(timeout=30) != 0
        assert "was written with an earlier layout of the data directory" in \
            refused.output()
    finally:
        refused.stop()
    assert _contents(datadir / "engine_v1") == engine
