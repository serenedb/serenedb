from __future__ import annotations

import os
import platform
import shutil
import socket
import subprocess
import time
from pathlib import Path

import psycopg
import pytest
from serened import listens_on  # type: ignore[import-untyped]

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
    def __init__(self, datadir: Path, listen: str | None = None) -> None:
        self.port = _free_port()
        if listen is None:
            listen = f"postgres://127.0.0.1:{self.port}"
        self.log = datadir.parent / f"serened-{self.port}.log"
        with self.log.open("w") as out:
            self.proc = subprocess.Popen(
                [SERENED_BIN, str(datadir), f"--listen={listen}"],
                stdout=out, stderr=subprocess.STDOUT)

    def wait_ready(self, timeout: float = 60.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                return False
            if listens_on(self.proc.pid, [self.port]):
                try:
                    psycopg.connect(host="127.0.0.1", port=self.port,
                                    user="postgres", dbname="postgres").close()
                    return True
                except psycopg.OperationalError:
                    pass
            time.sleep(0.2)
        return False

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=60)

    def output(self) -> str:
        return self.log.read_text()


def _run_until_ready(datadir: Path) -> _Server:
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
    finally:
        server.stop()
    return server


def test_info_timeline(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    first = _run_until_ready(datadir)
    for line in ("starting SereneDB ", "Created database directory: ",
                 f"listening on postgres://127.0.0.1:{first.port}",
                 "SereneDB is ready for business",
                 "received shutdown signal", "SereneDB is shut down"):
        assert line in first.output(), first.output()

    second = _run_until_ready(datadir)
    assert "Created database directory" not in second.output(), second.output()
    assert "search index recovery" not in second.output(), second.output()


def test_recovery_summary_after_a_crash(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with psycopg.connect(host="127.0.0.1", port=server.port,
                             user="postgres", dbname="postgres",
                             autocommit=True) as conn:
            conn.execute("CREATE TABLE pudge (id INT PRIMARY KEY, body TEXT)")
            conn.execute(
                "CREATE INDEX pudge_idx ON pudge USING inverted(id, body)")
            conn.execute("INSERT INTO pudge SELECT i, 'hook ' || i "
                         "FROM range(100) t(i)")
    finally:
        server.proc.kill()
        server.proc.wait(timeout=60)

    restarted = _run_until_ready(datadir)
    assert ("search index recovery: replayed the WAL into 1 of 1 inverted "
            "index(es)" in restarted.output()), restarted.output()


def test_search_table_recovery_summary_after_a_crash(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with psycopg.connect(host="127.0.0.1", port=server.port,
                             user="postgres", dbname="postgres",
                             autocommit=True) as conn:
            conn.execute("CREATE TABLE tusk (id INT, body TEXT) "
                         "WITH (storage = 'search', refresh_interval = 0)")
            conn.execute("CREATE TABLE idle (id INT) "
                         "WITH (storage = 'search', refresh_interval = 0)")
            conn.execute("INSERT INTO tusk SELECT i, 'ice ' || i "
                         "FROM range(100) t(i)")
    finally:
        server.proc.kill()
        server.proc.wait(timeout=60)

    restarted = _run_until_ready(datadir)
    assert ("search index recovery: replayed the WAL into 1 of 2 search "
            "table(s) in " in restarted.output()), restarted.output()


def test_terminate_while_starting(tmp_path: Path) -> None:
    server = _Server(tmp_path / "data")
    deadline = time.monotonic() + 60
    while "Created database directory: " not in server.output():
        assert server.proc.poll() is None, server.output()
        assert time.monotonic() < deadline, server.output()
        time.sleep(0.005)
    server.proc.terminate()
    assert server.proc.wait(timeout=60) == 0, server.output()
    assert "SIGTERM received" not in server.output(), server.output()
    assert "SereneDB is shut down" in server.output(), server.output()


def test_second_instance_on_the_same_datadir(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    first = _Server(datadir)
    try:
        assert first.wait_ready(), first.output()
        second = _Server(datadir)
        try:
            assert second.proc.wait(timeout=60) != 0, second.output()
            assert (f"database is locked by process {first.proc.pid}"
                    in second.output()), second.output()
        finally:
            second.stop()
    finally:
        first.stop()


def test_unknown_unix_socket_group(tmp_path: Path) -> None:
    server = _Server(tmp_path / "data",
                     f"postgres://{tmp_path / 'sdb.sock'}"
                     "?group=sdb_no_such_group")
    try:
        assert server.proc.wait(timeout=60) != 0, server.output()
        assert ("cannot find group 'sdb_no_such_group'"
                in server.output()), server.output()
    finally:
        server.stop()


def test_cleartext_password_on_a_unix_socket(tmp_path: Path) -> None:
    sockets = tmp_path / "s"
    sockets.mkdir()
    server = _Server(tmp_path / "data", f"postgres://{sockets}?port=5432")

    def connect(**kwargs) -> psycopg.Connection:
        return psycopg.connect(host=str(sockets), port=5432, dbname="postgres",
                               **kwargs)

    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                admin = connect(user="postgres", autocommit=True)
                break
            except psycopg.OperationalError:
                assert (server.proc.poll() is None
                        and time.monotonic() < deadline), server.output()
                time.sleep(0.2)
        with admin:
            admin.execute("CREATE ROLE cleartext_login LOGIN PASSWORD 'secret'")
            admin.execute("SET hba = 'local all cleartext_login password'")
        try:
            with connect(user="cleartext_login", password="secret") as conn:
                assert conn.execute("SELECT current_user").fetchone() == (
                    "cleartext_login",)
        except psycopg.OperationalError as error:
            pytest.fail(f"{error}\n{server.output()}")
    finally:
        server.stop()


def _qemu_x86_64() -> str | None:
    if platform.machine() != "x86_64":
        return None
    return shutil.which("qemu-x86_64") or shutil.which("qemu-x86_64-static")


@pytest.mark.skipif(_qemu_x86_64() is None,
                    reason="needs qemu-x86_64 on an x86-64 host")
def test_cpu_without_the_build_baseline(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    result = subprocess.run(
        [_qemu_x86_64(), "-cpu", "Haswell-noTSX,-f16c", SERENED_BIN,
         str(datadir)],
        capture_output=True, text=True, timeout=60)
    assert result.returncode == 1, result
    assert result.stderr == "serened cannot run on this CPU: it lacks F16C\n", (
        result.stderr)
    assert not datadir.exists()
