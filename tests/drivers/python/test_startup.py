from __future__ import annotations

import base64
import os
import platform
import shutil
import socket
import struct
import subprocess
import tempfile
import time
from collections.abc import Iterator
from decimal import Decimal
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
    def __init__(self, datadir: Path, listen: str | None = None,
                 args: tuple[str, ...] = (),
                 env: dict[str, str] | None = None,
                 datadir_flag: bool = False) -> None:
        self.reserved = reserve_port()
        self.port = self.reserved.getsockname()[1]
        if listen is None:
            listen = f"postgres://127.0.0.1:{self.port}"
        self.log = datadir.parent / f"serened-{self.port}.log"
        where = f"--server_directory={datadir}" if datadir_flag else str(datadir)
        with self.log.open("w") as out:
            self.proc = subprocess.Popen(
                [SERENED_BIN, where, f"--listen={listen}", *args],
                stdout=out, stderr=subprocess.STDOUT,
                env={**os.environ, **env} if env else None)

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
        self.reserved.close()

    def output(self) -> str:
        return self.log.read_text()


@pytest.fixture
def socket_dir() -> Iterator[Path]:
    path = Path(tempfile.mkdtemp(prefix="sdb-", dir="/tmp"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


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
    assert "replayed the log" not in second.output(), second.output()


def test_trace_logging_shuts_down_cleanly(tmp_path: Path) -> None:
    server = _Server(tmp_path / "data", args=("--log_level=trace",))
    try:
        assert server.wait_ready(), server.output()
    finally:
        server.stop()
    assert server.proc.returncode == 0, server.output()


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
    assert ("recovery: replayed the log into 1 of 1 table(s) in "
            in restarted.output()), restarted.output()


def test_search_table_recovery_summary_after_a_crash(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with psycopg.connect(host="127.0.0.1", port=server.port,
                             user="postgres", dbname="postgres",
                             autocommit=True) as conn:
            conn.execute("CREATE TABLE tusk (id INT, body TEXT) ")
            conn.execute("CREATE TABLE idle (id INT) ")
            conn.execute("INSERT INTO tusk SELECT i, 'ice ' || i "
                         "FROM range(100) t(i)")
    finally:
        server.proc.kill()
        server.proc.wait(timeout=60)

    restarted = _run_until_ready(datadir)
    assert ("recovery: replayed the log into 1 of 2 table(s) in "
            in restarted.output()), restarted.output()


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
            assert (f"the data directory is in use: '{datadir}/PG_VERSION' "
                    "is locked by a running server or a read-only attachment"
                    in second.output()), second.output()
        finally:
            second.stop()
    finally:
        first.stop()


SEARCH_FAULT = "search_start_throws"
SUBSCRIPTIONS_FAULT = "subscriptions_start_throws"
JOBS_FAULT = "jobs_start_throws"
LISTENERS_FAULT = "listeners_start_throws"


@pytest.mark.faults
@pytest.mark.parametrize("fault", [SEARCH_FAULT, SUBSCRIPTIONS_FAULT,
                                   JOBS_FAULT, LISTENERS_FAULT])
def test_a_component_failing_to_start_stops_the_ones_before_it(
        tmp_path: Path, fault: str) -> None:
    server = _Server(tmp_path / "data", env={"SDB_FAULTS": fault})
    try:
        assert server.proc.wait(timeout=60) == 1, server.output()
    finally:
        server.stop()
    output = server.output()
    assert "serened terminated because of an exception" in output, output
    assert "SereneDB is shut down" in output, output
    assert "ready for business" not in output, output


def _setting(server: _Server, name: str) -> str:
    with psycopg.connect(host="127.0.0.1", port=server.port, user="postgres",
                         dbname="postgres") as conn:
        return conn.execute("SELECT setting FROM sdb_flags WHERE name = %s",
                            (name,)).fetchone()[0]


def test_io_threads(tmp_path: Path) -> None:
    server = _Server(tmp_path / "explicit", args=("--io_threads=3",))
    try:
        assert server.wait_ready(), server.output()
        assert _setting(server, "io_threads") == "3"
    finally:
        server.stop()
    server = _Server(tmp_path / "default")
    try:
        assert server.wait_ready(), server.output()
        assert int(_setting(server, "io_threads")) > 0
    finally:
        server.stop()


def test_server_directory_flag_without_a_positional(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    server = _Server(datadir, datadir_flag=True)
    try:
        assert server.wait_ready(), server.output()
        assert (datadir / "PG_VERSION").exists(), server.output()
    finally:
        server.stop()


def test_two_positional_data_directories(tmp_path: Path) -> None:
    result = subprocess.run(
        [SERENED_BIN, str(tmp_path / "a"), str(tmp_path / "b")],
        capture_output=True, text=True, timeout=60)
    assert result.returncode != 0, result
    assert ("expected at most one positional data-dir arg"
            in result.stderr), result
    assert not (tmp_path / "a").exists()


def test_a_data_directory_that_cannot_be_created(tmp_path: Path) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("")
    datadir = blocker / "data"
    result = subprocess.run([SERENED_BIN, str(datadir)], capture_output=True,
                            text=True, timeout=60)
    assert result.returncode != 0, result
    assert (f"Unable to create database directory '{datadir}'"
            in result.stdout + result.stderr), result


def test_a_pg_version_that_cannot_be_checked(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    datadir.mkdir()
    (datadir / "PG_VERSION").symlink_to("PG_VERSION")
    result = subprocess.run([SERENED_BIN, str(datadir)], capture_output=True,
                            text=True, timeout=60)
    assert result.returncode != 0, result
    assert (f"Unable to check '{datadir}/PG_VERSION'"
            in result.stdout + result.stderr), result
    assert not (datadir / "engine_v1").exists()


@pytest.mark.parametrize("relative", [True, False])
def test_hba_config_resolves_against_the_data_directory(
        tmp_path: Path, relative: bool) -> None:
    datadir = tmp_path / "data"
    hba = Path("custom_hba.conf") if relative else tmp_path / "custom_hba.conf"
    server = _Server(datadir, args=(f"--hba_config={hba}",))
    try:
        assert server.wait_ready(), server.output()
        with psycopg.connect(host="127.0.0.1", port=server.port,
                             user="postgres", dbname="postgres",
                             autocommit=True) as conn:
            conn.execute("SET hba = 'host all all 127.0.0.1/32 trust'")
    finally:
        server.stop()
    written = datadir / hba if relative else hba
    assert "host all all 127.0.0.1/32 trust" in written.read_text(), (
        server.output())


@pytest.mark.parametrize(("listen", "message"), [
    ("postgres://127.0.0.1:{port}?sslmode=require",
     "requires TLS but no certificate is configured"),
    ("https://127.0.0.1:{port}?api=es",
     "requires TLS but no certificate is configured"),
    ("postgres://127.0.0.1:{port}?sslmode=verify-full"
     "&cert=/no/such/cert.pem&key=/no/such/key.pem",
     "uses sslmode=verify-* but no client-cert CA is configured"),
])
def test_tls_listener_without_its_files(tmp_path: Path, listen: str,
                                        message: str) -> None:
    reserved = reserve_port()
    server = _Server(tmp_path / "data",
                     listen.format(port=reserved.getsockname()[1]))
    try:
        assert server.proc.wait(timeout=60) != 0, server.output()
        assert message in server.output(), server.output()
    finally:
        server.stop()
        reserved.close()


def _wait_listening(server: _Server, port: int) -> None:
    deadline = time.monotonic() + 60
    while not listens_on(server.proc.pid, [port]):
        assert server.proc.poll() is None, server.output()
        assert time.monotonic() < deadline, server.output()
        time.sleep(0.2)


def test_verify_full_listener_with_its_own_ca(tmp_path: Path) -> None:
    pem = Path(__file__).resolve().parents[3] / "resources" / "ca.pem"
    reserved = reserve_port()
    port = reserved.getsockname()[1]
    server = _Server(tmp_path / "data",
                     f"postgres://127.0.0.1:{port}?sslmode=verify-full"
                     f"&cert={pem}&key={pem}&ca={pem}",
                     args=("--tls_min_version=1.3",))
    try:
        _wait_listening(server, port)
        assert f"listening on postgres://127.0.0.1:{port}" in server.output()
        assert ", tls)" in server.output(), server.output()
    finally:
        server.stop()
        reserved.close()


@pytest.mark.skipif(platform.system() != "Linux",
                    reason="abstract unix sockets are Linux-only")
def test_abstract_unix_socket(tmp_path: Path) -> None:
    name = f"sdb-test-{os.getpid()}-{time.monotonic_ns()}"
    server = _Server(tmp_path / "data", f"postgres:///@{name}")
    try:
        deadline = time.monotonic() + 60
        while True:
            assert server.proc.poll() is None, server.output()
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                try:
                    sock.connect("\0" + name)
                    break
                except (ConnectionRefusedError, FileNotFoundError):
                    pass
            assert time.monotonic() < deadline, server.output()
            time.sleep(0.2)
        assert f"(unix:@{name})" in server.output(), server.output()
    finally:
        server.stop()


def test_unknown_unix_socket_group(tmp_path: Path, socket_dir: Path) -> None:
    server = _Server(tmp_path / "data",
                     f"postgres://{socket_dir / 'sdb.sock'}"
                     "?group=sdb_no_such_group")
    try:
        assert server.proc.wait(timeout=60) != 0, server.output()
        assert ("cannot find group 'sdb_no_such_group'"
                in server.output()), server.output()
    finally:
        server.stop()


def test_cleartext_password_on_a_unix_socket(tmp_path: Path,
                                             socket_dir: Path) -> None:
    server = _Server(tmp_path / "data", f"postgres://{socket_dir}?port=5432")

    def connect(**kwargs) -> psycopg.Connection:
        return psycopg.connect(host=str(socket_dir), port=5432,
                               dbname="postgres", **kwargs)

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


def _scram_attempt(port: int, user: str) -> tuple[str, str]:
    sock = socket.create_connection(("127.0.0.1", port))

    def recv_exact(n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise EOFError("connection closed")
            buf += chunk
        return buf

    def read_msg() -> tuple[bytes, bytes]:
        t = recv_exact(1)
        (ln,) = struct.unpack("!I", recv_exact(4))
        return t, recv_exact(ln - 4)

    def send(t: bytes, payload: bytes) -> None:
        sock.sendall(t + struct.pack("!I", len(payload) + 4) + payload)

    try:
        params = b"user\0" + user.encode() + b"\0database\0postgres\0\0"
        sock.sendall(struct.pack("!II", len(params) + 8, 196608) + params)
        t, body = read_msg()
        assert (t, struct.unpack("!I", body[:4])[0]) == (b"R", 10), (user, t,
                                                                     body)
        first = b"n,,n=,r=probenonce"
        send(b"p", b"SCRAM-SHA-256\0" + struct.pack("!i", len(first)) + first)
        t, body = read_msg()
        assert (t, struct.unpack("!I", body[:4])[0]) == (b"R", 11), (user, t,
                                                                     body)
        fields = dict(part.split("=", 1)
                      for part in body[4:].decode().split(","))
        proof = base64.b64encode(bytes(32)).decode()
        send(b"p", f"c=biws,r={fields['r']},p={proof}".encode())
        t, body = read_msg()
        assert t == b"E", (user, t, body)
        codes = {chr(f[0]): f[1:].decode() for f in body.split(b"\0") if f}
        return fields["s"], codes["C"]
    finally:
        sock.close()


def test_scram_hides_which_roles_exist(tmp_path: Path) -> None:
    datadir = tmp_path / "data"
    users = ("probe_pw", "probe_nopw", "probe_md5", "probe_missing",
             "probe_other_missing")
    server = _Server(datadir)
    try:
        assert server.wait_ready(), server.output()
        with psycopg.connect(host="127.0.0.1", port=server.port,
                             user="postgres", dbname="postgres",
                             autocommit=True) as admin:
            admin.execute("CREATE ROLE probe_pw LOGIN PASSWORD 'probe_secret'")
            admin.execute("CREATE ROLE probe_nopw LOGIN")
            admin.execute("CREATE ROLE probe_md5 LOGIN "
                          "PASSWORD 'md5c0f3ed6e0bd5c24ab7cd2e0a14b9be3f'")
        attempts = {user: _scram_attempt(server.port, user) for user in users}
        assert {code for _, code in attempts.values()} == {"28P01"}, attempts
        assert len({salt for salt, _ in attempts.values()}) == len(users), (
            attempts)
        for user in users:
            assert _scram_attempt(server.port, user) == attempts[user]
    finally:
        server.stop()

    restarted = _Server(datadir)
    try:
        assert restarted.wait_ready(), restarted.output()
        for user in users:
            assert _scram_attempt(restarted.port, user) == attempts[user]
    finally:
        restarted.stop()


def test_startup_parameters_on_a_unix_socket(tmp_path: Path,
                                             socket_dir: Path) -> None:
    server = _Server(tmp_path / "data", f"postgres://{socket_dir}?port=5432")
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                conn = psycopg.connect(
                    host=str(socket_dir), port=5432, user="postgres",
                    dbname="postgres", application_name="unix_startup",
                    options="-c statement_timeout=5s")
                break
            except psycopg.OperationalError:
                assert (server.proc.poll() is None
                        and time.monotonic() < deadline), server.output()
                time.sleep(0.2)
        with conn:
            assert conn.execute(
                "SELECT current_setting('application_name'), "
                "current_setting('statement_timeout')").fetchone() == (
                    "unix_startup", "5s")
    finally:
        server.stop()


def test_copy_and_binds_on_a_unix_socket_and_over_tls(tmp_path: Path,
                                                       socket_dir: Path) -> None:
    pem = Path(__file__).resolve().parents[3] / "resources" / "ca.pem"
    reserved = reserve_port()
    tls_port = reserved.getsockname()[1]
    server = _Server(tmp_path / "data",
                     f"postgres://{socket_dir}?port=5432,"
                     f"postgres://127.0.0.1:{tls_port}?sslmode=require"
                     f"&cert={pem}&key={pem}")
    targets = [{"host": str(socket_dir), "port": 5432},
               {"host": "127.0.0.1", "port": tls_port, "sslmode": "require"}]
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                psycopg.connect(user="postgres", dbname="postgres",
                                **targets[0]).close()
                break
            except psycopg.OperationalError:
                assert (server.proc.poll() is None
                        and time.monotonic() < deadline), server.output()
                time.sleep(0.2)
        _wait_listening(server, tls_port)
        for k, target in enumerate(targets):
            with psycopg.connect(user="postgres", dbname="postgres",
                                 autocommit=True, **target) as conn:
                table = f"transport_copy_{k}"
                conn.execute(f"CREATE TABLE {table} (x INT, label TEXT)")
                with conn.cursor() as cur:
                    with cur.copy(f"COPY {table} FROM STDIN") as cp:
                        cp.write(b"1\tone\n")
                        time.sleep(0.2)
                        cp.write(b"2\ttwo\n")
                    cur.execute(f"SELECT count(*), sum(x) FROM {table}")
                    assert cur.fetchone() == (2, 3)
                    cur.execute("SELECT %b::numeric + 1", [Decimal("1.5")])
                    assert cur.fetchone() == (Decimal("2.5"),)
    finally:
        server.stop()
        reserved.close()


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
