"""The quack protocol between two serened processes and from `serened shell`.

Server B serves quack on an HTTP listener (api=quack); server A and the shell
ATTACH it. B runs with a short --http_body_timeout, so a statement that takes
longer than that must still answer its FETCH.
"""

from __future__ import annotations

import base64
import contextlib
import itertools
import os
import subprocess
import threading
import time
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path

import psycopg
import pytest
import quack_wire
from serened import Serened

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)

ENDLESS = "SELECT count(*) FROM range(10000000000000) a"
BEARER = "quack-test-bearer-token"
HANDED_OVER_LATE_FAULT = "quack_statement_handed_over_late"
HEARTBEAT_PAUSED_FAULT = "quack_heartbeat_paused"
PREPARE_ANSWERS_LATE_FAULT = "quack_prepare_answers_late"
SWEEP_PAUSED_FAULT = "quack_sweep_paused"
REPLACED_LATE_FAULT = "quack_statement_replaced_late"


@contextlib.contextmanager
def _serened(path: Path, **kwargs) -> Iterator[Serened]:
    server = Serened(SERENED_BIN, datadir_root=str(path),
                     log_path=str(path / "serened.log"), **kwargs)
    server.start()
    try:
        yield server
    finally:
        server.stop_cleanly()


@pytest.fixture(scope="module")
def servers(tmp_path_factory: pytest.TempPathFactory):
    with (_serened(tmp_path_factory.mktemp("b"), http="api=quack",
                   extra_args=["--http_body_timeout=1s",
                               f"--auth_bearer_token={BEARER}"]) as b,
          _serened(tmp_path_factory.mktemp("a")) as a,
          psycopg.connect(b.dsn(), autocommit=True) as conn_b,
          psycopg.connect(a.dsn(), autocommit=True) as conn_a):
        conn_b.execute("CREATE TABLE t (id INT, name TEXT)")
        conn_b.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, NULL)")
        conn_b.execute("CREATE ROLE alice LOGIN PASSWORD 'secret'")
        conn_b.execute("CREATE TABLE private (x INT)")
        conn_a.execute(
            f"ATTACH 'quack:127.0.0.1:{b.http_port}' AS b (TOKEN 'postgres:')")
        yield conn_a, conn_b, b.http_port, a.dsn()


def _refresh(conn_a: psycopg.Connection) -> None:
    conn_a.execute("SELECT * FROM quack_clear_cache()")


def _remote(quack_port: int, sql: str, token: str = "postgres:") -> str:
    quoted = sql.replace("'", "''")
    return (f"SELECT * FROM quack_query('quack:127.0.0.1:{quack_port}', "
            f"'{quoted}', token := '{token}')")


def _start(work: Callable[[], object]) -> tuple[threading.Thread, list[object]]:
    outcome: list[object] = []

    def run() -> None:
        try:
            outcome.append(work())
        except psycopg.Error as e:
            outcome.append(e)

    worker = threading.Thread(target=run)
    worker.start()
    return worker, outcome


def _wait_until(done: Callable[[], object], what: str,
                timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while not done():
        assert time.monotonic() < deadline, what
        time.sleep(0.05)


def _sessions(conn_b: psycopg.Connection, where: str,
              params: tuple[object, ...] | None = None) -> int:
    return conn_b.execute("SELECT count(*) FROM quack_active_connections() "
                          f"WHERE {where}", params).fetchone()[0]


def _wait_gone(conn_b: psycopg.Connection, where: str,
               params: tuple[object, ...] | None = None,
               timeout: float = 5) -> None:
    _wait_until(lambda: not _sessions(conn_b, where, params),
                f"B still has a session {where}", timeout)


def _wait_running(conn_b: psycopg.Connection, query: str) -> None:
    _wait_until(lambda: _sessions(
        conn_b, "state = 'active' AND query LIKE %s", (f"%{query}%",)),
                f"B never ran {query}")


@contextlib.contextmanager
def _armed(conn: psycopg.Connection, fault: str) -> Iterator[None]:
    conn.execute(f"SET sdb_faults = '{fault}'")
    try:
        yield
    finally:
        conn.execute(f"SET sdb_faults = '-{fault}'")


@contextlib.contextmanager
def _setting(conn: psycopg.Connection, name: str,
             value: object) -> Iterator[None]:
    conn.execute(f"SET {name} = {value}")
    try:
        yield
    finally:
        conn.execute(f"RESET {name}")


def _shell(sql: str) -> list[str]:
    result = subprocess.run(
        [SERENED_BIN, "shell", ":memory:", "-csv", "-c", sql],
        capture_output=True, text=True, timeout=60,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.splitlines()


def test_banner(servers) -> None:
    _, _, quack_b, _ = servers
    with urllib.request.urlopen(f"http://127.0.0.1:{quack_b}/quack") as r:
        assert r.status == 200
        assert b"quack" in r.read().lower()


def test_select(servers) -> None:
    conn_a, _, _, _ = servers
    rows = conn_a.execute("SELECT id, name FROM b.public.t ORDER BY id")
    assert rows.fetchall() == [(1, "a"), (2, "b"), (3, None)]
    rows = conn_a.execute("SELECT id, name FROM b.t ORDER BY id")
    assert rows.fetchall() == [(1, "a"), (2, "b"), (3, None)]
    assert conn_a.execute(
        "SELECT count(*) FROM b.t JOIN b.t AS u ON b.t.id = u.id").fetchone() == (3,)


def test_types(servers) -> None:
    conn_a, conn_b, _, _ = servers
    conn_b.execute("""
        CREATE TABLE typed (
            i BIGINT, d DOUBLE, n DECIMAL(18, 3), b BOOLEAN, s VARCHAR,
            dt DATE, ts TIMESTAMP, l INTEGER[])""")
    conn_b.execute("""
        INSERT INTO typed VALUES
            (-9000000000, 1.5, 12345.678, true, 'x',
             DATE '2026-10-14', TIMESTAMP '2026-10-14 12:30:00', [1, 2, NULL]),
            (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)""")
    _refresh(conn_a)
    local = conn_b.execute("SELECT * FROM typed ORDER BY i").fetchall()
    remote = conn_a.execute(
        "SELECT * FROM b.public.typed ORDER BY i").fetchall()
    assert remote == local


def test_scan_many_batches(servers) -> None:
    conn_a, conn_b, _, _ = servers
    conn_b.execute(
        "CREATE TABLE big AS SELECT i AS id, 'row' || i AS name "
        "FROM range(500000) r(i)")
    _refresh(conn_a)
    conn_a.execute(
        "CREATE TEMP TABLE picks AS SELECT i AS id FROM range(0, 500000, 7) r(i)")
    count, total = conn_a.execute(
        "SELECT count(*), sum(big.id) FROM b.public.big JOIN picks USING (id)"
    ).fetchone()
    assert count == len(range(0, 500000, 7))
    assert total == sum(range(0, 500000, 7))


def test_insert_from_client(servers) -> None:
    conn_a, conn_b, _, _ = servers
    conn_b.execute("CREATE TABLE sink (id INT, name TEXT)")
    _refresh(conn_a)
    conn_a.execute(
        "CREATE TEMP TABLE src AS SELECT i AS id, 'local' || i AS name "
        "FROM range(200000) r(i)")
    conn_a.execute("INSERT INTO b.public.sink SELECT * FROM src")
    assert conn_b.execute(
        "SELECT count(*), sum(id), count(DISTINCT name) FROM sink"
    ).fetchone() == (200000, sum(range(200000)), 200000)


def test_ddl(servers) -> None:
    conn_a, conn_b, _, _ = servers
    conn_a.execute("CREATE TABLE b.public.made_remotely (x INT, y VARCHAR)")
    conn_a.execute("INSERT INTO b.public.made_remotely VALUES (1, 'one')")
    assert conn_b.execute(
        "SELECT * FROM made_remotely").fetchall() == [(1, "one")]


def test_attaches_itself(tmp_path: Path) -> None:
    # With one worker, a statement that waits on a session of its own server
    # must leave that session a worker.
    with (_serened(tmp_path, http="api=quack",
                   extra_args=["--cpu_threads=1"]) as server,
          psycopg.connect(server.dsn(), autocommit=True) as conn):
        conn.execute("SET statement_timeout = '30s'")
        conn.execute("CREATE TABLE own AS SELECT * FROM range(5) r(x)")
        conn.execute(f"ATTACH 'quack:localhost:{server.http_port}' AS self "
                     "(TOKEN 'postgres:')")
        assert conn.execute(
            "SELECT count(*) FROM self.public.own").fetchone() == (5,)
        assert conn.execute(
            "SELECT * FROM self.query('SELECT count(*) FROM self.public.own')"
        ).fetchone() == (5,)
        assert conn.execute(
            "SELECT * FROM self.query('SELECT * FROM self.query(''SELECT 7'')')"
        ).fetchone() == (7,)
        assert conn.execute(
            _remote(server.http_port, "SELECT 41")).fetchone() == (41,)


def test_remote_error(servers) -> None:
    conn_a, _, quack_b, _ = servers
    with pytest.raises(psycopg.Error, match="nonexistent"):
        conn_a.execute(_remote(quack_b, "SELECT * FROM nonexistent"))
    assert conn_a.execute(_remote(quack_b, "SELECT 1")).fetchone() == (1,)


def test_credentials(servers) -> None:
    conn_a, _, quack_b, _ = servers
    with pytest.raises(psycopg.Error, match="authentication failed"):
        conn_a.execute(
            _remote(quack_b, "SELECT current_user", token="alice:wrong"))
    assert conn_a.execute(
        _remote(quack_b, "SELECT current_user", token="alice:secret")
    ).fetchone() == ("alice",)
    with pytest.raises(psycopg.Error, match="permission denied"):
        conn_a.execute(
            _remote(quack_b, "SELECT * FROM private", token="alice:secret"))
    basic = base64.b64encode(b"alice:secret").decode()
    assert conn_a.execute(
        _remote(quack_b, "SELECT current_user", token=f"Basic {basic}")
    ).fetchone() == ("alice",)
    for bearer in (BEARER, f"Bearer {BEARER}"):
        assert conn_a.execute(
            _remote(quack_b, "SELECT current_user", token=bearer)
        ).fetchone() == ("postgres",)
    with pytest.raises(psycopg.Error, match="authentication failed"):
        conn_a.execute(_remote(quack_b, "SELECT 1", token="not-the-token"))
    with pytest.raises(psycopg.Error, match="authentication failed"):
        conn_a.execute(_remote(quack_b, "SELECT current_user", token=":"))


def test_login_rules(servers) -> None:
    conn_a, conn_b, quack_b, _ = servers
    conn_b.execute(
        "CREATE ROLE bob LOGIN PASSWORD 'pw' VALID UNTIL '2000-01-01'")
    with pytest.raises(psycopg.Error, match="authentication failed"):
        conn_a.execute(_remote(quack_b, "SELECT 1", token="bob:pw"))
    conn_b.execute("ALTER ROLE bob VALID UNTIL '2100-01-01'")
    assert conn_a.execute(
        _remote(quack_b, "SELECT current_user", token="bob:pw")
    ).fetchone() == ("bob",)
    conn_b.execute("SET hba = 'host all bob 0.0.0.0/0 reject\n"
                   "host all all 0.0.0.0/0 scram-sha-256'")
    try:
        with pytest.raises(psycopg.Error, match="authentication failed"):
            conn_a.execute(_remote(quack_b, "SELECT 1", token="bob:pw"))
        assert conn_a.execute(
            _remote(quack_b, "SELECT current_user", token="alice:secret")
        ).fetchone() == ("alice",)
    finally:
        conn_b.execute("RESET hba")


def test_superuser_only(servers) -> None:
    conn_a, _, quack_b, dsn_a = servers
    conn_a.execute("CREATE ROLE mallory LOGIN PASSWORD 'm'")
    conn_a.execute("CREATE ROLE creator LOGIN CREATEDB PASSWORD 'c'")
    with psycopg.connect(dsn_a, user="mallory", password="m",
                         autocommit=True) as mallory:
        for statement in (_remote(quack_b, "SELECT current_user"),
                          "SELECT * FROM quack_active_connections()"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege,
                               match="must be superuser"):
                mallory.execute(statement)
    with psycopg.connect(dsn_a, user="creator", password="c",
                         autocommit=True) as creator:
        with pytest.raises(psycopg.errors.InsufficientPrivilege,
                           match="must be superuser"):
            creator.execute(f"ATTACH 'quack:127.0.0.1:{quack_b}' AS c "
                            "(TOKEN 'postgres:')")


def test_request_longer_than_body_timeout(servers) -> None:
    conn_a, _, quack_b, _ = servers
    # The padding spreads the request over many reads, and PREPARE answers
    # only once the statement is done.
    padding = "-- " + "x" * (1 << 20) + "\n"
    started = time.monotonic()
    assert conn_a.execute(_remote(
        quack_b, padding +
        "CREATE OR REPLACE TEMP TABLE slow AS "
        "SELECT pg_sleep(2) IS NULL AS s")).fetchone() == (1,)
    assert time.monotonic() - started > 1


def test_cancel(servers) -> None:
    conn_a, conn_b, quack_b, dsn_a = servers
    with psycopg.connect(dsn_a, autocommit=True) as running:
        pid = running.execute("SELECT pg_backend_pid()").fetchone()[0]
        # ends the query even when the cancel does not, so a failure is no hang
        running.execute("SET statement_timeout = '60s'")
        worker, outcome = _start(
            lambda: running.execute(_remote(quack_b, ENDLESS)))
        _wait_running(conn_b, ENDLESS)
        cancelled_at = time.monotonic()
        conn_a.execute("SELECT pg_cancel_backend(%s)", (pid,))
        worker.join(timeout=30)
        stopped = time.monotonic() - cancelled_at < 30 and not worker.is_alive()
        worker.join(timeout=90)
        assert stopped, "the cancel did not stop the query"
        assert outcome and "cancel" in str(outcome[0]).lower()
    assert conn_a.execute(_remote(quack_b, "SELECT 2")).fetchone() == (2,)


@pytest.mark.parametrize("path", ["attach", "quack_query", "query"])
def test_statement_timeout(servers, path: str) -> None:
    conn_a, conn_b, quack_b, dsn_a = servers
    conn_b.execute(f"CREATE VIEW IF NOT EXISTS endless AS {ENDLESS}")
    _refresh(conn_a)
    statement = {
        "attach": "SELECT * FROM b.public.endless",
        "quack_query": _remote(quack_b, ENDLESS),
        "query": "SELECT * FROM b.query('{}')".format(ENDLESS),
    }[path]
    with psycopg.connect(dsn_a, autocommit=True) as conn:
        pid = conn.execute("SELECT pg_backend_pid()").fetchone()[0]
        conn.execute("SET statement_timeout = '1s'")
        worker, outcome = _start(lambda: conn.execute(statement))
        worker.join(timeout=10)
        if worker.is_alive():
            conn_a.execute("SELECT pg_cancel_backend(%s)", (pid,))
            worker.join(timeout=30)
            pytest.fail("statement_timeout did not stop the query")
        assert isinstance(outcome[0], psycopg.errors.QueryCanceled)
        assert "statement timeout" in str(outcome[0])
        _wait_gone(conn_b, "state = 'active'")
        conn.execute("RESET statement_timeout")
        assert conn.execute(
            "SELECT count(*) FROM b.public.t").fetchone() == (3,)


def test_default_statement_timeout(servers) -> None:
    _, conn_b, _, dsn_a = servers
    with psycopg.connect(dsn_a, autocommit=True) as conn:
        conn.execute("SET quack_default_statement_timeout = 1000")
        started = time.monotonic()
        with pytest.raises(psycopg.errors.QueryCanceled, match="statement timeout"):
            conn.execute("SELECT * FROM b.query('SELECT pg_sleep(5) IS NULL')")
        assert time.monotonic() - started < 4
        _wait_gone(conn_b, "state = 'active'")
        conn.execute("SET statement_timeout = '10s'")
        assert conn.execute(
            "SELECT * FROM b.query('SELECT pg_sleep(2) IS NULL')").fetchone() == (True,)


def test_insert_timeout(servers) -> None:
    conn_a, conn_b, _, dsn_a = servers
    conn_b.execute("CREATE TABLE timed_sink (x INT)")
    _refresh(conn_a)
    with psycopg.connect(dsn_a, autocommit=True) as conn:
        conn.execute("SET statement_timeout = '1s'")
        with pytest.raises(psycopg.errors.QueryCanceled):
            conn.execute("INSERT INTO b.public.timed_sink SELECT x "
                         "FROM range(3) r(x) WHERE pg_sleep(1) IS NULL")
        _wait_gone(conn_b, "state = 'active'")
        conn.execute("RESET statement_timeout")
        conn.execute("INSERT INTO b.public.timed_sink VALUES (7)")
    assert conn_b.execute("SELECT * FROM timed_sink").fetchall() == [(7,)]


def test_ddl_timeout(servers) -> None:
    conn_a, conn_b, _, dsn_a = servers
    with psycopg.connect(dsn_a, autocommit=True) as conn:
        conn.execute("SET statement_timeout = '1s'")
        with pytest.raises(psycopg.errors.QueryCanceled):
            conn.execute("SELECT * FROM b.query("
                         "'CREATE TABLE pushed_timed AS SELECT pg_sleep(3) AS s')")
        with pytest.raises(psycopg.errors.QueryCanceled):
            conn.execute("CREATE TABLE b.public.ctas_timed AS SELECT x "
                         "FROM range(3) r(x) WHERE pg_sleep(1) IS NULL")
        _wait_gone(conn_b, "state = 'active'")
    time.sleep(3)
    assert conn_b.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_name IN ('pushed_timed', 'ctas_timed')").fetchone() == (0,)


def test_timeout_ends_with_its_statement(servers) -> None:
    _, _, _, dsn_a = servers
    with psycopg.connect(dsn_a, autocommit=True) as conn:
        conn.execute("SET statement_timeout = '1s'")
        assert conn.execute(
            "SELECT count(*) FROM b.public.t").fetchone() == (3,)
        conn.execute("RESET statement_timeout")
        assert conn.execute(
            "SELECT * FROM b.query('CREATE OR REPLACE TEMP TABLE pause AS "
            "SELECT pg_sleep(2) AS s')").fetchone() == (1,)


@pytest.mark.faults
def test_expired_lease_reconnects(servers) -> None:
    conn_a, conn_b, quack_b, dsn_a = servers
    with _setting(conn_b, "quack_max_heartbeat_timeout", 3):
        conn_a.execute(f"ATTACH 'quack:127.0.0.1:{quack_b}' AS leased "
                       "(TOKEN 'postgres:', HEARTBEAT_TIMEOUT 3)")
        try:
            with psycopg.connect(dsn_a, autocommit=True) as conn:
                assert conn.execute(
                    "SELECT count(*) FROM leased.public.t").fetchone() == (3,)
                with (_armed(conn_a, HEARTBEAT_PAUSED_FAULT),
                      _armed(conn_b, SWEEP_PAUSED_FAULT)):
                    time.sleep(4)
                    with pytest.raises(psycopg.Error, match="lease expired"):
                        conn.execute("SELECT count(*) FROM leased.public.t")
                assert conn.execute(
                    "SELECT count(*) FROM leased.public.t").fetchone() == (3,)
        finally:
            conn_a.execute("DETACH leased")


def test_prepared_scan_after_lost_session(servers) -> None:
    _, conn_b, quack_b, dsn_a = servers

    def lose(tag: str) -> None:
        assert conn_b.execute(
            "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
            f"WHERE query LIKE '%{tag}%' AND pid <> pg_backend_pid()"
        ).fetchone() == (1,)

    with psycopg.connect(dsn_a, autocommit=True) as conn:
        conn.execute("PREPARE attached AS SELECT count(*) FROM b.public.t")
        conn.execute("PREPARE own AS " + _remote(quack_b, "SELECT 44 AS own_lost"))
        assert conn.execute("EXECUTE attached").fetchone() == (3,)
        assert conn.execute("EXECUTE own").fetchone() == (44,)
        conn.execute("SELECT * FROM b.query('SELECT 43 AS attached_lost')")
        lose("43 AS attached_lost")
        lose("44 AS own_lost")
        for name, row in (("attached", (3,)), ("own", (44,))):
            with pytest.raises(psycopg.Error, match="Invalid connection id"):
                conn.execute(f"EXECUTE {name}")
            assert conn.execute(f"EXECUTE {name}").fetchone() == row
            assert conn.execute(f"EXECUTE {name}").fetchone() == row


def test_heartbeat_follows_the_granted_lease(servers) -> None:
    conn_a, conn_b, quack_b, dsn_a = servers
    with _setting(conn_b, "quack_max_heartbeat_timeout", 3):
        conn_a.execute(f"ATTACH 'quack:127.0.0.1:{quack_b}' AS capped "
                       "(TOKEN 'postgres:', HEARTBEAT_TIMEOUT 30)")
        try:
            with psycopg.connect(dsn_a, autocommit=True) as conn:
                (session,) = conn.execute(
                    "SELECT quack_connection_id('capped')").fetchone()
                time.sleep(7)
                assert conn.execute(
                    "SELECT count(*) FROM capped.public.t").fetchone() == (3,)
                assert conn.execute(
                    "SELECT quack_connection_id('capped')").fetchone() == (session,)
        finally:
            conn_a.execute("DETACH capped")


def test_server_settings_need_superuser(servers) -> None:
    _, conn_b, _, _ = servers
    with psycopg.connect(host="127.0.0.1", port=conn_b.info.port, user="alice",
                         password="secret", dbname="postgres",
                         autocommit=True) as alice:
        for setting in ("quack_max_sessions = 1",
                        "quack_max_heartbeat_timeout = 1",
                        "quack_result_ttl = 1",
                        "quack_cache_max_rows = 0",
                        "quack_server_stream_buffer_bytes = 0",
                        "quack_enable_reconnects = true",
                        "quack_prepare_inline_rows = 1",
                        "quack_default_client_id = 'abcd'"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege,
                               match="permission denied to set parameter"):
                alice.execute(f"SET {setting}")
        with pytest.raises(psycopg.errors.InsufficientPrivilege,
                           match="permission denied to set parameter"):
            alice.execute("RESET quack_max_sessions")
        alice.execute("SET quack_default_statement_timeout = 5000")
    assert conn_b.execute(
        "SELECT current_setting('quack_max_sessions')").fetchone() == ("0",)


def test_session_limit(servers) -> None:
    conn_a, conn_b, quack_b, _ = servers
    with (_setting(conn_b, "quack_max_sessions", 1),
          pytest.raises(psycopg.Error, match="Too many Quack sessions")):
        conn_a.execute(_remote(quack_b, "SELECT 1"))
    assert conn_a.execute(_remote(quack_b, "SELECT 1")).fetchone() == (1,)


@pytest.mark.faults
def test_remote_statement_leaves_the_worker_free(servers,
                                                tmp_path: Path) -> None:
    _, conn_b, quack_b, _ = servers
    # B answers a PREPARE only once its rows are there, as a DuckDB server does
    with (_armed(conn_b, PREPARE_ANSWERS_LATE_FAULT),
          _serened(tmp_path, extra_args=["--cpu_threads=1"]) as c,
          psycopg.connect(c.dsn(), autocommit=True) as slow,
          psycopg.connect(c.dsn(), autocommit=True) as fast):
        slow.execute(f"ATTACH 'quack:127.0.0.1:{quack_b}' AS b "
                     "(TOKEN 'postgres:')")
        worker, outcome = _start(lambda: slow.execute(
            "SELECT * FROM b.query("
            "'SELECT pg_sleep(3) IS NULL AS s')").fetchone())
        _wait_running(conn_b, "pg_sleep(3)")
        assert fast.execute("SELECT 42").fetchone() == (42,)
        assert worker.is_alive(), "the remote statement ended too soon"
        worker.join(timeout=120)
        assert outcome == [(True,)]


def _wait_parked(conn: psycopg.Connection, fault: str) -> None:
    def parked() -> bool:
        try:
            conn.execute(f"SET sdb_faults = '-{fault}:parked'")
        except psycopg.Error:
            return False
        return True

    _wait_until(parked, f"{fault} never parked")


@pytest.mark.faults
def test_late_handovers_leave_the_newest_statement(tmp_path: Path) -> None:
    with (_serened(tmp_path, http="api=quack",
                   extra_args=["--cpu_threads=8",
                               f"--auth_bearer_token={BEARER}"]) as server,
          psycopg.connect(server.dsn(), autocommit=True) as conn):
        conn.execute("CREATE TABLE handed_over (x INT)")
        session = quack_wire.Session(server.http_port, BEARER)
        late, stalled, newest = (session.on_another_connection()
                                 for _ in range(3))
        try:
            conn.execute("SET sdb_faults = 'pause_checkpoint'")
            checkpoint, _ = _start(lambda: session.prepare("CHECKPOINT", 1))
            _wait_parked(conn, "pause_checkpoint")
            conn.execute(f"SET sdb_faults = '{HANDED_OVER_LATE_FAULT}'")
            late_run, late_outcome = _start(lambda: late.prepare("SELECT 1", 2))
            _wait_parked(conn, HANDED_OVER_LATE_FAULT)
            conn.execute(f"SET sdb_faults = '{REPLACED_LATE_FAULT}'")
            stalled_run, stalled_outcome = _start(
                lambda: stalled.prepare("SELECT 2", 3))
            _wait_parked(conn, REPLACED_LATE_FAULT)
            newest_run, newest_outcome = _start(lambda: newest.prepare(
                "INSERT INTO handed_over VALUES (1)", 4))
            _wait_until(lambda: _sessions(
                conn, "query LIKE 'INSERT INTO handed_over%'"),
                        "the newest PREPARE never came")
            time.sleep(0.2)
            conn.execute(f"SET sdb_faults = '{HANDED_OVER_LATE_FAULT}:release'")
            time.sleep(0.2)
            conn.execute(f"SET sdb_faults = '{REPLACED_LATE_FAULT}:release'")
            for run, outcome in ((late_run, late_outcome),
                                 (stalled_run, stalled_outcome)):
                run.join(timeout=30)
                assert outcome[0][0] == quack_wire.ERROR_RESPONSE
                assert "superseded by a new query" in outcome[0][1]
            conn.execute("SET sdb_faults = 'pause_checkpoint:release'")
            checkpoint.join(timeout=30)
            newest_run.join(timeout=30)
            assert newest_outcome[0][0] != quack_wire.ERROR_RESPONSE, \
                newest_outcome[0][1]
            assert conn.execute(
                "SELECT count(*) FROM handed_over").fetchone() == (1,)
        finally:
            conn.execute("RESET sdb_faults")
            for channel in (session, late, stalled, newest):
                channel.close()


def test_lease_limit(servers, tmp_path: Path) -> None:
    _, conn_b, quack_b, _ = servers
    with _setting(conn_b, "quack_max_heartbeat_timeout", 2):
        with Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "serened.log")) as c:
            c.start()
            with psycopg.connect(c.dsn(), autocommit=True) as conn_c:
                conn_c.execute(f"ATTACH 'quack:127.0.0.1:{quack_b}' AS b "
                               "(TOKEN 'postgres:', HEARTBEAT_TIMEOUT 3600)")
                assert conn_c.execute(
                    "SELECT * FROM b.query('SELECT 7 AS lease_limit')"
                ).fetchone() == (7,)
            c.kill()
        _wait_gone(conn_b, "query LIKE '%7 AS lease_limit%'", timeout=15)


def test_sessions_run_concurrently(servers) -> None:
    conn_a, conn_b, _, dsn_a = servers
    conn_b.execute("CREATE VIEW IF NOT EXISTS slow AS "
                   "SELECT sum(a % 7) AS s FROM range(300000000) r(a)")
    _refresh(conn_a)
    expected = conn_b.execute("SELECT s FROM slow").fetchone()

    def read() -> object:
        with psycopg.connect(dsn_a, autocommit=True) as conn:
            return conn.execute("SELECT s FROM b.public.slow").fetchone()

    started = [_start(read) for _ in range(4)]
    for worker, _ in started:
        worker.join(timeout=120)
    assert [outcome for _, outcome in started] == [[expected]] * 4


def test_sessions_isolated(servers) -> None:
    conn_a, conn_b, _, dsn_a = servers
    conn_b.execute("CREATE TABLE iso (x INT)")
    conn_b.execute("CREATE VIEW boom AS SELECT error('boom')::INT AS y")
    _refresh(conn_a)
    with (psycopg.connect(dsn_a, autocommit=True) as writer,
          psycopg.connect(dsn_a, autocommit=True) as other):
        writer.execute("BEGIN")
        writer.execute("INSERT INTO b.public.iso VALUES (1)")
        assert other.execute(
            "SELECT count(*) FROM b.public.iso").fetchone() == (0,)
        with pytest.raises(psycopg.Error, match="boom"):
            other.execute("SELECT * FROM b.public.boom")
        assert writer.execute(
            "SELECT count(*) FROM b.public.iso").fetchone() == (1,)
        writer.execute("COMMIT")
        assert other.execute(
            "SELECT count(*) FROM b.public.iso").fetchone() == (1,)


def test_prepared_statement_reruns(servers) -> None:
    conn_a, conn_b, quack_b, dsn_a = servers
    conn_b.execute("CREATE TABLE counter (x INT)")
    conn_b.execute(
        "CREATE VIEW counter_view AS SELECT count(*) AS c FROM counter")
    _refresh(conn_a)
    reads = [
        _remote(quack_b, "SELECT count(*) FROM counter"),
        "SELECT count(*) FROM b.public.counter",
        "SELECT c FROM b.public.counter_view",
        "SELECT * FROM b.query('SELECT count(*) FROM counter')",
    ]
    with psycopg.connect(dsn_a, autocommit=True) as conn:
        for rows in range(1, 4):
            conn_b.execute("INSERT INTO counter VALUES (1)")
            for sql in reads:
                assert conn.execute(sql, prepare=True).fetchone() == (rows,), sql
        for i in range(3):
            conn_b.execute(f"CREATE TABLE added{i} (x INT)")
            conn.execute("SELECT * FROM quack_clear_cache()", prepare=True)
            assert conn.execute(
                f"SELECT count(*) FROM b.public.added{i}").fetchone() == (0,)


def test_statements_run_when_executed(servers) -> None:
    conn_a, conn_b, quack_b, dsn_a = servers
    conn_b.execute("CREATE TABLE once AS SELECT * FROM range(3) r(x)")
    _refresh(conn_a)

    def rows() -> int:
        return conn_b.execute("SELECT count(*) FROM once").fetchone()[0]

    with psycopg.connect(dsn_a, autocommit=True) as conn:
        conn.execute("PREPARE drop_zero AS DELETE FROM b.public.once WHERE x = 0")
        conn.execute("PREPARE add_ten AS " + _remote(
            quack_b, "INSERT INTO once VALUES (10) RETURNING x"))
        conn.execute("EXPLAIN DELETE FROM b.public.once WHERE x = 1")
        assert rows() == 3
        conn.execute("EXECUTE drop_zero")
        assert rows() == 2
        assert conn.execute("EXECUTE add_ten").fetchall() == [(10,)]
        assert conn.execute("EXECUTE add_ten").fetchall() == [(10,)]
        assert rows() == 4
        conn.execute(_remote(quack_b, "INSERT INTO once VALUES (20)"))
        assert rows() == 5


def test_detach_closes_sessions(servers) -> None:
    conn_a, conn_b, quack_b, dsn_a = servers
    with psycopg.connect(dsn_a, autocommit=True) as conn:
        conn.execute(
            f"ATTACH 'quack:127.0.0.1:{quack_b}' AS detached (TOKEN 'postgres:')")
        conn.execute("SELECT count(*) FROM detached.public.t")
        (session,) = conn.execute(
            "SELECT quack_connection_id('detached')").fetchone()
        assert conn_b.execute(
            "SELECT count(*) FROM quack_active_connections() "
            "WHERE connection_id = %s", (session,)).fetchone() == (1,)
        conn.execute("DETACH detached")
        _wait_gone(conn_b, "connection_id = %s", (session,))


def test_quack_cancel(servers) -> None:
    conn_a, conn_b, _, dsn_a = servers
    conn_b.execute(f"CREATE VIEW IF NOT EXISTS endless AS {ENDLESS}")
    _refresh(conn_a)
    with (psycopg.connect(dsn_a, autocommit=True) as running,
          psycopg.connect(dsn_a, autocommit=True) as other):
        pid = running.execute("SELECT pg_backend_pid()").fetchone()[0]
        (target,) = running.execute(
            "SELECT quack_connection_id('b')").fetchone()
        for _ in range(2):
            worker = _start(
                lambda: running.execute("SELECT * FROM b.public.endless"))[0]
            _wait_running(conn_b, "endless")
            try:
                rows = other.execute(
                    f"SELECT * FROM quack_cancel('b', '{target}')",
                    prepare=True).fetchall()
                worker.join(timeout=10)
                stopped = not worker.is_alive()
            finally:
                if worker.is_alive():
                    conn_a.execute("SELECT pg_cancel_backend(%s)", (pid,))
                    worker.join(timeout=30)
            assert rows == [(target, True)]
            assert stopped, "quack_cancel did not stop the query"


def test_shell(servers) -> None:
    _, _, quack_b, _ = servers
    lines = _shell(
        f"ATTACH 'quack:127.0.0.1:{quack_b}' AS b (TOKEN 'postgres:'); "
        "CREATE TABLE b.from_shell (x INT); "
        "INSERT INTO b.from_shell SELECT * FROM range(10); "
        "SELECT count(*) FROM b.from_shell; "
        "EXPLAIN CREATE TABLE b.explained AS FROM range(10); "
        "CREATE TABLE b.ctas AS FROM range(10000000); "
        "SELECT count(*) FROM b.ctas; "
        "SELECT count(*) FROM b.t JOIN b.t AS u ON b.t.id = u.id; "
        "SELECT id, name FROM b.public.t ORDER BY id;")
    assert lines[:5] == ["Count", "Count", "10", "count", "10"]
    assert lines[-10:] == ["Count", "10000000", "count", "10000000",
                           "count", "3", "id,name", "1,a", "2,b", "3,NULL"]


def test_unqualified_tables_follow_the_current_schema(servers) -> None:
    _, conn_b, quack_b, _ = servers
    conn_b.execute("CREATE SCHEMA current_s")
    conn_b.execute("CREATE TABLE current_s.picked AS SELECT 1 AS v")
    conn_b.execute("CREATE TABLE public.picked AS SELECT 2 AS v")
    conn_b.execute("CREATE TABLE current_s.only_here AS SELECT 3 AS v")
    try:
        assert _shell(
            f"ATTACH 'quack:127.0.0.1:{quack_b}' AS b (TOKEN 'postgres:'); "
            "USE b.current_s; "
            "SELECT v FROM picked; "
            "SELECT v FROM only_here; "
            "SELECT x.v FROM picked x JOIN picked y USING (v);"
        ) == ["v", "1", "v", "3", "v", "1"]
    finally:
        conn_b.execute("DROP SCHEMA current_s CASCADE")
        conn_b.execute("DROP TABLE public.picked")


def test_unqualified_targets_follow_the_current_schema(servers) -> None:
    _, conn_b, quack_b, _ = servers
    conn_b.execute("CREATE SCHEMA target_s")
    for schema in ("target_s", "public"):
        conn_b.execute(f"CREATE TABLE {schema}.filled (v INT)")
        conn_b.execute(f"CREATE TABLE {schema}.dropped (v INT)")
    try:
        _shell(
            f"ATTACH 'quack:127.0.0.1:{quack_b}' AS b (TOKEN 'postgres:'); "
            "USE b.target_s; "
            "INSERT INTO filled SELECT 1; "
            "ALTER TABLE filled ADD COLUMN w INT; "
            "CREATE TABLE made (v INT); "
            "DROP TABLE dropped;")
        assert conn_b.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_name IN ('filled', 'made', 'dropped') "
            "ORDER BY 1, 2").fetchall() == [
                ("public", "dropped"), ("public", "filled"),
                ("target_s", "filled"), ("target_s", "made")]
        assert conn_b.execute(
            "SELECT v, w FROM target_s.filled").fetchall() == [(1, None)]
        assert conn_b.execute(
            "SELECT count(*) FROM public.filled").fetchone() == (0,)
    finally:
        conn_b.execute("DROP SCHEMA target_s CASCADE")
        conn_b.execute("DROP TABLE IF EXISTS public.filled")
        conn_b.execute("DROP TABLE IF EXISTS public.dropped")


def test_truncate(servers) -> None:
    conn_a, conn_b, quack_b, _ = servers
    conn_b.execute("CREATE SCHEMA truncate_s")
    for schema in ("truncate_s", "public"):
        conn_b.execute(f"CREATE TABLE {schema}.emptied AS SELECT 1 AS v")
        conn_b.execute(f"CREATE TABLE {schema}.also_emptied AS SELECT 1 AS v")
    try:
        _refresh(conn_a)
        conn_a.execute("TRUNCATE b.public.emptied")
        assert conn_b.execute(
            "SELECT count(*) FROM public.emptied").fetchone() == (0,)
        _shell(
            f"ATTACH 'quack:127.0.0.1:{quack_b}' AS b (TOKEN 'postgres:'); "
            "USE b.truncate_s; "
            "TRUNCATE emptied, also_emptied;")
        assert conn_b.execute(
            "SELECT (SELECT count(*) FROM truncate_s.emptied), "
            "(SELECT count(*) FROM truncate_s.also_emptied), "
            "(SELECT count(*) FROM public.also_emptied)").fetchone() == (0, 0, 1)
    finally:
        conn_b.execute("DROP SCHEMA truncate_s CASCADE")
        conn_b.execute("DROP TABLE IF EXISTS public.emptied")
        conn_b.execute("DROP TABLE IF EXISTS public.also_emptied")


def test_committed_ddl_reaches_other_sessions(servers) -> None:
    conn_a, conn_b, _, dsn_a = servers
    with psycopg.connect(dsn_a, autocommit=True) as other:
        conn_a.execute("BEGIN")
        try:
            conn_a.execute("CREATE TABLE b.public.made_in_txn (x INT)")
            other.execute("SELECT count(*) FROM b.public.t")
            conn_a.execute("COMMIT")
            assert other.execute(
                "SELECT count(*) FROM b.public.made_in_txn").fetchone() == (0,)
        finally:
            if conn_a.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                conn_a.execute("ROLLBACK")
            conn_b.execute("DROP TABLE IF EXISTS made_in_txn")
            _refresh(conn_a)


def test_shell_pg_wire_and_quack_agree(servers) -> None:
    _, conn_b, quack_b, _ = servers
    conn_b.execute(
        "CREATE TABLE wire_types (i INT, b BIGINT, d DOUBLE, n NUMERIC(10, 2), "
        "s TEXT, f BOOL, dt DATE, ts TIMESTAMP)")
    conn_b.execute(
        "INSERT INTO wire_types VALUES "
        "(1, 10000000000, 1.5, 12.34, 'one', true, '2026-01-02', "
        "'2026-01-02 03:04:05'), "
        "(2, -1, -0.25, -0.5, 'two', false, '1999-12-31', "
        "'1999-12-31 23:59:59'), "
        "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    queries = (
        "SELECT * FROM {db}.public.wire_types ORDER BY i NULLS LAST; "
        "SELECT count(*) AS rows, count(i) AS ints, sum(b) AS sum_b, "
        "avg(d) AS avg_d, sum(n) AS sum_n, max(s) AS max_s, "
        "min(dt) AS min_dt, max(ts) AS max_ts FROM {db}.public.wire_types; "
        "SELECT s, i * 2 FROM {db}.public.wire_types WHERE f; "
        "SELECT t.id, w.s FROM {db}.public.t "
        "JOIN {db}.public.wire_types w ON t.id = w.i ORDER BY t.id;")
    pg_wire = _shell(
        "SET pg_use_text_protocol = true; SET pg_use_binary_copy = false; "
        f"ATTACH 'host=127.0.0.1 port={conn_b.info.port} dbname=postgres "
        "user=postgres' AS pg (TYPE postgres); " + queries.format(db="pg"))
    quack = _shell(
        f"ATTACH 'quack:127.0.0.1:{quack_b}' AS q (TOKEN 'postgres:'); "
        + queries.format(db="q"))
    assert pg_wire == quack
    assert quack[:4] == [
        "i,b,d,n,s,f,dt,ts",
        "1,10000000000,1.5,12.34,one,true,2026-01-02,2026-01-02 03:04:05",
        "2,-1,-0.25,-0.50,two,false,1999-12-31,1999-12-31 23:59:59",
        "NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL",
    ]
    assert quack[4:6] == [
        "rows,ints,sum_b,avg_d,sum_n,max_s,min_dt,max_ts",
        "3,2,9999999999,0.625,11.84,two,1999-12-31,2026-01-02 03:04:05",
    ]


def _forged_batches() -> list:
    w = quack_wire
    bigint, boolean, varchar = (
        w.logical_type(t) for t in (w.BIGINT, w.BOOLEAN, w.VARCHAR))
    seven = (7).to_bytes(8, "little")
    wide_enum = [f"v{number}" for number in range(300)]
    huge_enum = [f"v{number}" for number in range(70000)]

    def union(tag: bytes, i: bytes, s: bytes) -> bytes:
        return w.chunk(1, [w.union_type([("i", bigint), ("s", varchar)])],
                       [w.struct_vector([tag, i, s])])

    cases = [
        # what the client forges, the column type of its stream, the batch,
        # the server's error
        ("a list with more entries than its rows", "BIGINT[]",
         w.chunk(1, [w.list_type(bigint)],
                 [w.list_vector(0, 4096, w.fixed_vector(b""))]),
         '"entries" holds 4096 elements instead of 1'),
        ("a list entry past the end of its child", "BIGINT[]",
         w.chunk(1, [w.list_type(bigint)],
                 [w.list_vector(1, 1, w.fixed_vector(seven), length=5)]),
         "List entry (0, 5) is outside its 1 child rows"),
        ("a string longer than the string data", "VARCHAR",
         w.chunk(1, [varchar], [w.string_vector([100], b"abc")]),
         "lengths exceed its 3 bytes"),
        ("an array of another size than its type", "BIGINT[2]",
         w.chunk(1, [w.array_type(bigint, 2)],
                 [w.array_vector(3, w.fixed_vector(seven))]),
         "Array size 3 does not match type BIGINT[2]"),
        ("a dictionary index past the dictionary", "BIGINT",
         w.chunk(2, [bigint], [w.dictionary_vector([0, 5], 1, seven)]),
         "Dictionary index 5 is outside its 1 entries"),
        ("a dictionary larger than the rows it serves", "BIGINT",
         w.chunk(2, [bigint], [w.dictionary_vector([0, 1], 3, seven * 3)]),
         "Dictionary of 3 entries for 2 rows"),
        ("a sequence in place of a list", "BIGINT[]",
         w.chunk(1, [w.list_type(bigint)], [w.sequence_vector()]),
         "A sequence vector of type BIGINT[]"),
        ("constants nested two million deep", "BIGINT",
         w.chunk(1, [bigint], [w.nested_constant_vector(2_000_000)]),
         "stack depth limit exceeded"),
        ("a chunk without columns", "BIGINT",
         w.chunk(1, [], []),
         "A data chunk has no columns"),
        ("more columns than column types", "BIGINT",
         w.chunk(1, [bigint], [w.fixed_vector(seven)] * 2),
         '"columns" holds 2 elements instead of 1'),
        ("more rows than a chunk holds", "BIGINT",
         w.chunk(4096, [bigint], [w.fixed_vector(bytes(8 * 4096))]),
         "A batch chunk holds 4096 rows, more than 2048"),
        ("a column of another type than the stream's", "BIGINT",
         w.chunk(1, [varchar], [w.string_vector([5], b"hello")]),
         "column types of its data stream"),
        ("a string that is not UTF-8", "VARCHAR",
         w.chunk(1, [varchar], [w.string_vector([2], b"\xff\xfe")]),
         "Invalid UTF-8"),
        ("a boolean that is neither 0 nor 1", "BOOLEAN",
         w.chunk(1, [boolean], [w.fixed_vector(b"\x02")]),
         "Invalid BOOLEAN value 2"),
        ("a bad boolean inside a list", "BOOLEAN[]",
         w.chunk(1, [w.list_type(boolean)],
                 [w.list_vector(1, 1, w.fixed_vector(b"\x04"), length=1)]),
         "Invalid BOOLEAN value 4"),
        ("a bad boolean inside a dictionary", "BOOLEAN",
         w.chunk(1, [boolean], [w.dictionary_vector([0], 1, b"\x03")]),
         "Invalid BOOLEAN value 3"),
        ("a bad boolean inside a constant", "BOOLEAN",
         w.chunk(1, [boolean], [w.constant_vector(w.fixed_vector(b"\x05"))]),
         "Invalid BOOLEAN value 5"),
        ("a bad boolean inside an array", "BOOLEAN[1]",
         w.chunk(1, [w.array_type(boolean, 1)],
                 [w.array_vector(1, w.fixed_vector(b"\x06"))]),
         "Invalid BOOLEAN value 6"),
        ("a bad boolean inside a struct", "STRUCT(b BOOLEAN)",
         w.chunk(1, [w.struct_type([("b", boolean)])],
                 [w.struct_vector([w.fixed_vector(b"\x07")])]),
         "Invalid BOOLEAN value 7"),
        ("an enum index past the enum's values", "ENUM('a', 'b')",
         w.chunk(1, [w.enum_type(["a", "b"])], [w.fixed_vector(b"\x05")]),
         "ENUM index 5 is outside its 2 values"),
        ("an index past the values of an enum of two-byte indexes",
         f"ENUM({', '.join(repr(value) for value in wide_enum)})",
         w.chunk(1, [w.enum_type(wide_enum)],
                 [w.fixed_vector((300).to_bytes(2, "little"))]),
         "ENUM index 300 is outside its 300 values"),
        ("an index past the values of an enum of four-byte indexes",
         f"ENUM({', '.join(repr(value) for value in huge_enum)})",
         w.chunk(1, [w.enum_type(huge_enum)],
                 [w.fixed_vector((70000).to_bytes(4, "little"))]),
         "ENUM index 70000 is outside its 70000 values"),
        ("a union with two members set", "UNION(i BIGINT, s VARCHAR)",
         union(w.fixed_vector(b"\x00"), w.fixed_vector(seven),
               w.string_vector([1], b"x")),
         "A UNION value has more than one member set"),
        ("a union tag past the union's members", "UNION(i BIGINT, s VARCHAR)",
         union(w.fixed_vector(b"\x05"), w.fixed_vector(seven),
               w.string_vector([1], b"x", null=True)),
         "A UNION tag is outside its 2 members"),
        ("a union without a tag", "UNION(i BIGINT, s VARCHAR)",
         union(w.fixed_vector(b"\x00", null=True), w.fixed_vector(seven),
               w.string_vector([1], b"x", null=True)),
         "A UNION value has a NULL tag"),
        ("a union tag naming the member that is not set",
         "UNION(i BIGINT, s VARCHAR)",
         union(w.fixed_vector(b"\x01"), w.fixed_vector(seven),
               w.string_vector([1], b"x", null=True)),
         "A UNION tag names a member other than the one set"),
        ("a type whose values cannot be checked", "BIT",
         w.chunk(1, [w.logical_type(w.BIT)], [w.string_vector([2], b"\x00\x01")]),
         "from an untrusted peer are not supported"),
    ]
    return [pytest.param(*case[1:], id=case[0]) for case in cases]


@pytest.fixture(scope="module")
def forger(servers) -> Iterator[tuple[quack_wire.Session, Iterator[int]]]:
    session = quack_wire.Session(servers[2], BEARER)
    try:
        yield session, itertools.count(1)
    finally:
        session.close()


@pytest.mark.parametrize("column_type, batch, error", _forged_batches())
def test_send_data_refuses_a_forged_batch(servers, forger, column_type: str,
                                          batch: bytes, error: str) -> None:
    _, conn_b, _, _ = servers
    session, numbers = forger
    number = next(numbers)
    session.prepare(
        f"SELECT * FROM scan_data_from_quack_client('forged_{number}', "
        f"NULL::STRUCT(a {column_type}))", number)
    message_type, message = session.send_data(f"forged_{number}", batch)
    assert message_type == quack_wire.ERROR_RESPONSE
    assert error in message
    assert conn_b.execute("SELECT 1").fetchone() == (1,)


def test_connect_agrees_on_a_storage_version(servers) -> None:
    _, _, quack_b, _ = servers
    newer = (100 << 32) + 7
    session = quack_wire.Session(quack_b, BEARER, str(newer))
    try:
        assert session.message_type == quack_wire.CONNECTION_RESPONSE
        agreed = session.serenedb_storage_version
        assert 0 < agreed < newer
        assert session.prepare("SELECT 42", 1)[0] != quack_wire.ERROR_RESPONSE
    finally:
        session.close()
    session = quack_wire.Session(quack_b, BEARER, str(agreed))
    session.close()
    assert session.serenedb_storage_version == agreed
    for header, error in (("69", "writes storage version v2.0.0 at newest"),
                          ("abc", "names no storage version")):
        session = quack_wire.Session(quack_b, BEARER, header)
        session.close()
        assert session.message_type == quack_wire.ERROR_RESPONSE
        assert error in session.error
