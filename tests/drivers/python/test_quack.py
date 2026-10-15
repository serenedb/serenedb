"""The quack protocol between two serened processes and from `serened shell`.

Server B serves quack on an HTTP listener (api=quack); server A and the shell
ATTACH it. B runs with a short --http_body_timeout, so a statement that takes
longer than that must still answer its FETCH.
"""

from __future__ import annotations

import base64
import os
import socket
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

import psycopg
import pytest

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)

# A TSan build exits 66 once it has findings to report, and CI reads those from
# the sanitizer logs rather than the exit status, so it is not a test signal.
_CLEAN_EXITS = (0, -15) + ((66,) if os.environ.get("TSAN_OPTIONS") else ())

ENDLESS = "SELECT count(*) FROM range(10000000000000) a"
BEARER = "quack-test-bearer-token"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_tcp(port: int, deadline: float) -> None:
    while True:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=2):
                return
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


def _connect(port: int, deadline: float) -> psycopg.Connection:
    while True:
        try:
            return psycopg.connect(
                host="127.0.0.1", port=port, user="postgres", dbname="postgres",
                autocommit=True,
            )
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


class Server:
    def __init__(self, path: Path, listen: str, *flags: str):
        self.proc = subprocess.Popen(
            [SERENED_BIN, str(path), f"--listen={listen}", *flags],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )

    def stop(self) -> None:
        self.proc.terminate()
        _, stderr = self.proc.communicate(timeout=60)
        assert self.proc.returncode in _CLEAN_EXITS, stderr.decode(
            errors="replace")


@pytest.fixture(scope="module")
def servers(tmp_path_factory: pytest.TempPathFactory):
    pg_a, pg_b, quack_b = _free_port(), _free_port(), _free_port()
    b = Server(tmp_path_factory.mktemp("b"),
               f"postgres://127.0.0.1:{pg_b},"
               f"http://127.0.0.1:{quack_b}?api=quack",
               "--http_body_timeout=1s", f"--auth_bearer_token={BEARER}")
    a = Server(tmp_path_factory.mktemp("a"), f"postgres://127.0.0.1:{pg_a}")
    try:
        deadline = time.monotonic() + 60
        conn_b = _connect(pg_b, deadline)
        conn_a = _connect(pg_a, deadline)
        _wait_tcp(quack_b, deadline)
        conn_b.execute("CREATE TABLE t (id INT, name TEXT)")
        conn_b.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, NULL)")
        conn_b.execute("CREATE ROLE alice LOGIN PASSWORD 'secret'")
        conn_b.execute("CREATE TABLE private (x INT)")
        conn_a.execute(
            f"ATTACH 'quack:127.0.0.1:{quack_b}' AS b (TOKEN 'postgres:')")
        yield conn_a, conn_b, quack_b, pg_a
        conn_a.close()
        conn_b.close()
    finally:
        a.stop()
        b.stop()


def _refresh(conn_a: psycopg.Connection) -> None:
    conn_a.execute("SELECT * FROM quack_clear_cache()")


def _remote(quack_port: int, sql: str, token: str = "postgres:") -> str:
    quoted = sql.replace("'", "''")
    return (f"SELECT * FROM quack_query('quack:127.0.0.1:{quack_port}', "
            f"'{quoted}', token := '{token}')")


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


def test_statement_longer_than_body_timeout(servers) -> None:
    conn_a, _, quack_b, _ = servers
    started = time.monotonic()
    (total,) = conn_a.execute(_remote(
        quack_b, "SELECT sum(a % 7) FROM range(4000000000) r(a)")).fetchone()
    assert time.monotonic() - started > 1
    assert total == sum(i % 7 for i in range(7)) * (4000000000 // 7) + sum(
        range(4000000000 % 7))


def test_request_longer_than_body_timeout(servers) -> None:
    conn_a, _, quack_b, _ = servers
    # The padding spreads the request over many reads, and PREPARE answers
    # only once the statement before the SELECT is done.
    padding = "-- " + "x" * (1 << 20) + "\n"
    started = time.monotonic()
    (total,) = conn_a.execute(_remote(
        quack_b, padding +
        "CREATE OR REPLACE TEMP TABLE slow AS "
        "SELECT sum(a % 7) AS s FROM range(1000000000) r(a); "
        "SELECT s FROM slow")).fetchone()
    assert time.monotonic() - started > 1
    assert total == sum(i % 7 for i in range(7)) * (1000000000 // 7) + sum(
        range(1000000000 % 7))


def test_cancel(servers) -> None:
    conn_a, _, quack_b, pg_a = servers
    with _connect(pg_a, time.monotonic() + 10) as running:
        pid = running.execute("SELECT pg_backend_pid()").fetchone()[0]
        # ends the query even when the cancel does not, so a failure is no hang
        running.execute("SET statement_timeout = '60s'")
        outcome: list[BaseException] = []

        def run() -> None:
            try:
                running.execute(_remote(quack_b, ENDLESS))
            except psycopg.Error as e:
                outcome.append(e)

        worker = threading.Thread(target=run)
        worker.start()
        time.sleep(2)
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
    conn_a, conn_b, quack_b, pg_a = servers
    conn_b.execute(f"CREATE VIEW IF NOT EXISTS endless AS {ENDLESS}")
    _refresh(conn_a)
    statement = {
        "attach": "SELECT * FROM b.public.endless",
        "quack_query": _remote(quack_b, ENDLESS),
        "query": "SELECT * FROM b.query('{}')".format(ENDLESS),
    }[path]
    with _connect(pg_a, time.monotonic() + 10) as conn:
        pid = conn.execute("SELECT pg_backend_pid()").fetchone()[0]
        conn.execute("SET statement_timeout = '1s'")
        outcome: list[BaseException] = []

        def run() -> None:
            try:
                conn.execute(statement)
            except psycopg.Error as e:
                outcome.append(e)

        worker = threading.Thread(target=run)
        worker.start()
        worker.join(timeout=10)
        if worker.is_alive():
            conn_a.execute("SELECT pg_cancel_backend(%s)", (pid,))
            worker.join(timeout=30)
            pytest.fail("statement_timeout did not stop the query")
        assert isinstance(outcome[0], psycopg.errors.QueryCanceled)
        assert "statement timeout" in str(outcome[0])
        stopped_by = time.monotonic() + 5
        while conn_b.execute("SELECT count(*) FROM quack_active_connections() "
                             "WHERE state = 'active'").fetchone()[0]:
            assert time.monotonic() < stopped_by, "the statement goes on on B"
            time.sleep(0.1)
        conn.execute("RESET statement_timeout")
        assert conn.execute(
            "SELECT count(*) FROM b.public.t").fetchone() == (3,)


def test_sessions_run_concurrently(servers) -> None:
    conn_a, conn_b, _, pg_a = servers
    conn_b.execute("CREATE VIEW IF NOT EXISTS slow AS "
                   "SELECT sum(a % 7) AS s FROM range(300000000) r(a)")
    _refresh(conn_a)
    expected = conn_b.execute("SELECT s FROM slow").fetchone()
    results: list[object] = []

    def run() -> None:
        try:
            with _connect(pg_a, time.monotonic() + 10) as conn:
                results.append(
                    conn.execute("SELECT s FROM b.public.slow").fetchone())
        except psycopg.Error as e:
            results.append(e)

    workers = [threading.Thread(target=run) for _ in range(4)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=120)
    assert results == [expected] * 4


def test_sessions_isolated(servers) -> None:
    conn_a, conn_b, _, pg_a = servers
    conn_b.execute("CREATE TABLE iso (x INT)")
    conn_b.execute("CREATE VIEW boom AS SELECT error('boom')::INT AS y")
    _refresh(conn_a)
    deadline = time.monotonic() + 10
    with _connect(pg_a, deadline) as writer, _connect(pg_a, deadline) as other:
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
    conn_a, conn_b, quack_b, pg_a = servers
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
    with _connect(pg_a, time.monotonic() + 10) as conn:
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
    conn_a, conn_b, quack_b, pg_a = servers
    conn_b.execute("CREATE TABLE once AS SELECT * FROM range(3) r(x)")
    _refresh(conn_a)

    def rows() -> int:
        return conn_b.execute("SELECT count(*) FROM once").fetchone()[0]

    with _connect(pg_a, time.monotonic() + 10) as conn:
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
    conn_a, conn_b, quack_b, pg_a = servers

    def sessions() -> int:
        return conn_b.execute(
            "SELECT count(*) FROM quack_active_connections()").fetchone()[0]

    before = sessions()
    with _connect(pg_a, time.monotonic() + 10) as conn:
        conn.execute(
            f"ATTACH 'quack:127.0.0.1:{quack_b}' AS detached (TOKEN 'postgres:')")
        conn.execute("SELECT count(*) FROM detached.public.t")
        assert sessions() == before + 1
        conn.execute("DETACH detached")
        assert sessions() == before


def test_quack_cancel(servers) -> None:
    conn_a, conn_b, _, pg_a = servers
    conn_b.execute(f"CREATE VIEW IF NOT EXISTS endless AS {ENDLESS}")
    _refresh(conn_a)
    deadline = time.monotonic() + 10
    with _connect(pg_a, deadline) as running, _connect(pg_a, deadline) as other:
        pid = running.execute("SELECT pg_backend_pid()").fetchone()[0]
        (target,) = running.execute(
            "SELECT quack_connection_id('b')").fetchone()
        for _ in range(2):
            outcome: list[BaseException] = []

            def run() -> None:
                try:
                    running.execute("SELECT * FROM b.public.endless")
                except psycopg.Error as e:
                    outcome.append(e)

            worker = threading.Thread(target=run)
            worker.start()
            time.sleep(1)
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
    result = subprocess.run(
        [SERENED_BIN, "shell", ":memory:", "-csv", "-c",
         f"ATTACH 'quack:127.0.0.1:{quack_b}' AS b (TOKEN 'postgres:'); "
         "CREATE TABLE b.from_shell (x INT); "
         "INSERT INTO b.from_shell SELECT * FROM range(10); "
         "SELECT count(*) FROM b.from_shell; "
         "EXPLAIN CREATE TABLE b.explained AS FROM range(10); "
         "CREATE TABLE b.ctas AS FROM range(10000000); "
         "SELECT count(*) FROM b.ctas; "
         "SELECT count(*) FROM b.t JOIN b.t AS u ON b.t.id = u.id; "
         "SELECT id, name FROM b.public.t ORDER BY id;"],
        capture_output=True, text=True, timeout=60,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )
    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines[:5] == ["Count", "Count", "10", "count", "10"]
    assert lines[-10:] == ["Count", "10000000", "count", "10000000",
                           "count", "3", "id,name", "1,a", "2,b", "3,NULL"]
