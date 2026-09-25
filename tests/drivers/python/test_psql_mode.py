"""`serened psql` subcommand against the harness's pg-wire endpoint."""

from __future__ import annotations

import os
import fcntl
import pty
import re
import select
import struct
import subprocess
import termios
import time
import uuid
from pathlib import Path

import pytest
from spec_loader import conn_kwargs  # type: ignore[import-untyped]


SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)


def _have_binary() -> bool:
    return Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)


pytestmark = pytest.mark.skipif(
    not _have_binary(),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


def _run(args: list[str], *, env_extra: dict[str, str] | None = None,
         input_text: str | None = None,
         timeout: float = 20.0) -> subprocess.CompletedProcess:
    env = {**os.environ, "NO_COLOR": "1", "TERM": "dumb"}
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [SERENED_BIN, "psql", *args],
        input=input_text,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


def _kw() -> dict[str, str]:
    k = conn_kwargs()
    return {
        "host": str(k["host"]),
        "port": str(k["port"]),
        "dbname": str(k["dbname"]),
        "user": str(k["user"]),
    }


def test_psql_help() -> None:
    r = _run(["--help"])
    assert r.returncode == 0
    assert "serened psql" in r.stdout
    assert "--host=HOSTNAME" in r.stdout
    assert "--port=PORT" in r.stdout


def test_psql_version() -> None:
    r = _run(["-V"])
    assert r.returncode == 0
    assert r.stdout.startswith("SereneDB ")


def test_psql_command_short_flags() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-U", k["user"],
              "-d", k["dbname"], "-c", "SELECT 1+1 AS sum;"])
    assert r.returncode == 0, r.stderr
    assert " 2 " in r.stdout, f"unexpected stdout: {r.stdout!r}"


def test_psql_command_long_flags() -> None:
    k = _kw()
    r = _run([f"--host={k['host']}", f"--port={k['port']}",
              f"--username={k['user']}", f"--dbname={k['dbname']}",
              "--command=SELECT 10+20 AS sum;"])
    assert r.returncode == 0, r.stderr
    assert " 30 " in r.stdout, f"unexpected stdout: {r.stdout!r}"


def test_psql_positional_dbname_username() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], k["dbname"], k["user"],
              "-c", "SELECT 7*6 AS answer;"])
    assert r.returncode == 0, r.stderr
    assert " 42 " in r.stdout


def test_psql_env_vars() -> None:
    k = _kw()
    r = _run(["-c", "SELECT 100+1 AS x;"], env_extra={
        "PGHOST": k["host"], "PGPORT": k["port"],
        "PGUSER": k["user"], "PGDATABASE": k["dbname"],
    })
    assert r.returncode == 0, r.stderr
    assert " 101 " in r.stdout


def test_psql_user_default_falls_back_to_os_user() -> None:
    # psql defaults USERNAME to the OS user (via getpwuid / $USER); mirror
    # that. duckdb_databases().path exposes the libpq DSN we synthesised
    # so we can read the user value back directly. Login as an unknown role
    # is rejected, so the sentinel role must exist for the connection to
    # come up.
    import psycopg

    k = _kw()

    def _admin(sql: str) -> None:
        with psycopg.connect(**conn_kwargs(), autocommit=True) as c:
            c.execute(sql)

    # The psql lane's unfiltered \du goldens list every role in the cluster;
    # hold the cross-lane lock for as long as the sentinel exists.
    lock = open("/tmp/sdb-drivers-global-roles.lock", "w")
    fcntl.flock(lock, fcntl.LOCK_EX)
    _admin("DROP ROLE IF EXISTS zzz_sentinel_user;")
    _admin("CREATE ROLE zzz_sentinel_user LOGIN;")
    try:
        env = {**os.environ, "NO_COLOR": "1", "TERM": "dumb"}
        env.pop("PGUSER", None)  # so the $USER fallback actually runs
        env.update({
            "PGHOST": k["host"], "PGPORT": k["port"], "PGDATABASE": k["dbname"],
            "USER": "zzz_sentinel_user",
        })
        r = subprocess.run(
            [SERENED_BIN, "psql", "-c",
             "SELECT path FROM duckdb_databases() WHERE database_name='postgres';"],
            capture_output=True, text=True, timeout=20, env=env,
        )
        assert r.returncode == 0, r.stderr
        assert "user=zzz_sentinel_user" in r.stdout, \
            f"OS-user fallback didn't reach the DSN:\nstdout={r.stdout!r}"
    finally:
        _admin("DROP ROLE zzz_sentinel_user;")
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


def test_psql_list_databases() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-U", k["user"],
              "-d", k["dbname"], "-l"])
    assert r.returncode == 0, r.stderr
    # SHOW DATABASES emits at least the attached one.
    assert k["dbname"] in r.stdout, f"missing dbname in stdout: {r.stdout!r}"


def test_psql_no_password_flag_accepted() -> None:
    # -w / -W are no-ops in serened (no auth) but must not break parsing.
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-U", k["user"],
              "-d", k["dbname"], "-w", "-c", "SELECT 1;"])
    assert r.returncode == 0, r.stderr


def test_psql_round_trip() -> None:
    # CREATE / INSERT (postgres_scanner -> COPY FROM STDIN) / SELECT / DROP
    # against the attached server -- the original regression that motivated
    # the COPY STDIN fix, exercised through `serened psql`.
    k = _kw()
    table = f"psql_probe_{uuid.uuid4().hex[:10]}"
    sql = (
        f'CREATE TABLE public."{table}"(x INT, name VARCHAR); '
        f'INSERT INTO public."{table}" '
        "VALUES (1,'one'),(2,'two'),(3,'three'); "
        f'SELECT count(*), sum(x), string_agg(name, \',\' ORDER BY x) '
        f'FROM public."{table}"; '
        f'DROP TABLE public."{table}";'
    )
    r = _run(["-h", k["host"], "-p", k["port"], "-U", k["user"],
              "-d", k["dbname"], "-c", sql])
    assert r.returncode == 0, f"stderr={r.stderr!r}"
    assert " 3 " in r.stdout and " 6 " in r.stdout, \
        f"row count/sum missing: {r.stdout!r}"
    assert "one,two,three" in r.stdout, \
        f"string_agg missing: {r.stdout!r}"


def test_psql_create_drop_database() -> None:
    # CREATE DATABASE / DROP DATABASE are rewritten by the parser to
    # ATTACH (TYPE serenedb) / DETACH -- local catalog ops the client shell
    # cannot run (it has no serenedb storage extension). serened psql must
    # forward them to the attached server instead of failing locally with
    # `Extension "serenedb.duckdb_extension" not found`.
    k = _kw()
    db = f"psql_cdb_{uuid.uuid4().hex[:10]}"

    def psql(sql: str, dbname: str | None = None):
        return _run(["-h", k["host"], "-p", k["port"], "-U", k["user"],
                     "-d", dbname or k["dbname"], "-c", sql])

    # CREATE forwards to the server -- no local serenedb.duckdb_extension error.
    r = psql(f'CREATE DATABASE "{db}";')
    assert r.returncode == 0, f"stderr={r.stderr!r}"
    assert "serenedb.duckdb_extension" not in r.stderr, \
        f"statement ran locally instead of being forwarded: {r.stderr!r}"

    # IF NOT EXISTS is idempotent.
    r = psql(f'CREATE DATABASE IF NOT EXISTS "{db}";')
    assert r.returncode == 0, f"stderr={r.stderr!r}"

    # A plain re-create surfaces the server's duplicate error.
    r = psql(f'CREATE DATABASE "{db}";')
    assert r.returncode != 0, f"expected duplicate error, stdout={r.stdout!r}"
    assert "already exists" in r.stderr, \
        f"missing duplicate-database error: {r.stderr!r}"

    # The database really exists on the server: connect straight to it.
    r = psql("SELECT 1 AS ok;", dbname=db)
    assert r.returncode == 0, f"could not connect to created db: {r.stderr!r}"

    # DROP forwards too; afterwards the database is gone (connect fails).
    r = psql(f'DROP DATABASE "{db}";')
    assert r.returncode == 0, f"stderr={r.stderr!r}"
    r = psql("SELECT 1;", dbname=db)
    assert r.returncode != 0, \
        f"database still connectable after DROP: {r.stdout!r}"


def test_psql_unknown_flag_passes_through() -> None:
    # --csv is a duckdb shell flag; serened psql should leave it alone so
    # the embedded shell sees it.
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-U", k["user"],
              "-d", k["dbname"], "--csv", "-c", "SELECT 1 AS a, 2 AS b;"])
    assert r.returncode == 0, r.stderr
    assert "a,b" in r.stdout or "1,2" in r.stdout, \
        f"--csv didn't reach the shell: {r.stdout!r}"


def _conn_args() -> list[str]:
    k = _kw()
    return ["-h", k["host"], "-p", k["port"], "-U", k["user"], "-d", k["dbname"]]


def test_psql_no_align_short() -> None:
    r = _run(_conn_args() + ["-A", "-c", "SELECT 1 AS a, 2 AS b;"])
    assert r.returncode == 0, r.stderr
    assert "a|b" in r.stdout and "1|2" in r.stdout, \
        f"-A didn't produce unaligned output: {r.stdout!r}"


def test_psql_no_align_long() -> None:
    r = _run(_conn_args() + ["--no-align", "-c", "SELECT 1 AS a;"])
    assert r.returncode == 0, r.stderr
    assert "1" in r.stdout


def test_psql_field_separator() -> None:
    r = _run(_conn_args() + ["-A", "-F", ";", "-c",
                              "SELECT 1 AS a, 2 AS b;"])
    assert r.returncode == 0, r.stderr
    assert "a;b" in r.stdout and "1;2" in r.stdout, \
        f"-F didn't change the separator: {r.stdout!r}"


def test_psql_field_separator_long_form() -> None:
    r = _run(_conn_args() + ["-A", "--field-separator=,", "-c",
                              "SELECT 1 AS a, 2 AS b;"])
    assert r.returncode == 0, r.stderr
    assert "a,b" in r.stdout and "1,2" in r.stdout


def test_psql_html() -> None:
    r = _run(_conn_args() + ["-H", "-c", "SELECT 1 AS a;"])
    assert r.returncode == 0, r.stderr
    assert "<tr>" in r.stdout and "</tr>" in r.stdout, \
        f"-H didn't produce HTML: {r.stdout!r}"


def test_psql_tuples_only_suppresses_header() -> None:
    r = _run(_conn_args() + ["-t", "-c", "SELECT 1 AS a, 2 AS b;"])
    assert r.returncode == 0, r.stderr
    lines = [l for l in r.stdout.splitlines() if l.strip()]
    assert lines == ["1|2"], \
        f"-t should emit data only: {r.stdout!r}"


def test_psql_expanded() -> None:
    r = _run(_conn_args() + ["-x", "-c", "SELECT 1 AS a, 2 AS b;"])
    assert r.returncode == 0, r.stderr
    # Line mode renders one field per line: "a = 1\nb = 2\n" (with padding).
    assert "a" in r.stdout and "= 1" in r.stdout and "= 2" in r.stdout, \
        f"-x didn't produce expanded output: {r.stdout!r}"


def test_psql_echo_queries() -> None:
    r = _run(_conn_args() + ["-e", "-c", "SELECT 1+1;"])
    assert r.returncode == 0, r.stderr
    assert "SELECT 1+1" in r.stdout, \
        f"-e should echo the command: {r.stdout!r}"


def test_psql_echo_all_short() -> None:
    r = _run(_conn_args() + ["-a", "-c", "SELECT 1+1;"])
    assert r.returncode == 0, r.stderr
    assert "SELECT 1+1" in r.stdout


def test_psql_record_separator() -> None:
    r = _run(_conn_args() + ["-A", "-R", "|||", "-c",
                              "SELECT 1 AS a UNION ALL SELECT 2;"])
    assert r.returncode == 0, r.stderr
    assert "|||" in r.stdout, \
        f"-R didn't change the record separator: {r.stdout!r}"


def test_psql_output_to_file(tmp_path: Path) -> None:
    out = tmp_path / "psql_out.txt"
    r = _run(_conn_args() + ["-o", str(out), "-c",
                              "SELECT 42 AS answer;"])
    assert r.returncode == 0, r.stderr
    body = out.read_text()
    assert "42" in body, f"output file empty/missing: {body!r}"


def test_psql_output_to_file_long_form(tmp_path: Path) -> None:
    out = tmp_path / "psql_out2.txt"
    r = _run(_conn_args() + [f"--output={out}", "-c",
                              "SELECT 7*6 AS answer;"])
    assert r.returncode == 0, r.stderr
    assert "42" in out.read_text()


def test_psql_no_psqlrc() -> None:
    r = _run(_conn_args() + ["-X", "-c", "SELECT 1;"])
    assert r.returncode == 0, r.stderr


# ---- short/long form parity for flags not already exercised above ----------

def test_psql_file_short(tmp_path: Path) -> None:
    sql_file = tmp_path / "q.sql"
    sql_file.write_text("SELECT 99 AS x;\n")
    r = _run(_conn_args() + ["-f", str(sql_file)])
    assert r.returncode == 0, r.stderr
    assert " 99 " in r.stdout


def test_psql_file_long(tmp_path: Path) -> None:
    sql_file = tmp_path / "q.sql"
    sql_file.write_text("SELECT 88 AS x;\n")
    r = _run(_conn_args() + [f"--file={sql_file}"])
    assert r.returncode == 0, r.stderr
    assert " 88 " in r.stdout


def test_psql_list_long() -> None:
    r = _run(_conn_args() + ["--list"])
    assert r.returncode == 0, r.stderr
    assert _kw()["dbname"] in r.stdout


def test_psql_version_long() -> None:
    r = _run(["--version"])
    assert r.returncode == 0
    assert r.stdout.startswith("SereneDB ")


def test_psql_question_mark_is_help() -> None:
    r = _run(["-?"])
    assert r.returncode == 0
    assert "serened psql" in r.stdout
    assert "Connection options" in r.stdout


def test_psql_password_force_short() -> None:
    # -W must be accepted (no-op since serened has no auth).
    r = _run(_conn_args() + ["-W", "-c", "SELECT 1;"])
    assert r.returncode == 0, r.stderr


def test_psql_password_force_long() -> None:
    r = _run(_conn_args() + ["--password", "-c", "SELECT 1;"])
    assert r.returncode == 0, r.stderr


def test_psql_no_password_long() -> None:
    r = _run(_conn_args() + ["--no-password", "-c", "SELECT 1;"])
    assert r.returncode == 0, r.stderr


def test_psql_tuples_only_long() -> None:
    r = _run(_conn_args() + ["--tuples-only", "-c",
                              "SELECT 1 AS a, 2 AS b;"])
    assert r.returncode == 0, r.stderr
    lines = [l for l in r.stdout.splitlines() if l.strip()]
    assert lines == ["1|2"], f"unexpected stdout: {r.stdout!r}"


def test_psql_expanded_long() -> None:
    r = _run(_conn_args() + ["--expanded", "-c",
                              "SELECT 1 AS a, 2 AS b;"])
    assert r.returncode == 0, r.stderr
    assert "= 1" in r.stdout and "= 2" in r.stdout


def test_psql_echo_all_long() -> None:
    r = _run(_conn_args() + ["--echo-all", "-c", "SELECT 1+1;"])
    assert r.returncode == 0, r.stderr
    assert "SELECT 1+1" in r.stdout


def test_psql_echo_queries_long() -> None:
    r = _run(_conn_args() + ["--echo-queries", "-c", "SELECT 1+1;"])
    assert r.returncode == 0, r.stderr
    assert "SELECT 1+1" in r.stdout


def test_psql_record_separator_long() -> None:
    r = _run(_conn_args() + ["-A", "--record-separator=|||",
                              "-c", "SELECT 1 UNION ALL SELECT 2;"])
    assert r.returncode == 0, r.stderr
    assert "|||" in r.stdout


def test_psql_no_psqlrc_long() -> None:
    r = _run(_conn_args() + ["--no-psqlrc", "-c", "SELECT 1;"])
    assert r.returncode == 0, r.stderr


def test_psql_csv_long() -> None:
    r = _run(_conn_args() + ["--csv", "-c", "SELECT 1 AS a, 2 AS b;"])
    assert r.returncode == 0, r.stderr
    assert "a,b" in r.stdout and "1,2" in r.stdout


def test_psql_html_long() -> None:
    r = _run(_conn_args() + ["--html", "-c", "SELECT 1 AS a;"])
    assert r.returncode == 0, r.stderr
    assert "<tr>" in r.stdout


def test_psql_no_align_long_only() -> None:
    # exercises --no-align without -A as the form.
    r = _run(_conn_args() + ["--no-align", "-c", "SELECT 1 AS a, 2 AS b;"])
    assert r.returncode == 0, r.stderr
    assert "a|b" in r.stdout and "1|2" in r.stdout


# ---- meaningful combinations ----------------------------------------------

def test_psql_combo_A_t() -> None:
    # unaligned + tuples-only -> data rows only with default '|' separator.
    r = _run(_conn_args() + ["-A", "-t", "-c",
                              "SELECT 1 AS a, 2 AS b;"])
    assert r.returncode == 0, r.stderr
    lines = [l for l in r.stdout.splitlines() if l.strip()]
    assert lines == ["1|2"], f"unexpected: {r.stdout!r}"


def test_psql_combo_A_t_F() -> None:
    # unaligned + tuples-only + custom separator: bare CSV-style rows.
    r = _run(_conn_args() + ["-A", "-t", "-F", ",",
                              "-c", "SELECT 'x','y' UNION ALL SELECT 'a','b';"])
    assert r.returncode == 0, r.stderr
    body = [l for l in r.stdout.splitlines() if l.strip()]
    assert sorted(body) == sorted(["x,y", "a,b"]), \
        f"unexpected stdout: {r.stdout!r}"


def test_psql_combo_echo_one_shot() -> None:
    # -e + -c: command echoed, then result.
    r = _run(_conn_args() + ["-e", "-c", "SELECT 42 AS answer;"])
    assert r.returncode == 0, r.stderr
    out = r.stdout
    assert "SELECT 42 AS answer" in out
    assert "42" in out
    # Echo should appear before the result.
    assert out.index("SELECT 42 AS answer") < out.index("answer"), \
        f"echo didn't precede result: {out!r}"


def test_psql_combo_csv_one_shot_quoting() -> None:
    r = _run(_conn_args() + ["--csv", "-c",
                              "SELECT 'hello, world' AS msg;"])
    assert r.returncode == 0, r.stderr
    # CSV mode must quote a field that contains the comma.
    assert '"hello, world"' in r.stdout, f"unexpected csv: {r.stdout!r}"


def test_psql_combo_o_with_f(tmp_path: Path) -> None:
    sql_file = tmp_path / "q.sql"
    sql_file.write_text("SELECT 'A' AS s UNION ALL SELECT 'B';\n")
    out = tmp_path / "captured.txt"
    r = _run(_conn_args() + ["-o", str(out), "-f", str(sql_file)])
    assert r.returncode == 0, r.stderr
    body = out.read_text()
    assert "A" in body and "B" in body


# ---- defaults & env-var precedence ----------------------------------------

def test_psql_default_host_localhost() -> None:
    # With no -h and no $PGHOST, the synthesised DSN must include
    # host=localhost. Force a connection failure (bogus port) so the
    # DSN echoes back via DuckDB's IO Error -- this avoids depending on
    # whether a server happens to listen on localhost in the test env.
    env = {**os.environ, "NO_COLOR": "1", "TERM": "dumb"}
    env.pop("PGHOST", None)
    env["PGPORT"] = "1"
    r = subprocess.run(
        [SERENED_BIN, "psql", "-c", "SELECT 1;"],
        capture_output=True, text=True, timeout=20, env=env,
    )
    assert "host=localhost" in (r.stdout + r.stderr), \
        f"default host wasn't localhost: stdout={r.stdout!r} stderr={r.stderr!r}"


def test_psql_default_port_5432() -> None:
    # Default port is 5432 -- assert it lands in the DSN even when no
    # server listens there (we never actually connect).
    env = {**os.environ, "NO_COLOR": "1", "TERM": "dumb"}
    env.pop("PGPORT", None)
    # Print the planned ATTACH path. We use --echo so the synthesised
    # -cmd is visible, but the easier route: trigger the connection and
    # let DuckDB's IO Error echo back the DSN. Either way the port
    # value lives in the error message.
    env["PGHOST"] = "127.0.0.1"
    r = subprocess.run(
        [SERENED_BIN, "psql", "-h", "127.0.0.1", "-c", "SELECT 1;"],
        capture_output=True, text=True, timeout=20, env=env,
    )
    # Server isn't running on 5432 in the test fixture (it's on 6161),
    # so we expect failure and a port=5432 mention in the error.
    assert "port=5432" in (r.stdout + r.stderr), \
        f"default port wasn't 5432: stdout={r.stdout!r} stderr={r.stderr!r}"


def test_psql_flag_overrides_env() -> None:
    # -p on the command line must override $PGPORT.
    k = _kw()
    env_extra = {"PGHOST": k["host"], "PGPORT": "1",  # bogus env port
                 "PGUSER": k["user"], "PGDATABASE": k["dbname"]}
    r = _run(["-p", k["port"], "-c", "SELECT 1+1 AS sum;"],
             env_extra=env_extra)
    assert r.returncode == 0, r.stderr
    assert " 2 " in r.stdout


def test_psql_pguser_overrides_user_env() -> None:
    # PGUSER must beat $USER.
    k = _kw()
    env_extra = {
        "PGHOST": k["host"], "PGPORT": k["port"], "PGDATABASE": k["dbname"],
        "PGUSER": k["user"],
        "USER": "this_should_be_ignored",
    }
    r = _run(["-c", "SELECT path FROM duckdb_databases() "
                    "WHERE database_name <> 'memory' AND path IS NOT NULL;"],
             env_extra=env_extra)
    assert r.returncode == 0, r.stderr
    assert f"user={k['user']}" in r.stdout
    assert "this_should_be_ignored" not in r.stdout


# ---- error paths -----------------------------------------------------------

def test_psql_missing_required_argument() -> None:
    r = _run(["-h"])  # -h without a value
    assert r.returncode != 0
    # Shell-side error wording: "Missing Argument Error: Argument '-h' needs ..."
    err = r.stdout + r.stderr
    assert "Missing Argument" in err or "needs" in err, \
        f"unexpected error text: {err!r}"


# ---- catch-all: combined help still has both halves -----------------------

def test_psql_help_has_all_sections() -> None:
    r = _run(["--help"])
    assert r.returncode == 0
    assert "serened psql" in r.stdout
    # all five sections present in psql mode
    for header in ["General options:", "Input and output options:",
                   "Output format options:", "Database options:",
                   "Connection options:"]:
        assert header in r.stdout, f"missing section: {header}"
    # database options listed under their own section now (no tail)
    assert "storage-version" in r.stdout
    # footer
    assert "Report bugs at" in r.stdout
    assert "serenedb.com" in r.stdout or "github.com/serenedb" in r.stdout


# ---- alternate help targets (-?, --help=interactive[-all]) -----------------

def test_psql_help_interactive_lists_dot_commands() -> None:
    # --help=interactive prints `.help` (dot commands), then exits.
    r = _run(["--help=interactive"])
    assert r.returncode == 0
    # `.help` always lists at least these dot-commands.
    assert ".about" in r.stdout
    assert ".help" in r.stdout
    # --help=interactive must NOT contain the psql Usage/Connection text.
    assert "Connection options" not in r.stdout


def test_psql_help_interactive_all_is_verbose() -> None:
    # --help=interactive-all uses `.help --all`, which lists more entries
    # than the short form. We just assert it's strictly longer.
    short = _run(["--help=interactive"]).stdout.splitlines()
    long = _run(["--help=interactive-all"]).stdout.splitlines()
    assert len(long) > len(short)


def test_psql_help_unknown_target_errors() -> None:
    # --help=anything-else should fall back to the long-form parser,
    # which can't pass a value to a 0-arg --help. Expect a clear error.
    r = _run(["--help=variables"])
    assert r.returncode != 0
    err = r.stdout + r.stderr
    assert "inline value" in err or "Unrecognized option" in err, err


# ---- input/output flags newly mapped --------------------------------------

def test_psql_quiet_suppresses_startup_banner() -> None:
    # -q doesn't affect -c output (no banner is emitted there anyway),
    # but it must be accepted and not change the query result.
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-q", "-c", "SELECT 1+1 AS s;"])
    assert r.returncode == 0, r.stderr
    assert " 2 " in r.stdout


def test_psql_quiet_long_form() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "--quiet", "-c", "SELECT 5;"])
    assert r.returncode == 0, r.stderr
    assert " 5 " in r.stdout


def test_psql_no_readline_accepted() -> None:
    # -n disables linenoise. With -c it shouldn't matter, but the option
    # must parse cleanly.
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-n", "-c", "SELECT 3;"])
    assert r.returncode == 0, r.stderr
    assert " 3 " in r.stdout


def test_psql_echo_errors_short() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-b", "-c", "SELECT 4;"])
    assert r.returncode == 0, r.stderr
    # `-b` maps to plain echo, which prints the statement before result.
    assert "SELECT 4" in r.stdout


def test_psql_echo_errors_long() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "--echo-errors", "-c", "SELECT 6;"])
    assert r.returncode == 0, r.stderr
    assert "SELECT 6" in r.stdout


# ---- single-transaction ----------------------------------------------------

def test_psql_single_transaction_wraps_command() -> None:
    # -1 wraps the -c body in BEGIN/COMMIT. With a valid statement the
    # result is unchanged and the exit code is 0.
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-1", "-c", "SELECT 11 AS v;"])
    assert r.returncode == 0, r.stderr
    assert " 11 " in r.stdout


def test_psql_single_transaction_long_form() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "--single-transaction",
              "-c", "SELECT 22 AS v;"])
    assert r.returncode == 0, r.stderr
    assert " 22 " in r.stdout


# ---- zero-byte separators --------------------------------------------------

def test_psql_field_separator_zero() -> None:
    # -A + -z gives NUL-separated columns (psql convention for xargs -0).
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-A", "-z", "-c", "SELECT 1, 2, 3;"])
    assert r.returncode == 0, r.stderr
    # the row line carries two NUL bytes between the three values.
    assert "\x00" in r.stdout
    # rendered values still appear.
    assert "1" in r.stdout and "3" in r.stdout


def test_psql_field_separator_zero_long_form() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-A", "--field-separator-zero",
              "-c", "SELECT 'a', 'b';"])
    assert r.returncode == 0, r.stderr
    assert "\x00" in r.stdout


def test_psql_record_separator_zero() -> None:
    # -A + -0 gives NUL-terminated rows.
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-A", "-0", "-c", "SELECT 7 AS v;"])
    assert r.returncode == 0, r.stderr
    assert "\x00" in r.stdout


def test_psql_record_separator_zero_long_form() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-A", "--record-separator-zero",
              "-c", "SELECT 8;"])
    assert r.returncode == 0, r.stderr
    assert "\x00" in r.stdout


# ---- accepted-and-ignored psql flags --------------------------------------

@pytest.mark.parametrize("flag,arg", [
    ("-E", None),
    ("--echo-hidden", None),
    ("-L", "/tmp/_serened_psql_log.txt"),
    ("--log-file", "/tmp/_serened_psql_log.txt"),
    ("-s", None),
    ("--single-step", None),
    ("-S", None),
    ("--single-line", None),
    # -P/-T moved to dedicated tests below; -T still accept-and-ignore.
    ("-T", "border=1"),
    ("--table-attr", "width=100"),
])
def test_psql_accepted_and_ignored(flag: str, arg: str | None) -> None:
    k = _kw()
    extra = [flag] + ([arg] if arg is not None else [])
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], *extra, "-c", "SELECT 99 AS v;"])
    assert r.returncode == 0, r.stderr
    assert " 99 " in r.stdout


# ---- --pset implementation (format/expanded/null/tuples_only/pager) -------

def test_pset_format_csv_uses_commas() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "format=csv",
              "-c", "SELECT 1 AS id, 'a' AS name;"])
    assert r.returncode == 0, r.stderr
    # csv: header + row, both comma-separated.
    assert "id,name" in r.stdout
    assert "1,a" in r.stdout
    # no pipe (which is `list` / `unaligned`)
    assert "|" not in r.stdout


def test_pset_format_unaligned_uses_pipes() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "format=unaligned",
              "-c", "SELECT 1 AS id, 'a' AS name;"])
    assert r.returncode == 0, r.stderr
    assert "id|name" in r.stdout
    assert "1|a" in r.stdout


def test_pset_format_html_wraps_rows() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "format=html",
              "-c", "SELECT 1 AS id;"])
    assert r.returncode == 0, r.stderr
    assert "<th>id</th>" in r.stdout
    assert "<td>1</td>" in r.stdout


def test_pset_expanded_on() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "expanded=on",
              "-c", "SELECT 1 AS id, 'a' AS name;"])
    assert r.returncode == 0, r.stderr
    # line mode: " id = 1\nname = a"
    assert "id = 1" in r.stdout
    assert "name = a" in r.stdout


def test_pset_expanded_no_value_toggles_on() -> None:
    # `-P expanded` (no value) starts off -> toggles to on.
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "expanded",
              "-c", "SELECT 1 AS id;"])
    assert r.returncode == 0, r.stderr
    assert "id = 1" in r.stdout


def test_pset_null_string() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "--pset=null=(null!)",
              "-c", "SELECT NULL::INTEGER;"])
    assert r.returncode == 0, r.stderr
    assert "(null!)" in r.stdout
    # default "NULL" replaced
    assert " NULL " not in r.stdout


def test_pset_tuples_only_suppresses_header() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "tuples_only=on",
              "-c", "SELECT 7 AS myhdr;"])
    assert r.returncode == 0, r.stderr
    assert "7" in r.stdout
    assert "myhdr" not in r.stdout


def test_pset_pager_off_runs_normally() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "pager=off",
              "-c", "SELECT 9 AS v;"])
    assert r.returncode == 0, r.stderr
    assert " 9 " in r.stdout


def test_pset_unknown_key_warns_but_succeeds() -> None:
    # unknown key (e.g. `border`) -> warning to stderr, query still runs.
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "border=2",
              "-c", "SELECT 5 AS v;"])
    assert r.returncode == 0, r.stderr
    assert " 5 " in r.stdout
    assert "Warning" in (r.stderr + r.stdout)
    assert "border" in (r.stderr + r.stdout)


def test_pset_unsupported_format_warns() -> None:
    # `format=asciidoc` is recognised key but unsupported value.
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "format=asciidoc",
              "-c", "SELECT 8 AS v;"])
    assert r.returncode == 0, r.stderr
    assert " 8 " in r.stdout
    assert "Warning" in (r.stderr + r.stdout)
    assert "asciidoc" in (r.stderr + r.stdout)


def test_pset_long_form() -> None:
    # --pset=key=value form (vs short -P key=value).
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "--pset=format=csv",
              "-c", "SELECT 1 AS x;"])
    assert r.returncode == 0, r.stderr
    assert "x\n1" in r.stdout or "x\r\n1" in r.stdout


def test_pset_format_accepts_duckdb_modes() -> None:
    k = _kw()
    # markdown is a duckdb mode psql doesn't have; --pset format=markdown
    # should still switch us into it.
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "format=markdown",
              "-c", "SELECT 1 AS id;"])
    assert r.returncode == 0, r.stderr
    # markdown table header marker
    assert "|---" in r.stdout or "| --" in r.stdout


def test_pset_format_box_is_duckdb_mode() -> None:
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "format=box",
              "-c", "SELECT 1 AS id;"])
    assert r.returncode == 0, r.stderr
    # box mode uses unicode box-drawing
    assert "┌" in r.stdout


def test_pset_format_psql_alias_aligned() -> None:
    # psql `aligned` should map to duckbox (default), which renders a
    # unicode box with a type row.
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "format=aligned",
              "-c", "SELECT 1 AS id;"])
    assert r.returncode == 0, r.stderr
    assert "int32" in r.stdout  # duckbox shows type row


def test_pset_format_warning_mentions_both_namespaces() -> None:
    # unsupported format -> warning lists both psql and duckdb names.
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "format=invalidxyz",
              "-c", "SELECT 1;"])
    assert r.returncode == 0, r.stderr
    msg = r.stdout + r.stderr
    assert "Warning" in msg
    assert "psql" in msg and "duckdb" in msg


def test_shell_command_long_alias() -> None:
    # shell mode accepts --command as an alias for -c.
    r = subprocess.run(
        [SERENED_BIN, "shell", "--command=SELECT 42 AS v;"],
        capture_output=True, text=True, timeout=20.0,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )
    assert r.returncode == 0, r.stderr
    assert "42" in r.stdout


def test_shell_file_long_alias(tmp_path: Path) -> None:
    # shell mode accepts --file as an alias for -f.
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("SELECT 'hello' AS greeting;\n")
    r = subprocess.run(
        [SERENED_BIN, "shell", f"--file={sql_file}"],
        capture_output=True, text=True, timeout=20.0,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )
    assert r.returncode == 0, r.stderr
    assert "hello" in r.stdout


def test_shell_short_V_alias() -> None:
    # shell mode accepts -V as an alias for --version.
    r = subprocess.run(
        [SERENED_BIN, "shell", "-V"],
        capture_output=True, text=True, timeout=20.0,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.startswith("SereneDB ")


# ---- shell mode (no auto-connect, accepts psql-style flags) ---------------

def _run_shell(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [SERENED_BIN, "shell", *args],
        capture_output=True, text=True, timeout=20.0,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )


def test_shell_help_has_all_sections_except_connection() -> None:
    r = _run_shell(["--help"])
    assert r.returncode == 0, r.stderr
    assert "serened shell" in r.stdout
    for header in ["General options:", "Input and output options:",
                   "Output format options:", "Database options:"]:
        assert header in r.stdout, f"missing section: {header}"
    # shell mode does NOT auto-connect, so no Connection section.
    assert "Connection options:" not in r.stdout


def test_shell_rejects_connection_options() -> None:
    # --host / --port etc. only exist in psql mode.
    r = _run_shell(["--host=127.0.0.1"])
    assert r.returncode != 0
    assert "Unrecognized option" in (r.stdout + r.stderr)


def test_shell_accepts_psql_no_align() -> None:
    # psql-style -A / --no-align works in shell mode too.
    r = _run_shell(["-A", "-c", "SELECT 1 AS id, 'a' AS name;"])
    assert r.returncode == 0, r.stderr
    assert "id|name" in r.stdout
    assert "1|a" in r.stdout


def test_shell_accepts_psql_expanded() -> None:
    # psql-style -x / --expanded works in shell mode too.
    r = _run_shell(["-x", "-c", "SELECT 1 AS id;"])
    assert r.returncode == 0, r.stderr
    assert "id = 1" in r.stdout


def test_shell_accepts_psql_pset() -> None:
    # psql-style -P / --pset works in shell mode too.
    r = _run_shell(["-P", "format=csv", "-c", "SELECT 1 AS id, 'a' AS name;"])
    assert r.returncode == 0, r.stderr
    assert "id,name" in r.stdout
    assert "1,a" in r.stdout


def test_shell_accepts_psql_tuples_only() -> None:
    # psql-style -t / --tuples-only works in shell mode too.
    r = _run_shell(["-t", "-c", "SELECT 7 AS hdr;"])
    assert r.returncode == 0, r.stderr
    assert "7" in r.stdout
    assert "hdr" not in r.stdout


def test_shell_does_not_auto_attach() -> None:
    # shell mode should NOT try to attach to a pg-wire endpoint.
    # passing a fake host should still error -- but on the SQL side
    # (no such database/table), not at the ATTACH stage.
    r = _run_shell(["-c", "SELECT 1;"])
    assert r.returncode == 0, r.stderr
    assert "1" in r.stdout
    # no ATTACH-related error in stderr
    assert "Unable to connect" not in r.stderr
    assert "ATTACH" not in r.stderr


def test_pset_combines_with_other_flags() -> None:
    # multiple -P + -A interact cleanly.
    k = _kw()
    r = _run(["-h", k["host"], "-p", k["port"], "-d", k["dbname"],
              "-U", k["user"], "-P", "format=unaligned",
              "--pset=null=NULL",
              "-c", "SELECT NULL::TEXT, 'x';"])
    assert r.returncode == 0, r.stderr
    assert "NULL|x" in r.stdout


# ---- .docs (embedded documentation) ---------------------------------------

def _docs_shell(arg: str) -> subprocess.CompletedProcess:
    r = _run_shell(["-c", f".docs {arg}".rstrip()])
    if "carries no documentation index" in (r.stdout + r.stderr):
        pytest.skip("build has no embedded documentation index")
    return r


def _has_docs_index() -> bool:
    r = _run_shell(["-c", ".docs"])
    return "carries no documentation index" not in (r.stdout + r.stderr)


def test_docs_without_an_index_says_so() -> None:
    r = _run_shell(["-c", ".docs"])
    if _has_docs_index():
        pytest.skip("build carries a documentation index")
    assert r.returncode != 0
    assert "SDB_EMBEDDED_DOCS=ON" in (r.stdout + r.stderr)


def _docs_search_paths(out: str) -> list[str]:
    prefix = ".docs "
    return [line.strip()[len(prefix):]
            for line in out.splitlines() if line.strip().startswith(prefix)]


def test_docs_appears_in_help() -> None:
    r = _run_shell(["-c", ".help"])
    assert r.returncode == 0, r.stderr
    assert ".docs" in r.stdout


def test_docs_index_lists_sections() -> None:
    r = _docs_shell("")
    assert r.returncode == 0, r.stderr
    assert "SereneDB documentation" in r.stdout
    assert "sql (" in r.stdout
    assert "like .docs how do I highlight matches, to search every page" in r.stdout


def test_docs_renders_a_page_with_its_sections() -> None:
    r = _docs_shell("sql/indexes/index.md")
    assert r.returncode == 0, r.stderr
    assert "path: sql/indexes/index.md#Indexes" in r.stdout
    assert "Sections" in r.stdout
    assert ".docs sql/indexes/index.md#Indexes#Index_Types" in r.stdout


def test_docs_looks_up_an_object_by_name() -> None:
    r = _docs_shell("BM25")
    assert r.returncode == 0, r.stderr
    assert "BM25(tableoid" in r.stdout


def _docs_session(*commands: str) -> subprocess.CompletedProcess:
    r = subprocess.run(
        [SERENED_BIN, "shell"],
        input="".join(f"{command}\n" for command in commands),
        capture_output=True, text=True, timeout=20.0,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )
    if "carries no documentation index" in (r.stdout + r.stderr):
        pytest.skip("build has no embedded documentation index")
    return r


def test_docs_several_matches_print_a_numbered_menu() -> None:
    r = _docs_shell("date_trunc")
    assert r.returncode == 0, r.stderr
    assert "'date_trunc' matches" in r.stdout
    assert "\n1. date_trunc(" in r.stdout
    assert "\n2. date_trunc(" in r.stdout
    assert "path: " not in r.stdout


def test_docs_number_opens_an_item_of_the_last_list() -> None:
    r = _docs_session(".docs date_trunc", ".docs 2")
    assert r.returncode == 0, r.stderr
    assert "path: sql/functions/" in r.stdout


def test_docs_number_walks_into_a_page_section() -> None:
    r = _docs_session(".docs sql/indexes/index.md", ".docs 1")
    assert r.returncode == 0, r.stderr
    assert r.stdout.count("path: sql/indexes/index.md#Indexes") >= 2


def test_docs_all_renders_every_match() -> None:
    r = _docs_shell("--all date_trunc")
    assert r.returncode == 0, r.stderr
    assert r.stdout.count("path: sql/functions/") >= 2


def test_docs_all_numbers_its_listings_as_one_list() -> None:
    r = _docs_session(".docs --all search", ".docs 36")
    assert r.returncode == 0, r.stderr
    under = r.stdout.index("Documentation under 'sql/functions/search':")
    assert "\n36. " in r.stdout[under:]
    assert "path: sql/functions/search/" in r.stdout[under:].split("\n36. ")[1]


def test_docs_a_directory_name_lists_its_pages() -> None:
    r = _docs_shell("functions")
    assert r.returncode == 0, r.stderr
    assert "Documentation under 'sql/functions'" in r.stdout
    assert ".docs sql/functions/date.md" in r.stdout


def test_docs_a_page_name_offers_every_page_of_that_name() -> None:
    r = _docs_shell("geometry")
    assert r.returncode == 0, r.stderr
    assert ".docs sql/functions/geometry.md" in r.stdout
    assert ".docs sql/data_types/geometry.md" in r.stdout


def test_docs_a_page_documenting_an_object_is_listed_once() -> None:
    r = _docs_shell("union")
    assert r.returncode == 0, r.stderr
    page = ".docs sql/statements/create_text_search_dictionary/union.md#union"
    assert r.stdout.count(page) == 1


_SHELL_PAGE = "clients/serened-shell.md"


def _docs_link(prefix: str) -> str:
    r = _docs_shell(_SHELL_PAGE)
    assert r.returncode == 0, r.stderr
    lines = r.stdout.splitlines()
    assert "Links" in lines
    links = lines[lines.index("Links") + 1:]
    return next(item.split(".")[0].strip()
                for item, target in zip(links, links[1:])
                if target.strip().startswith(prefix))


def test_docs_numbers_links_and_follows_them() -> None:
    number = _docs_link(".docs clients/serened-psql.md")
    r = _docs_session(f".docs {_SHELL_PAGE}", f".docs {number}")
    assert r.returncode == 0, r.stderr
    assert "path: clients/serened-psql.md" in r.stdout


def test_docs_follows_a_link_to_the_heading_it_names() -> None:
    number = _docs_link(".docs sql/functions/docs.md#")
    r = _docs_session(f".docs {_SHELL_PAGE}", f".docs {number}")
    assert r.returncode == 0, r.stderr
    assert any(line.startswith("path: sql/functions/docs.md#")
               and line.endswith("#The_object_catalog")
               for line in r.stdout.splitlines()), r.stdout


def test_docs_an_external_link_prints_its_url() -> None:
    number = _docs_link("https://serenedb.com/docs/")
    r = _docs_session(f".docs {_SHELL_PAGE}", f".docs {number}")
    assert r.returncode == 0, r.stderr
    assert "https://serenedb.com/docs/" in r.stdout.splitlines()


def test_docs_a_section_lists_its_subsections() -> None:
    r = _docs_shell("sql")
    assert r.returncode == 0, r.stderr
    assert ".docs sql/functions" in [line.strip()
                                     for line in r.stdout.splitlines()]
    assert ".docs sql/functions/date.md" not in r.stdout


def test_docs_a_one_page_subsection_is_listed_as_its_page() -> None:
    r = _docs_shell("query_syntax")
    assert r.returncode == 0, r.stderr
    assert re.search(r"\d+\. Set Operations", r.stdout), r.stdout
    assert not re.search(r"\d+\. setops/", r.stdout), r.stdout


def test_docs_a_one_page_section_opens_its_page() -> None:
    r = _docs_shell("setops")
    assert r.returncode == 0, r.stderr
    assert "path: sql/query_syntax/setops/index.md" in r.stdout


def test_docs_headings_wrap_to_the_width() -> None:
    if not _has_docs_index():
        pytest.skip("build has no embedded documentation index")
    r = _run_shell(["-c", ".maxwidth 50", "-c", ".docs"])
    assert r.returncode == 0, r.stderr
    heading = r.stdout.splitlines()
    assert "SereneDB documentation" in heading
    assert max(len(line) for line in heading) <= 50, heading


def test_docs_a_dot_command_renders_its_card() -> None:
    r = _docs_shell(".timer")
    assert r.returncode == 0, r.stderr
    assert ".timer on|off" in r.stdout
    assert "Kind: command" in r.stdout


_TERMINAL_ESCAPES = re.compile(
    r"\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\r")


class _Terminal:
    def __init__(self, home: Path, columns: int) -> None:
        self.raw = ""
        self._pid, self._fd = pty.fork()
        if self._pid == 0:
            env = {**os.environ, "TERM": "xterm-256color", "HOME": str(home),
                   "DUCKDB_PAGER": "echo PAGER_USED; cat"}
            env.pop("NO_COLOR", None)
            os.execve(SERENED_BIN,
                      [SERENED_BIN, "shell", "-dark-mode", "--no-init"], env)
        fcntl.ioctl(self._fd, termios.TIOCSWINSZ,
                    struct.pack("HHHH", 40, columns, 0, 0))
        self.read_until(".help")

    def read_until(self, text: str, timeout: float = 30.0) -> str:
        raw = b""
        deadline = time.monotonic() + timeout
        while True:
            self.raw = raw.decode("utf-8", "replace")
            seen = _TERMINAL_ESCAPES.sub("", self.raw)
            if text in seen:
                return seen
            remaining = deadline - time.monotonic()
            assert remaining > 0, f"{text!r} never appeared in {seen!r}"
            ready, _, _ = select.select([self._fd], [], [], remaining)
            if ready:
                chunk = os.read(self._fd, 65536)
                assert chunk, f"the shell exited before {text!r}: {seen!r}"
                raw += chunk

    def send(self, keys: str, until: str) -> str:
        os.write(self._fd, keys.encode())
        return self.read_until(until)

    def close(self) -> None:
        try:
            os.write(self._fd, b"\x03.quit\r")
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if os.waitpid(self._pid, os.WNOHANG) != (0, 0):
                    return
                time.sleep(0.1)
            os.kill(self._pid, 9)
            os.waitpid(self._pid, 0)
        finally:
            os.close(self._fd)


def _open_terminal(home: Path, columns: int):
    if not _has_docs_index():
        pytest.skip("build has no embedded documentation index")
    terminal = _Terminal(home, columns)
    try:
        yield terminal
    finally:
        terminal.close()


@pytest.fixture
def docs_terminal(tmp_path: Path):
    yield from _open_terminal(tmp_path, 160)


@pytest.fixture
def narrow_docs_terminal(tmp_path: Path):
    yield from _open_terminal(tmp_path, 80)


def test_docs_opens_a_list_in_the_completion_menu(docs_terminal) -> None:
    out = docs_terminal.send(".docs date_trunc\r",
                             until="3. date_trunc(part, timestamptz)")
    assert "'date_trunc' matches 3 entries:" in out
    assert "1. date_trunc(part, date) (function)" in out
    assert "Open one with .docs <number>." not in out
    assert ".docs 1" in out
    docs_terminal.send("\x1b[B", until=".docs 2")
    docs_terminal.send("\r", until="path: sql/functions/timestamp.md")


def test_docs_esc_closes_the_menu(docs_terminal) -> None:
    docs_terminal.send(".docs date_trunc\r",
                       until="3. date_trunc(part, timestamptz)")
    docs_terminal.send("\x1b", until=".docs")
    out = docs_terminal.send("\r", until="SereneDB documentation")
    assert "path: sql/functions/date.md" not in out


def _select_first_table(terminal: _Terminal) -> None:
    terminal.send("SELECT * FROM narr", until="FROM narr")
    time.sleep(0.3)
    terminal.send("\t", until="narrow_beta")
    time.sleep(0.3)
    terminal.send("\t", until="FROM narrow_alpha")


def _two_tables(terminal: _Terminal) -> None:
    terminal.send("ATTACH ':memory:' AS nd;\r", until="AS nd;")
    terminal.send("USE nd;\r", until="USE nd;")
    terminal.send("CREATE TABLE narrow_alpha (alpha_col INTEGER);\r",
                  until="narrow_alpha (alpha_col INTEGER);")
    terminal.send("CREATE TABLE narrow_beta (beta_col INTEGER);\r",
                  until="narrow_beta (beta_col INTEGER);")


def test_sql_completion_accepts_the_selection_on_typing(docs_terminal) -> None:
    _two_tables(docs_terminal)
    _select_first_table(docs_terminal)
    out = docs_terminal.send("x", until="narrow_alphax")
    assert "narrx" not in out


def test_sql_completion_accepts_the_selection_on_arrows(docs_terminal) -> None:
    _two_tables(docs_terminal)
    _select_first_table(docs_terminal)
    os.write(docs_terminal._fd, b"\x1b[B")
    time.sleep(0.3)
    out = docs_terminal.send(";\r", until="alpha_col")
    assert "beta_col" not in out


def test_esc_prefix_still_reads_as_alt(docs_terminal) -> None:
    docs_terminal.send("SELECT 1 AS abc", until="AS abc")
    os.write(docs_terminal._fd, b"\x1b")
    time.sleep(0.3)
    out = docs_terminal.send("bx", until="ASx abc")
    assert "abcb" not in out


def test_docs_menu_does_not_swallow_typed_ahead_input(docs_terminal) -> None:
    out = docs_terminal.send(
        ".docs date_trunc\rSELECT 'typed' || '_ahead' AS v;\r",
        until="typed_ahead")
    assert "'date_trunc' matches" in out


def test_docs_writes_no_escapes_into_an_output_file(docs_terminal,
                                                    tmp_path: Path) -> None:
    target = tmp_path / "docs.txt"
    docs_terminal.send(f".output {target}\r", until=f".output {target}")
    docs_terminal.send(".docs sql/indexes/index.md\r",
                       until=".docs sql/indexes/index.md")
    docs_terminal.send(".output\rSELECT 'writ' || 'ten' AS done;\r",
                       until="written")
    text = target.read_text()
    assert "path: sql/indexes/index.md" in text
    assert "\x1b" not in text


def test_docs_a_long_menu_scrolls_to_the_selection(docs_terminal) -> None:
    docs_terminal.send(".docs --kind setting\r", until=".docs 1")
    out = docs_terminal.send("\x1b[C" * 60, until=".docs 61")
    assert re.search(r"(^|\s)61\. ", out), out


def test_docs_typing_a_number_narrows_the_menu(docs_terminal) -> None:
    docs_terminal.send(".docs date_trunc\r",
                       until="3. date_trunc(part, timestamptz)")
    out = docs_terminal.send("2", until="2. date_trunc(part, timestamp)")
    assert "1. date_trunc(part, date)" not in out
    assert "3. date_trunc(part, timestamptz)" not in out
    docs_terminal.send("\r", until="path: sql/functions/timestamp.md")


def test_docs_backspace_widens_a_narrowed_menu(docs_terminal) -> None:
    docs_terminal.send(".docs date_trunc\r",
                       until="3. date_trunc(part, timestamptz)")
    docs_terminal.send("2", until="2. date_trunc(part, timestamp)")
    out = docs_terminal.send("\x7f", until="3. date_trunc(part, timestamptz)")
    assert "1. date_trunc(part, date)" in out


def test_docs_arrows_move_through_the_menu_grid(narrow_docs_terminal) -> None:
    out = narrow_docs_terminal.send(".docs\r", until="11. sql (")
    assert "1. benchmarks (" in out
    for key, buffer in [("\x1b[B", ".docs 3"), ("\x1b[C", ".docs 4"),
                        ("\x1b[A", ".docs 2"), ("\x1b[D", ".docs 1")]:
        narrow_docs_terminal.send(key, until=buffer)
    narrow_docs_terminal.send("\r", until="Documentation under 'benchmarks':")


def test_docs_contents_fit_a_narrow_terminal(narrow_docs_terminal) -> None:
    out = narrow_docs_terminal.send(".docs\r", until="11. sql (")
    assert "PAGER_USED" not in out
    lines = out.splitlines()
    start = lines.index("SereneDB documentation")
    end = next(i for i, line in enumerate(lines)
               if line.startswith("1. benchmarks ("))
    assert max(len(line) for line in lines[start:end]) <= 80, lines[start:end]


def test_docs_hyperlinks_do_not_widen_a_page(narrow_docs_terminal) -> None:
    out = narrow_docs_terminal.send(
        ".docs clients/serened-psql.md#serened_psql#Dot_commands\r",
        until=".docs clients/serened-shell.md#serened_shell#Dot_commands")
    assert ("\x1b]8;;https://serenedb.com/docs/clients/serened-shell"
            "#dot-commands\x1b\\") in narrow_docs_terminal.raw
    assert "PAGER_USED" not in out


def test_docs_tab_after_docs_lists_the_sections(docs_terminal) -> None:
    out = docs_terminal.send(".docs \t", until="compatibility")
    assert "cookbook" in out
    assert "sql" in out


def test_docs_tab_after_kind_lists_every_kind(docs_terminal) -> None:
    docs_terminal.send(".docs --kind ", until=".docs --kind")
    out = docs_terminal.send("\t", until="index_type")
    for kind in ["command", "function", "setting", "statement", "tokenizer",
                 "type"]:
        assert kind in out


def test_docs_tab_completes_names_flags_and_paths(docs_terminal) -> None:
    for typed, completed in [(".docs date_tr", ".docs date_trunc"),
                             (".docs --kind setting thre",
                              ".docs --kind setting threads"),
                             (".docs --kind ty", ".docs --kind type "),
                             (".docs sql/functions/dat",
                              "sql/functions/datepart.md")]:
        docs_terminal.send(f"\x15{typed}", until=typed)
        docs_terminal.send("\t", until=completed)


def test_docs_number_without_a_list_fails() -> None:
    r = _docs_shell("2")
    assert r.returncode != 0
    assert "Nothing to open yet" in (r.stdout + r.stderr)


def test_docs_browses_a_section() -> None:
    r = _docs_shell("cookbook")
    assert r.returncode == 0, r.stderr
    assert "Documentation under 'cookbook'" in r.stdout
    assert ".docs cookbook/" in r.stdout


def test_docs_list_flag_emits_paths() -> None:
    r = _docs_shell("--list sql/functions/search/")
    assert r.returncode == 0, r.stderr
    lines = [line for line in r.stdout.splitlines() if line.strip()]
    assert lines
    for line in lines:
        assert line.startswith("sql/functions/search/")


def test_docs_search_flag_returns_pasteable_paths() -> None:
    r = _docs_shell("--search phrase search")
    assert r.returncode == 0, r.stderr
    assert "Documentation matching 'phrase search'" in r.stdout
    first = next(line for line in r.stdout.splitlines() if line.startswith("1. "))
    assert "phrase" in first.lower(), first
    assert re.search(r"\n +\.docs sql/", r.stdout), r.stdout


def test_docs_unknown_name_offers_candidates_and_fails() -> None:
    r = _docs_shell("to_tsvector")
    assert r.returncode != 0
    out = r.stdout + r.stderr
    assert "Closest matches" in out
    assert ".docs " in out


@pytest.mark.parametrize("typo", ["tsvecto", "vacum", "hnws"])
def test_docs_typo_still_finds_candidates(typo: str) -> None:
    r = _docs_shell(typo)
    assert r.returncode != 0
    out = r.stdout + r.stderr
    assert "Closest matches" in out
    assert ".docs " in out


def test_docs_search_is_identical_connected_and_offline() -> None:
    offline = _docs_shell("--search bm25 scoring")
    connected = _run(_conn_args() + ["-c", ".docs --search bm25 scoring"])
    assert connected.stdout == offline.stdout
    assert connected.returncode == offline.returncode


@pytest.mark.parametrize("query", [
    "inverted index",
    "vacuum",
    "bm25 scoring",
    "text search dictionary",
    "index",
])
def test_docs_search_matches_the_server_ranking(query: str) -> None:
    local = _docs_shell(f"--search {query}")
    assert local.returncode == 0, local.stderr
    remote = f"SELECT path FROM sdb_docs.search(''{query}'', 10)"
    served = _run(_conn_args() + [
        "-t", "-A", "-c",
        f"SELECT path FROM postgres_query('{_kw()['dbname']}', '{remote}')"])
    assert served.returncode == 0, served.stderr
    expected = [line.strip() for line in served.stdout.splitlines()
                if line.strip()]
    assert _docs_search_paths(local.stdout) == expected


def test_docs_search_reads_unparsable_text_as_words() -> None:
    r = _docs_shell("--search @@ operator")
    assert r.returncode == 0, r.stderr
    assert "Documentation matching '@@ operator':" in r.stdout


def test_docs_answers_a_question_with_its_search_results() -> None:
    r = _docs_shell("how do I create an inverted index?")
    assert r.returncode == 0, r.stderr
    assert "Documentation matching 'how do I create an inverted index?':" in r.stdout
    assert "sql/indexes/inverted/" in r.stdout


def test_docs_finds_the_page_of_a_pasted_error() -> None:
    r = _docs_shell("Only one scorer function is allowed per inverted index")
    assert r.returncode == 0, r.stderr
    first = next(line for line in r.stdout.splitlines() if line.startswith("1. "))
    assert "Relevance Scoring" in first


def test_docs_reads_a_call_as_its_name() -> None:
    call = _docs_shell("date_trunc(ts, 'day')")
    name = _docs_shell("date_trunc")
    assert call.returncode == 0, call.stderr
    assert call.stdout.replace("date_trunc(ts, day )", "date_trunc") == name.stdout


def test_docs_question_mark_shows_the_index() -> None:
    assert _docs_shell("?").stdout == _docs_shell("").stdout


def test_docs_opens_a_pasted_site_url() -> None:
    r = _docs_shell("https://serenedb.com/docs/sql/indexes#index-types")
    assert r.returncode == 0, r.stderr
    assert r.stdout.startswith("path: sql/indexes/index.md#Indexes#Index_Types\n")
    site = _docs_shell("https://serenedb.com/docs/sql/functions/search/scoring")
    assert site.stdout == _docs_shell("sql/functions/search/scoring.md").stdout
    assert _docs_shell("sql/functions/search/scoring").stdout == site.stdout


def test_docs_searches_operators_instead_of_reading_them_as_options() -> None:
    r = _docs_shell("->>")
    assert r.returncode == 0, r.stderr
    assert "Unknown option" not in (r.stdout + r.stderr)
    assert "Documentation matching '->>':" in r.stdout


def test_docs_search_refuses_exclusion() -> None:
    r = _docs_shell("--search +fox -red")
    assert r.returncode != 0
    assert "exclusion" in (r.stdout + r.stderr)


def test_docs_rejects_unknown_option() -> None:
    r = _docs_shell("--nope")
    assert r.returncode != 0
    assert "Unknown option" in (r.stdout + r.stderr)


def test_docs_emits_no_escapes_without_a_tty() -> None:
    r = _docs_shell("sql/functions/search/scoring.md")
    assert r.returncode == 0, r.stderr
    assert "\x1b" not in r.stdout


def test_docs_wraps_table_cells_instead_of_truncating() -> None:
    r = _docs_shell("sql/functions/aggregates/index.md")
    assert r.returncode == 0, r.stderr
    assert "approx_count_distinct(x)" in r.stdout
    assert "\N{HORIZONTAL ELLIPSIS}" not in r.stdout


def test_docs_shell_and_psql_modes_agree_byte_for_byte() -> None:
    if not _has_docs_index():
        pytest.skip("build has no embedded documentation index")
    k = _kw()
    shell = _run_shell(["-c", ".docs sql/indexes/index.md"])
    psql = _run(["-h", k["host"], "-p", k["port"], "-U", k["user"],
                 "-d", k["dbname"], "-c", ".docs sql/indexes/index.md"])
    assert shell.returncode == 0, shell.stderr
    assert psql.returncode == 0, psql.stderr
    assert shell.stdout == psql.stdout
