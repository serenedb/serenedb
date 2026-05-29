"""`serened psql` subcommand against the harness's pg-wire endpoint."""

from __future__ import annotations

import os
import subprocess
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
    # so we can read the user value back directly.
    k = _kw()
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
