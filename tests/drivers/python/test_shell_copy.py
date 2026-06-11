"""COPY ... FROM STDIN / TO STDOUT inside `serened shell`."""

from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path

import pytest


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


def _run_shell(script: str, *, db_path: str = ":memory:",
             extra_args: list[str] | None = None,
             timeout: float = 30.0) -> subprocess.CompletedProcess:
    argv = [SERENED_BIN, "shell", db_path]
    if extra_args:
        argv.extend(extra_args)
    return subprocess.run(
        argv,
        input=script,
        capture_output=True,
        text=True,
        timeout=timeout,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )


def _run_shell_with_data(sql: str, stdin_data: bytes, *,
                        db_path: str,
                        timeout: float = 30.0) -> subprocess.CompletedProcess:
    # SQL via -c, COPY data on stdin -- the only combination that works
    # for COPY FROM STDIN when the shell isn't attached to a terminal.
    return subprocess.run(
        [SERENED_BIN, "shell", db_path, "-c", sql],
        input=stdin_data,
        capture_output=True,
        timeout=timeout,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )


def test_shell_copy_from_stdin_csv(tmp_path: Path) -> None:
    """COPY FROM STDIN with CSV data piped on the shell's stdin.

    SQL via -c, raw rows on stdin -- the only combination the duckdb
    shell supports for interactive-style COPY from a pipe.
    """
    db = tmp_path / "shell.duckdb"
    # Seed the table first via a separate invocation (so the second
    # invocation's stdin is purely COPY data).
    seed = subprocess.run(
        [SERENED_BIN, "shell", str(db),
         "-c", "CREATE TABLE main.t(x INT, label VARCHAR);"],
        capture_output=True, text=True, timeout=15,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )
    assert seed.returncode == 0, f"seed failed: {seed.stderr}"

    r = _run_shell_with_data(
        "COPY main.t FROM STDIN (FORMAT CSV); "
        "SELECT count(*) AS n, sum(x) AS s FROM main.t;",
        b"1,one\n2,two\n3,three\n",
        db_path=str(db),
    )
    assert r.returncode == 0, f"stderr={r.stderr!r} stdout={r.stdout!r}"
    out = r.stdout.decode()
    assert " 3 " in out and " 6 " in out, f"unexpected stdout: {out!r}"


def test_shell_copy_from_stdin_bare_keyword(tmp_path: Path) -> None:
    db = tmp_path / "shell.duckdb"
    seed = subprocess.run(
        [SERENED_BIN, "shell", str(db),
         "-c", "CREATE TABLE main.t(x INT);"],
        capture_output=True, text=True, timeout=15,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )
    assert seed.returncode == 0, f"seed failed: {seed.stderr}"

    r = _run_shell_with_data(
        "COPY main.t FROM STDIN; SELECT count(*), sum(x) FROM main.t;",
        b"100\n200\n",
        db_path=str(db),
    )
    assert r.returncode == 0, f"stderr={r.stderr!r} stdout={r.stdout!r}"
    out = r.stdout.decode()
    assert " 2 " in out and " 300 " in out, f"unexpected stdout: {out!r}"


def test_shell_copy_to_stdout(tmp_path: Path) -> None:
    db = tmp_path / "shell.duckdb"
    script = (
        'CREATE TABLE main.t(x INT);\n'
        "INSERT INTO main.t VALUES (10),(20),(30);\n"
        "COPY (SELECT x FROM main.t ORDER BY x) TO STDOUT (FORMAT CSV);\n"
    )
    r = _run_shell(script, db_path=str(db))
    assert r.returncode == 0, f"stderr=\n{r.stderr}\nstdout=\n{r.stdout}"
    for v in ("10", "20", "30"):
        assert v in r.stdout, f"missing {v} in stdout:\n{r.stdout}"


def test_shell_copy_to_stdout_bare_keyword(tmp_path: Path) -> None:
    db = tmp_path / "shell.duckdb"
    script = (
        'CREATE TABLE main.t(x INT);\n'
        "INSERT INTO main.t VALUES (1),(2);\n"
        "COPY main.t TO STDOUT;\n"
    )
    r = _run_shell(script, db_path=str(db))
    assert r.returncode == 0, f"stderr=\n{r.stderr}\nstdout=\n{r.stdout}"
    assert "1" in r.stdout and "2" in r.stdout


def test_shell_postgres_scanner_round_trip() -> None:
    # serened shell attaches the running pg-wire endpoint via postgres_scanner
    # and exercises CREATE / INSERT (which the scanner emits as COPY ... FROM
    # STDIN) / SELECT end-to-end. Regression: pre-fix this failed with
    # "No files found that match the pattern 'stdin'".
    from spec_loader import conn_kwargs  # type: ignore[import-untyped]
    k = conn_kwargs()
    table = f"shell_pgscan_{uuid.uuid4().hex[:10]}"
    sql = (
        "SET pg_use_text_protocol = true; "
        "SET pg_use_binary_copy = false; "
        f"ATTACH 'host={k['host']} port={k['port']} dbname={k['dbname']} "
        f"user={k['user']}' AS sdb (TYPE postgres); "
        "USE sdb; "
        f"CREATE TABLE public.\"{table}\"(x INT, label VARCHAR); "
        f"INSERT INTO public.\"{table}\" "
        "VALUES (1,'one'),(2,'two'),(3,'three'); "
        f"SELECT count(*), sum(x), string_agg(label, ',' ORDER BY x) "
        f"FROM public.\"{table}\"; "
        f"DROP TABLE public.\"{table}\";"
    )
    r = subprocess.run(
        [SERENED_BIN, "shell", "-c", sql],
        capture_output=True, text=True, timeout=30,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )
    assert r.returncode == 0, f"stderr={r.stderr!r}"
    assert " 3 " in r.stdout, f"row count missing: {r.stdout!r}"
    assert " 6 " in r.stdout, f"sum missing: {r.stdout!r}"
    assert "one,two,three" in r.stdout, \
        f"string_agg missing: {r.stdout!r}"


def test_shell_copy_one_shot_command(tmp_path: Path) -> None:
    db = tmp_path / "shell.duckdb"
    seed = subprocess.run(
        [SERENED_BIN, "shell", str(db),
         "-c", "CREATE TABLE main.t(x INT); INSERT INTO main.t VALUES (5),(6),(7);"],
        capture_output=True, text=True, timeout=15,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )
    assert seed.returncode == 0, f"seed failed: {seed.stderr}"
    out = subprocess.run(
        [SERENED_BIN, "shell", str(db),
         "-c", "COPY (SELECT x FROM main.t ORDER BY x) TO STDOUT (FORMAT CSV);"],
        capture_output=True, text=True, timeout=15,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )
    assert out.returncode == 0, f"copy failed: {out.stderr}"
    lines = [l for l in out.stdout.splitlines() if l.strip()]
    assert "5" in lines and "6" in lines and "7" in lines, \
        f"unexpected stdout:\n{out.stdout}"
