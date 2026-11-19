"""serenedb runs the command its first argument names; serened is always the server."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

BIN = Path(__file__).resolve().parents[3] / "build" / "bin"
SERENEDB_BIN = os.environ.get("SDB_DRV_SERENEDB_BIN", str(BIN / "serenedb"))
SERENED_BIN = os.environ.get("SDB_DRV_SERENED_BIN", str(BIN / "serened"))
COMMANDS = "commands: docs-index, psql, server, shell"
SERVER_HELP = "serened -- SereneDB pg-wire server"

pytestmark = pytest.mark.skipif(
    not all(Path(b).is_file() and os.access(b, os.X_OK)
            for b in (SERENEDB_BIN, SERENED_BIN)),
    reason=f"serenedb or serened not found or not executable: {SERENEDB_BIN}, "
           f"{SERENED_BIN}")


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(list(args), capture_output=True, text=True, timeout=60,
                          env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"})


def test_serenedb_without_a_command_lists_the_commands() -> None:
    r = _run(SERENEDB_BIN)
    assert r.returncode == 1, r
    assert "usage: serenedb <command> [args ...]" in r.stderr
    assert COMMANDS in r.stderr


@pytest.mark.parametrize("flag", ["--help", "-h"])
def test_serenedb_help_lists_the_commands(flag: str) -> None:
    r = _run(SERENEDB_BIN, flag)
    assert r.returncode == 0, r
    assert COMMANDS in r.stdout
    assert "serened is serenedb server." in r.stdout


@pytest.mark.parametrize("args,program", [
    ((SERENEDB_BIN, "--version"), "serenedb"),
    ((SERENEDB_BIN, "server", "--version"), "serenedb"),
    ((SERENED_BIN, "--version"), "serened"),
])
def test_version_names_the_program_the_release_and_the_commit(
        args: tuple[str, ...], program: str) -> None:
    r = _run(*args)
    assert r.returncode == 0, r
    assert re.fullmatch(
        rf"{program} \(SereneDB\) \d+\.\d+\.\S+ \([0-9a-f]{{40}}(-dirty)?\)\n",
        r.stdout), r.stdout


def test_serenedb_rejects_an_unknown_command() -> None:
    r = _run(SERENEDB_BIN, "serve")
    assert r.returncode == 1, r
    assert "serenedb: unknown command 'serve'" in r.stderr
    assert COMMANDS in r.stderr


def test_serenedb_server_is_the_server() -> None:
    r = _run(SERENEDB_BIN, "server", "--help")
    assert SERVER_HELP in r.stdout + r.stderr


def test_serenedb_server_takes_the_arguments_after_its_name(
        tmp_path: Path) -> None:
    r = _run(SERENEDB_BIN, "server", str(tmp_path / "data"), "--listen=bogus")
    assert r.returncode == 1, r
    out = r.stdout + r.stderr
    assert "invalid network endpoint 'bogus'" in out
    assert "positional" not in out


def test_serened_is_the_server_whatever_its_first_argument() -> None:
    r = _run(SERENED_BIN, "psql", "--help")
    out = r.stdout + r.stderr
    assert SERVER_HELP in out
    assert "PostgreSQL-compatible client" not in out
