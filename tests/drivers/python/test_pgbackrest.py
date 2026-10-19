"""pgBackRest takes a full backup of a running serened and restores it."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import psycopg
import pytest
from serened import Serened  # type: ignore[import-untyped]


SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)
PGBACKREST = os.environ.get("SDB_DRV_PGBACKREST") or shutil.which("pgbackrest")
STANZA = "sdb"


def _have_binary() -> bool:
    return Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)


pytestmark = pytest.mark.skipif(
    not _have_binary(),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)


def _connect(server: Serened) -> psycopg.Connection:
    return psycopg.connect(server.dsn(), autocommit=True)


def _pgbackrest(config: Path, *args: str) -> str:
    result = subprocess.run([PGBACKREST, f"--config={config}", f"--stanza={STANZA}", *args],
                            capture_output=True, text=True, timeout=300)
    output = result.stdout + result.stderr
    assert result.returncode == 0, f"pgbackrest {' '.join(args)} failed:\n{output}"
    return output


def _state(server: Serened) -> tuple:
    with _connect(server) as conn:
        conn.execute("VACUUM (REFRESH_INDEX) docs_idx")
        conn.execute("VACUUM (REFRESH_TABLE) notes")
        return (
            conn.execute("SELECT count(*), sum(id) FROM docs").fetchone(),
            conn.execute("SELECT count(*) FROM docs_idx WHERE body @@ 'second'").fetchone(),
            conn.execute("SELECT count(*), sum(id) FROM notes").fetchone(),
        )


def test_pgbackrest_backs_up_and_restores_a_running_server(tmp_path: Path) -> None:
    assert PGBACKREST, "pgbackrest not found: set SDB_DRV_PGBACKREST or put it on PATH"
    source = Serened(SERENED_BIN, datadir_root=str(tmp_path),
                     log_path=str(tmp_path / "source.log"))
    restored_dir = tmp_path / "restored"
    restored = None
    try:
        source.start()
        config = tmp_path / "pgbackrest.conf"
        config.write_text(
            "[global]\n"
            f"repo1-path={tmp_path / 'repo'}\n"
            f"log-path={tmp_path / 'log'}\n"
            f"lock-path={tmp_path / 'lock'}\n"
            f"spool-path={tmp_path / 'spool'}\n"
            "log-level-console=info\n"
            "log-level-file=off\n"
            f"[{STANZA}]\n"
            f"pg1-path={source.datadir}\n"
            f"pg1-port={source.port}\n"
            "pg1-socket-path=127.0.0.1\n"
            "pg1-user=postgres\n")
        with _connect(source) as conn:
            conn.execute("CREATE TABLE docs (id BIGINT, body TEXT)")
            conn.execute("CREATE INDEX docs_idx ON docs USING inverted (body)")
            conn.execute("CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT) "
                         "WITH (storage = 'search')")
            conn.execute("INSERT INTO docs SELECT i, 'first' FROM range(0, 1000) r(i)")
            conn.execute("INSERT INTO notes SELECT i, 'first' FROM range(0, 500) r(i)")
            conn.execute("CHECKPOINT")
            conn.execute("INSERT INTO docs SELECT i, 'second' FROM range(1000, 1100) r(i)")
            conn.execute("INSERT INTO notes SELECT i, 'second' FROM range(500, 550) r(i)")
        expected = _state(source)

        _pgbackrest(config, "stanza-create")
        _pgbackrest(config, "--type=full", "--no-archive-check", "backup")
        source.stop()

        _pgbackrest(config, f"--pg1-path={restored_dir}", "--type=none", "restore")
        restored = Serened(SERENED_BIN, datadir=str(restored_dir),
                           log_path=str(tmp_path / "restored.log"))
        restored.start()
        assert _state(restored) == expected
        with _connect(restored) as conn:
            conn.execute("INSERT INTO docs VALUES (5000, 'after')")
        restored.restart()
        with _connect(restored) as conn:
            assert conn.execute("SELECT count(*) FROM docs").fetchone() == (1101,)
    finally:
        source.stop()
        if restored is not None:
            restored.stop()
