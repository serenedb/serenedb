"""`serenedb shell` runs for the psql output, backslash command and COPY stream tests."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


SERENEDB_BIN = os.environ.get(
    "SDB_DRV_SERENEDB_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serenedb"),
)

_AGENT_ENVIRONMENT = ("AI_AGENT", "AGENT", "CLAUDECODE", "CODEX_CI", "CODEX_SANDBOX",
                      "CODEX_THREAD_ID", "CURSOR_AGENT", "GEMINI_CLI", "COPILOT_AGENT",
                      "COPILOT_CLI", "COPILOT_AGENT_SESSION_ID", "DUCKDB_AGENT_MODE")


def _have_binary() -> bool:
    return Path(SERENEDB_BIN).is_file() and os.access(SERENEDB_BIN, os.X_OK)


requires_serenedb = pytest.mark.skipif(
    not _have_binary(),
    reason=f"serenedb binary not found or not executable: {SERENEDB_BIN}",
)


@pytest.fixture()
def shell() -> list[str]:
    return [SERENEDB_BIN, "shell"]


class TestResult:
    __test__ = False

    def __init__(self, stdout: str, stderr: str, status_code: int) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.status_code = status_code

    def check_stdout(self, expected: str | list[str] | None) -> None:
        if expected is None:
            assert self.stdout == ""
            return
        if isinstance(expected, list):
            expected = "\n".join(expected)
        assert self.status_code == 0, self.stderr
        assert expected and expected in self.stdout, self.stdout

    def check_not_exist(self, not_exist: str | list[str]) -> None:
        if isinstance(not_exist, list):
            not_exist = "\n".join(not_exist)
        assert self.status_code == 0, self.stderr
        assert not_exist not in self.stdout, self.stdout

    def check_stderr(self, expected: str | None) -> None:
        if expected is None:
            assert self.stderr == ""
        else:
            assert expected and expected in self.stderr, self.stderr


class ShellTest:
    __test__ = False

    def __init__(self, shell: list[str]) -> None:
        self.arguments = [*shell, "--batch"]

    def add_argument(self, *args: str) -> ShellTest:
        self.arguments.extend(args)
        return self

    def run(self) -> TestResult:
        return self.run_raw(b"")

    def run_raw(self, stdin: str | bytes) -> TestResult:
        env = {k: v for k, v in os.environ.items() if k not in _AGENT_ENVIRONMENT}
        env.update({"NO_COLOR": "1", "TERM": "dumb"})
        res = subprocess.run(
            self.arguments,
            input=stdin.encode("utf8") if isinstance(stdin, str) else stdin,
            capture_output=True,
            env=env,
            timeout=60,
        )
        return TestResult(res.stdout.decode("utf8").strip(),
                          res.stderr.decode("utf8").strip(), res.returncode)
