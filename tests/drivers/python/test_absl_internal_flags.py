"""Smoke tests for absl-internal flag-source machinery on the serened binary.

Covers --flagfile, --fromenv, --tryfromenv, --undefok. Each case invokes a
fresh serened subprocess with --version (the only built-in absl handler that
exits 0 after parsing completes) so the flag parser runs end-to-end without
the server actually booting.
"""

from __future__ import annotations

import os
import subprocess
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


def _run(args: list[str], *, env_extra: dict[str, str] | None = None,
         timeout: float = 15.0) -> subprocess.CompletedProcess:
    env = {**os.environ, "NO_COLOR": "1", "TERM": "dumb"}
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [SERENED_BIN, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


# ---- --flagfile -----------------------------------------------------------


def test_flagfile_is_parsed(tmp_path: Path) -> None:
    # --version inside the flagfile triggers exit(0); proves the file was
    # read AND its tokens were fed through the flag parser.
    ff = tmp_path / "f.flags"
    ff.write_text("--version\n")
    r = _run([f"--flagfile={ff}"])
    assert r.returncode == 0, r.stderr
    assert "Debug build" in r.stdout or "Release build" in r.stdout, r.stdout


def test_flagfile_tokens_reach_parser(tmp_path: Path) -> None:
    # An unknown flag inside the flagfile must reach the parser; --undefok on
    # the CLI tolerates it. If the flagfile contents bypassed parsing, undefok
    # would never see "this_does_not_exist" and the run would still succeed --
    # which doesn't prove anything. So we ALSO assert it fails without undefok.
    ff = tmp_path / "f.flags"
    ff.write_text("--this_does_not_exist=ignored\n--version\n")

    r_with = _run(["--undefok=this_does_not_exist", f"--flagfile={ff}"])
    assert r_with.returncode == 0, r_with.stderr

    r_without = _run([f"--flagfile={ff}"])
    assert r_without.returncode != 0
    assert "Unknown" in (r_without.stdout + r_without.stderr) or \
           "this_does_not_exist" in (r_without.stdout + r_without.stderr), \
        r_without.stderr


def test_flagfile_missing_file_errors(tmp_path: Path) -> None:
    bogus = tmp_path / "does_not_exist.flags"
    r = _run([f"--flagfile={bogus}", "--version"])
    assert r.returncode != 0, "missing flagfile should be an error"


# ---- --undefok ------------------------------------------------------------


def test_undefok_tolerates_unknown_flag() -> None:
    r = _run(["--undefok=this_does_not_exist",
              "--this_does_not_exist=foo", "--version"])
    assert r.returncode == 0, r.stderr


def test_unknown_flag_fails_without_undefok() -> None:
    r = _run(["--this_definitely_does_not_exist=foo", "--version"])
    assert r.returncode != 0


def test_undefok_listing_multiple_flags() -> None:
    # Comma-separated list of multiple unknowns.
    r = _run(["--undefok=foo,bar,baz",
              "--foo=1", "--bar=x", "--baz=y", "--version"])
    assert r.returncode == 0, r.stderr


# ---- --fromenv / --tryfromenv --------------------------------------------


def test_fromenv_reads_existing_env_var() -> None:
    r = _run(["--fromenv=server_io_threads", "--version"],
             env_extra={"FLAGS_server_io_threads": "13"})
    assert r.returncode == 0, r.stderr


def test_fromenv_missing_env_var_fails() -> None:
    # Explicit fromenv without the corresponding FLAGS_* env must abort.
    env_no = {k: v for k, v in os.environ.items()
              if k != "FLAGS_server_io_threads"}
    r = subprocess.run(
        [SERENED_BIN, "--fromenv=server_io_threads", "--version"],
        capture_output=True, text=True, timeout=15.0, env=env_no,
    )
    assert r.returncode != 0


def test_tryfromenv_silently_skips_missing_env_var() -> None:
    # Same as above but with --tryfromenv: missing env is silent, exit 0.
    env_no = {k: v for k, v in os.environ.items()
              if k != "FLAGS_server_io_threads"}
    r = subprocess.run(
        [SERENED_BIN, "--tryfromenv=server_io_threads", "--version"],
        capture_output=True, text=True, timeout=15.0, env=env_no,
    )
    assert r.returncode == 0, r.stderr


def test_tryfromenv_reads_existing_env_var() -> None:
    r = _run(["--tryfromenv=server_io_threads", "--version"],
             env_extra={"FLAGS_server_io_threads": "7"})
    assert r.returncode == 0, r.stderr


# ---- help variants (advertised in the usage banner) -----------------------


def test_helpfull_lists_absl_internal_flag_definitions() -> None:
    # --helpfull shows the full ABSL-managed flag listing where each absl
    # built-in is rendered as "--flagfile (description text); default: ...".
    # That parenthesised description is the discriminator from a plain
    # textual mention in the usage banner.
    r = _run(["--helpfull"])
    out = r.stdout + r.stderr
    for spec in ("--flagfile (",
                 "--fromenv (",
                 "--tryfromenv (",
                 "--undefok ("):
        assert spec in out, f"--helpfull missing flag definition `{spec}`"


def test_help_grep_substring() -> None:
    # --help=<substring> filters by name OR description.
    r = _run(["--help=server_io_threads"])
    assert "server_io_threads" in (r.stdout + r.stderr)


def test_help_shows_banner_with_flag_source_section() -> None:
    # The usage banner advertises the absl-internal flag sources. Confirms
    # SetProgramUsageMessage content reaches the user via --help.
    r = _run(["--help"])
    out = r.stdout + r.stderr
    assert "Flag sources (absl built-in" in out, \
        "usage banner missing the Flag-sources section"
    for ref in ("--flagfile=path1,path2", "--fromenv=name1,name2",
                "--tryfromenv=name1,name2", "--undefok=name1,name2"):
        assert ref in out, f"banner missing reference to {ref}"
