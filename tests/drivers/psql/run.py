#!/usr/bin/env python3
"""psql backslash-command harness -- golden-file mode.

Reads tests.test (sqllogic-style: optional `skipif` directives, the command,
a `----` separator, then the golden block). Runs each block's command against
a configured psql endpoint and either diffs the actual output against the
committed golden, or rewrites the golden in place when --update is set.

Inputs (from environment, all driver-test standard):
  SDB_DRV_HOST/PORT/DATABASE/USER  -- psql endpoint
  SDB_DRV_RUN_ID                   -- per-run id, used to scope seeded schemas
  SDB_DRV_JUNIT                    -- output dir for JUnit
  SDB_DRV_ENGINE                   -- label used for skipif matching
                                      (default: serenedb)

Use --update once against a postgres oracle to populate goldens:
  SDB_DRV_ENGINE=postgres SDB_DRV_HOST=<pg-host> SDB_DRV_PORT=<pg-port> \\
      tests/drivers/psql/run.py --update
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import html
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

PSQL_ARGS = [
    "--no-psqlrc", "-X", "-q",
    "-P", "pager=off", "-P", "format=aligned", "-P", "border=2",
    "-P", "linestyle=ascii", "-P", "null=[NULL]",
    "-v", "ON_ERROR_STOP=0",
]


@dataclasses.dataclass
class Block:
    """A single test block: optional comment lines, skipif set, command, golden."""
    leading_comments: list[str]
    skipifs: set[str]
    skip_lines: list[int]  # 1-based golden line numbers excluded from compare
    cmd: str
    golden: str  # joined with '\n', no trailing newline
    line: int    # 1-based start line of the block (for error messages)


def parse_file(path: Path) -> tuple[list[Block], list[str]]:
    """Parse tests.test into (blocks, trailing_comments).

    Format:
      # comment lines (zero or more) -- attached to the next block
      [skipif <engine>]              (zero or more)
      <command>                      (single line; use `<NL>` for embedded newline)
      ----
      > <golden line>                (each golden line prefixed with `> `)
      > <blank golden line>          (empty line in the golden is `>` alone)
      ...
      <next non-`> ` line ends the block>
    """
    raw = path.read_text().splitlines()
    blocks: list[Block] = []
    i = 0
    pending_comments: list[str] = []
    while i < len(raw):
        line = raw[i]
        if line.strip() == "":
            i += 1
            continue
        if line.lstrip().startswith("#"):
            pending_comments.append(line)
            i += 1
            continue

        start_line = i + 1
        skipifs: set[str] = set()
        skip_lines: list[int] = []
        while i < len(raw):
            m_skipif = re.match(r"^skipif\s+(\S+)\s*$", raw[i])
            m_skipline = re.match(r"^skip-line:\s*(\d+)\s*$", raw[i])
            if m_skipif:
                skipifs.add(m_skipif.group(1))
                i += 1
            elif m_skipline:
                skip_lines.append(int(m_skipline.group(1)))
                i += 1
            else:
                break
        if i >= len(raw):
            raise SyntaxError(f"{path}:{start_line}: directive without command")
        cmd = raw[i]
        i += 1
        if i >= len(raw) or raw[i].rstrip() != "----":
            raise SyntaxError(
                f"{path}:{i + 1}: expected '----' after command, got "
                f"{raw[i] if i < len(raw) else 'EOF'!r}"
            )
        i += 1
        golden_lines: list[str] = []
        while i < len(raw):
            ln = raw[i]
            if ln == ">":
                golden_lines.append("")
                i += 1
            elif ln.startswith("> "):
                golden_lines.append(ln[2:])
                i += 1
            else:
                break
        blocks.append(Block(
            leading_comments=pending_comments,
            skipifs=skipifs,
            skip_lines=skip_lines,
            cmd=cmd,
            golden="\n".join(golden_lines),
            line=start_line,
        ))
        pending_comments = []
    return blocks, pending_comments


def _quote_golden(golden: str) -> list[str]:
    return [(">" if ln == "" else f"> {ln}") for ln in golden.split("\n")] if golden else []


def emit_file(path: Path, blocks: list[Block], trailing_comments: list[str]) -> None:
    out: list[str] = []
    for blk in blocks:
        out.extend(blk.leading_comments)
        for engine in sorted(blk.skipifs):
            out.append(f"skipif {engine}")
        for n in blk.skip_lines:
            out.append(f"skip-line: {n}")
        out.append(blk.cmd)
        out.append("----")
        out.extend(_quote_golden(blk.golden))
        out.append("")
    out.extend(trailing_comments)
    if not out or out[-1] != "":
        out.append("")
    path.write_text("\n".join(out))


def normalize(text: str, schema: str, schema2: str) -> str:
    text = text.replace(schema, "SCHEMA").replace(schema2, "SCHEMA2")
    text = re.sub(r"\d+ (bytes|kB|MB|GB|TB)", "SIZE", text)
    text = re.sub(r"\b\d{5,}\b", "OID", text)
    text = re.sub(r"\d{4}-\d{2}-\d{2}( [\d:.+-]+)?", "TIMESTAMP", text)
    text = re.sub(r"[ \t]+$", "", text, flags=re.MULTILINE)
    return text.rstrip("\n")




def _psql_command() -> list[str]:
    """The psql invocation as an argv list.

    Defaults to ["psql"] (uses whatever's on PATH). Override with
    SDB_DRV_PSQL -- e.g. `docker run --rm --network=host postgres:18.3 psql`
    to pin a specific server-matched client version.
    """
    raw = os.environ.get("SDB_DRV_PSQL", "psql")
    return shlex.split(raw)


def run_psql(host: str, port: str, db: str, user: str,
             schema: str, schema2: str, suffix: str, cmd: str) -> str:
    # {suffix} is the per-run hex shared by both seeded schemas
    # (psqlh_<suffix>, psqlh2_<suffix>); tests use it as a psql pattern
    # to scope `\d*` listings to our schemas in shared CI state.
    cmd_text = (cmd.replace("<NL>", "\n")
                   .replace("{schema}", schema)
                   .replace("{schema2}", schema2)
                   .replace("{suffix}", suffix))
    args = [
        "env", "LC_ALL=C", "COLUMNS=120",
        *_psql_command(),
        f"host={host} port={port} dbname={db} user={user} connect_timeout=10",
        *PSQL_ARGS,
        "-v", f"schema={schema}", "-v", f"schema2={schema2}",
        "-v", f"on_serenedb={'true' if os.environ.get('SDB_DRV_ENGINE','serenedb')=='serenedb' else 'false'}",
    ]
    # Pin EDITOR inside psql so \e/\ef/\ev produce deterministic output
    # regardless of what editors happen to be installed. Done as a
    # prepended `\setenv` (not as a host env var) because `docker exec`
    # doesn't forward host env to the container -- this is the one knob
    # that works for both the local-psql and docker-psql transports.
    psql_input = "\\setenv EDITOR /bin/true\n" + cmd_text
    proc = subprocess.run(args, input=psql_input, capture_output=True, text=True, timeout=30)
    return proc.stdout + proc.stderr


def case_name(cmd: str) -> str:
    s = cmd.replace("<NL>", " ; ")
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")
    return s[:80] or "anon"


def xml_escape(text: str) -> str:
    return html.escape(text, quote=True)


def make_diff(expected: str, actual: str) -> str:
    import difflib
    diff = difflib.unified_diff(
        expected.splitlines(),
        actual.splitlines(),
        fromfile="expected (golden)",
        tofile="actual",
        lineterm="",
    )
    return "\n".join(list(diff)[:300])


def drop_lines(text: str, skip_lines: list[int]) -> str:
    """Drop the lines listed by 1-based index from `text`. Used at compare
    time to exclude non-deterministic lines (e.g. \\conninfo's Backend PID)
    from the diff. Both sides go through this so the comparison is
    apples-to-apples.

    Also block-localized: when skip_lines is non-empty we additionally
    collapse psql's cell trailing-pad and border-dash runs so that the
    column-width drift caused by the dropped (variable) cell doesn't
    ripple through every surrounding line. Other tests (no skip_lines)
    keep their nicely-aligned goldens."""
    if not skip_lines:
        return text
    skip = {n - 1 for n in skip_lines}  # 1-based -> 0-based
    text = re.sub(r"\| {2,}", "| ", text)   # collapse leading cell-pad
    text = re.sub(r" {2,}\|", " |", text)   # collapse trailing cell-pad
    text = re.sub(r"\+-{2,}", "+-", text)   # collapse border-dash runs
    return "\n".join(ln for i, ln in enumerate(text.splitlines()) if i not in skip)


def all_help_tokens(host: str, port: str, db: str, user: str) -> set[str]:
    """Every backslash command listed by psql's `\\?` -- used for coverage check."""
    txt = run_psql(host, port, db, user, "", "", "", "\\?")
    return set(re.findall(r"\\[a-zA-Z?!][a-zA-Z?!_]*", txt))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--update", action="store_true",
                    help="Rewrite goldens in tests.test from current engine output")
    ap.add_argument("--test-file", default="tests.test",
                    help="Path to the .test file (default: tests.test next to this script)")
    args = ap.parse_args()

    here = Path(__file__).resolve().parent
    test_file = Path(args.test_file)
    if not test_file.is_absolute():
        test_file = here / test_file

    driver = os.environ.get("SDB_DRV_DRIVER", "")
    if driver and "psql_psql" not in driver.split(","):
        print("[psql] psql_psql not selected via SDB_DRV_DRIVER; skipping", file=sys.stderr)
        return 0
    probe = _psql_command()
    if subprocess.run(["sh", "-c", f"command -v {shlex.quote(probe[0])}"], capture_output=True).returncode != 0:
        print(f"[psql] {probe[0]} not installed; skipping", file=sys.stderr)
        return 0

    host = os.environ.get("SDB_DRV_HOST", "localhost")
    port = os.environ.get("SDB_DRV_PORT", "5432")
    db = os.environ.get("SDB_DRV_DATABASE", "postgres")
    user = os.environ.get("SDB_DRV_USER", "postgres")
    engine = os.environ.get("SDB_DRV_ENGINE", "serenedb")
    junit_dir = Path(os.environ.get("SDB_DRV_JUNIT", "./out/drivers-tests"))
    run_id = os.environ.get("SDB_DRV_RUN_ID") or "local"
    # Use a fixed-length suffix so psql's column widths (sized to the widest
    # cell) are identical across runs -- otherwise different RUN_IDs would
    # produce different border lengths even after schema-name masking.
    suffix = hashlib.md5(run_id.encode()).hexdigest()[:6]
    schema = f"psqlh_{suffix}"
    schema2 = f"psqlh2_{suffix}"
    junit_dir.mkdir(parents=True, exist_ok=True)
    junit_path = junit_dir / "tests-drivers-psql-junit.xml"

    blocks, trailing = parse_file(test_file)

    # Seed
    seed_path = here / "seed.sql"
    if seed_path.exists():
        seed_sql = seed_path.read_text()
        seed_out = run_psql(host, port, db, user, schema, schema2, suffix, seed_sql)
        if re.search(r"(?m)^(psql:|ERROR:|FATAL:)", seed_out):
            sys.stderr.write(f"[psql] seed failed:\n{seed_out}\n")
            # Emit JUnit with a single seed failure and bail.
            xml = (
                '<?xml version="1.0" encoding="UTF-8"?>\n'
                f'<testsuite name="psql" tests="1" failures="1" errors="0" skipped="0">\n'
                f'  <testcase name="seed" classname="psql">\n'
                f'    <failure message="seed failed">{xml_escape(seed_out)}</failure>\n'
                "  </testcase>\n</testsuite>\n"
            )
            junit_path.write_text(xml)
            return 1

    try:
        # Coverage: every `\?` command must be present somewhere in tests.test.
        help_tokens = all_help_tokens(host, port, db, user)
        listed: set[str] = set()
        for blk in blocks:
            listed.update(re.findall(r"\\[a-zA-Z?!][a-zA-Z?!_]*", blk.cmd))
        missing = sorted(help_tokens - listed)

        cases: list[str] = []
        ntests = nfail = nskip = 0

        if missing:
            ntests += 1
            nfail += 1
            cases.append(
                f'  <testcase name="coverage_all_commands_listed" classname="psql">\n'
                f'    <failure message="psql lists commands absent from tests.test">'
                f'{xml_escape("Add to tests/drivers/psql/tests.test (active or skipif): " + " ".join(missing))}'
                f'</failure>\n  </testcase>\n'
            )
        else:
            ntests += 1
            cases.append('  <testcase name="coverage_all_commands_listed" classname="psql"/>\n')

        for blk in blocks:
            name = case_name(blk.cmd)
            if engine in blk.skipifs:
                ntests += 1
                nskip += 1
                cases.append(
                    f'  <testcase name="{xml_escape(name)}" classname="psql">'
                    f'<skipped message="skipif {xml_escape(engine)}"/></testcase>\n'
                )
                continue

            actual_raw = run_psql(host, port, db, user, schema, schema2, suffix, blk.cmd)
            actual = normalize(actual_raw, schema, schema2)

            if args.update:
                blk.golden = actual
                ntests += 1
                cases.append(f'  <testcase name="{xml_escape(name)}" classname="psql"/>\n')
                continue

            actual_cmp = drop_lines(actual, blk.skip_lines)
            golden_cmp = drop_lines(blk.golden, blk.skip_lines)
            if actual_cmp == golden_cmp:
                ntests += 1
                cases.append(f'  <testcase name="{xml_escape(name)}" classname="psql"/>\n')
            else:
                diff = make_diff(golden_cmp, actual_cmp)
                ntests += 1
                nfail += 1
                cases.append(
                    f'  <testcase name="{xml_escape(name)}" classname="psql">\n'
                    f'    <failure message="{xml_escape(engine)} output differs from golden">'
                    f'{xml_escape(diff)}</failure>\n  </testcase>\n'
                )
                print(f"[psql] FAIL {name}", file=sys.stderr)

        if args.update:
            emit_file(test_file, blocks, trailing)
            print(f"[psql] updated goldens in {test_file}", file=sys.stderr)

        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<testsuite name="psql" tests="{ntests}" failures="{nfail}" errors="0" skipped="{nskip}">\n'
            + "".join(cases)
            + "</testsuite>\n"
        )
        junit_path.write_text(xml)
        print(f"[psql] tests={ntests} failures={nfail} skipped={nskip} -> {junit_path}", file=sys.stderr)
        return 0 if nfail == 0 else 1
    finally:
        # Best-effort schema cleanup
        run_psql(host, port, db, user, schema, schema2, suffix,
                 f'DROP SCHEMA IF EXISTS "{schema}" CASCADE; DROP SCHEMA IF EXISTS "{schema2}" CASCADE;')


if __name__ == "__main__":
    sys.exit(main())
