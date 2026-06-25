#!/usr/bin/env python3
"""Generate and verify the serened CLI help reference.

Single source of truth: the `serened --help` banner. This tool never scrapes
C++ source -- whatever the binary prints is the contract.

Two modes:

  override   Run `serened --help`, parse it, write the reference JSON.
  check      Run `serened --help`, parse it, diff against the committed
             reference JSON. Exits non-zero on any drift.

The reference JSON is consumed by the docs site (serenedb-site), so its path
and shape are a published contract -- keep them stable.

Usage:
  python3 cli_help.py override [--bin PATH] [--out PATH]
  python3 cli_help.py check    [--bin PATH] [--reference PATH]
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[2]

# The docs site reads this exact path (raw GitHub + local dev). Don't move it
# without updating serene-cli-help-docs in serenedb-site.
DEFAULT_REFERENCE_PATH = (
    REPO_ROOT / "tests" / "drivers" / "python" / "fixtures" / "serened_cli_help.json"
)

DEFAULT_BINARY = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(REPO_ROOT / "build" / "bin" / "serened"),
)

# `serened --help` already groups flags under "Flags from <id>:" headings. These
# ids ARE the categories; labels and ordering are presentation only.
CATEGORY_LABELS = {
    "server": "Server",
    "http": "HTTP",
    "ssl": "TLS / SSL",
    "log": "Logging",
}
CATEGORY_ORDER = ["server", "http", "ssl", "log"]


# --------------------------------------------------------------------------
# Running the binary
# --------------------------------------------------------------------------


def run_help(binary: str = DEFAULT_BINARY, *, timeout: float = 15.0) -> str | None:
    """Return the `serened --help` banner, or None if the binary is absent."""
    env = {**os.environ, "NO_COLOR": "1", "TERM": "dumb"}
    try:
        result = subprocess.run(
            [binary, "--help"],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
    except FileNotFoundError:
        return None

    output = "\n".join(part for part in (result.stdout, result.stderr) if part)
    if "Usage:" not in output:
        raise RuntimeError(
            f"Help output from {binary} did not contain a Usage section",
        )
    return output


# --------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------

_PROGNAME_PREFIX = re.compile(r"^\S+:\s+")
_GROUP_HEADING = re.compile(r"^\s*Flags from (.+?):\s*$")
_FLAG_START = re.compile(r"^\s+--([A-Za-z0-9_]+)(?=\s|\(|$)")
_LIST_ITEM = re.compile(r"^\s*(--\S+)\s+(.*)$")
# A reconstructed flag entry: --name (description); default: value;
_FLAG_ENTRY = re.compile(
    r"^--(?P<name>[A-Za-z0-9_]+)\s+\((?P<desc>.*)\);\s*default:\s*(?P<default>.*?);?\s*$"
)


def _command_description(command: str) -> str:
    if " shell " in command:
        return "Open the embedded DuckDB shell without starting the SereneDB server."
    if " psql " in command:
        return "Open the psql-compatible shell subcommand without starting the SereneDB server."
    return "Start the SereneDB pg-wire server, using the optional data directory and flags."


def _parse_list_section(lines: list[str], heading: str) -> list[dict]:
    """Parse a `Heading:` block of `--flag   description` items until a blank line."""
    start = next(
        (i for i, line in enumerate(lines) if line.strip().startswith(heading)),
        -1,
    )
    if start < 0:
        return []

    items: list[dict] = []
    current: dict | None = None
    for line in lines[start + 1:]:
        if not line.strip():
            break
        match = _LIST_ITEM.match(line)
        if match:
            current = {"flag": match.group(1), "description": match.group(2).strip()}
            items.append(current)
        elif current is not None:
            current["description"] = f"{current['description']} {line.strip()}".strip()
    return items


def _parse_usage(lines: list[str]) -> list[str]:
    start = next((i for i, line in enumerate(lines) if line.strip() == "Usage:"), -1)
    if start < 0:
        return []
    usage: list[str] = []
    for line in lines[start + 1:]:
        if not line.strip():
            break
        usage.append(line.strip())
    return usage


def _normalize_default(raw: str) -> str:
    """Strip the trailing `;`, then unwrap a surrounding pair of double quotes."""
    value = raw.strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1]
    return value


def _parse_flag_groups(lines: list[str]) -> list[dict]:
    """Parse the `Flags from <id>:` sections into ordered groups of flags.

    Flag entries wrap across lines; we stitch each entry back into one logical
    string before matching, so multi-line descriptions and defaults parse cleanly.
    """
    groups: dict[str, dict] = {}
    current_group: str | None = None
    pending: list[str] = []

    def flush() -> None:
        nonlocal pending
        if not pending or current_group is None:
            pending = []
            return
        entry = re.sub(r"\s+", " ", " ".join(pending)).strip()
        match = _FLAG_ENTRY.match(entry)
        pending = []
        if not match:
            return
        name = match.group("name")
        label = CATEGORY_LABELS.get(current_group, current_group)
        groups[current_group]["flags"].append(
            {
                "name": name,
                "flag": f"--{name}",
                # --help never reports a type; the field stays for schema parity.
                "type": "",
                "defaultValue": _normalize_default(match.group("default")),
                "description": match.group("desc").strip(),
                # --help groups by category only; source mirrors the group id.
                "source": current_group,
                "category": current_group,
                "categoryLabel": label,
            }
        )

    for line in lines:
        heading = _GROUP_HEADING.match(line)
        if heading:
            flush()
            current_group = heading.group(1).strip()
            groups.setdefault(
                current_group,
                {
                    "id": current_group,
                    "label": CATEGORY_LABELS.get(current_group, current_group),
                    "flags": [],
                },
            )
            continue

        if current_group is None:
            continue

        if not line.strip():
            flush()
            current_group = None
            continue

        if _FLAG_START.match(line):
            flush()
            pending = [line.strip()]
        elif pending:
            pending.append(line.strip())

    flush()

    def sort_key(group_id: str) -> tuple:
        order = CATEGORY_ORDER.index(group_id) if group_id in CATEGORY_ORDER else 999
        return (order, groups[group_id]["label"])

    return [groups[key] for key in sorted(groups, key=sort_key)]


def parse_help(help_text: str) -> dict:
    """Turn a `serened --help` banner into the reference structure."""
    lines = help_text.splitlines()

    title_line = next((line.strip() for line in lines if line.strip()), "serened")
    # absl prints "<progname>: <usage message>"; drop the progname prefix.
    title = _PROGNAME_PREFIX.sub("", title_line)

    usage = _parse_usage(lines)

    return {
        "generatedFrom": "serened --help",
        "title": title,
        "usage": usage,
        "commands": [
            {"command": command, "description": _command_description(command)}
            for command in usage
        ],
        "flagSources": _parse_list_section(lines, "Flag sources"),
        "helpVariants": _parse_list_section(lines, "Help variants"),
        "flagGroups": _parse_flag_groups(lines),
    }


# --------------------------------------------------------------------------
# Reference IO + comparison
# --------------------------------------------------------------------------


def build_reference(binary: str = DEFAULT_BINARY) -> dict | None:
    """Parse the reference from the binary, or None if the binary is absent."""
    help_text = run_help(binary)
    if help_text is None:
        return None
    return parse_help(help_text)


def _serialize(data: dict) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False) + "\n"


def write_reference(out_path: Path, data: dict) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(_serialize(data), encoding="utf8")


def read_reference(reference_path: Path = DEFAULT_REFERENCE_PATH) -> dict:
    return json.loads(Path(reference_path).read_text(encoding="utf8"))


def diff_reference(expected: dict, actual: dict) -> str:
    """Unified diff of the serialized reference (empty when they match)."""
    return "".join(
        difflib.unified_diff(
            _serialize(expected).splitlines(keepends=True),
            _serialize(actual).splitlines(keepends=True),
            fromfile="reference (committed)",
            tofile="actual (serened --help)",
        )
    )


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _cmd_override(args: argparse.Namespace) -> int:
    data = build_reference(args.bin)
    if data is None:
        print(
            f"serened binary not found or not executable: {args.bin}\n"
            "Set SDB_DRV_SERENED_BIN or pass --bin.",
            file=sys.stderr,
        )
        return 1
    out = Path(args.out)
    write_reference(out, data)
    flag_count = sum(len(group["flags"]) for group in data["flagGroups"])
    print(
        f"Wrote {out} from {data['generatedFrom']}: "
        f"{len(data['commands'])} commands, {flag_count} flags"
    )
    return 0


def _cmd_check(args: argparse.Namespace) -> int:
    actual = build_reference(args.bin)
    if actual is None:
        print(f"serened binary not found: {args.bin}; skipping check", file=sys.stderr)
        return 0
    expected = read_reference(Path(args.reference))
    diff = diff_reference(expected, actual)
    if not diff:
        print("serened CLI help reference is up to date.")
        return 0
    print("serened CLI help reference is out of date:\n", file=sys.stderr)
    print(diff, file=sys.stderr)
    print(
        "\nIf this change is expected, regenerate with:\n"
        "  python3 cli_help.py override",
        file=sys.stderr,
    )
    return 1


def main(argv: list[str] | None = None) -> int:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--bin", default=DEFAULT_BINARY, help="path to the serened binary")

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)

    p_override = sub.add_parser(
        "override", parents=[common], help="generate the reference JSON"
    )
    p_override.add_argument("--out", default=str(DEFAULT_REFERENCE_PATH))
    p_override.set_defaults(func=_cmd_override)

    p_check = sub.add_parser(
        "check", parents=[common], help="diff the binary against the reference JSON"
    )
    p_check.add_argument("--reference", default=str(DEFAULT_REFERENCE_PATH))
    p_check.set_defaults(func=_cmd_check)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
