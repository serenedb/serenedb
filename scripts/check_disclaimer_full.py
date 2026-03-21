#!/usr/bin/env python3
"""
Heavy disclaimer checker. Run manually to catch year issues, company name
typos, wrong boilerplate, etc.

Usage:
    python3 scripts/check_disclaimer_full.py [files...]
    python3 scripts/check_disclaimer_full.py  # scans whole repo
"""

import difflib
import re
import subprocess
import sys
from pathlib import Path

EXCEPTIONS = {
    "libs/iresearch/include/iresearch/parser/lucene_parser.hpp",
    "libs/iresearch/include/iresearch/parser/lucene_parser.cpp",
    "libs/iresearch/include/iresearch/parser/lucene_lexer.cpp",
    "libs/iresearch/include/iresearch/utils/fstext/fst_draw.hpp",
    "libs/vpack/include/vpack/wyhash.h",
    "libs/vpack/include/vpack/events_from_slice.h",
    "libs/basics/logger/syslog_names.h",
    "server/pg/protocol.h",
    "server/pg/functions/interval.cpp",
    "tests/bench/micro/call_once.cpp",
    "tests/bench/micro/random.cpp",
    "tests/bench/micro/function.cpp",
    "libs/vpack/src/asm-utf8check.cpp",
    "libs/vpack/src/asm-utf8check.h",
    "libs/vpack/include/vpack/validation_types.h",
    "libs/vpack/include/vpack/validation.h",
    "libs/vpack/src/validation.cpp",
    "tests/libs/fuerte/main.cpp",
}

BORDER = "/" * 80
EXPECTED_SERENEDB_COMPANY = "SereneDB GmbH, Berlin, Germany"
SERENEDB_FIRST_YEAR = 2025
CURRENT_YEAR = 2026

# Lines between the copyright block and "Copyright holder", in order
EXPECTED_BOILERPLATE = [
    '/// Licensed under the Apache License, Version 2.0 (the "License");',
    "/// you may not use this file except in compliance with the License.",
    "/// You may obtain a copy of the License at",
    "///",
    "///     http://www.apache.org/licenses/LICENSE-2.0",
    "///",
    "/// Unless required by applicable law or agreed to in writing, software",
    '/// distributed under the License is distributed on an "AS IS" BASIS,',
    "/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
    "/// See the License for the specific language governing permissions and",
    "/// limitations under the License.",
]

COPYRIGHT_LINE_RE = re.compile(r"^/// Copyright (.+)$")
YEAR_RE = re.compile(r"^(\d{4})(-(\d{4}))?$")
COMPANY_RE = re.compile(r"^(\d{4}(-\d{4})?) (.+)$")


def similar(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio()


def check_year(year_str: str) -> list[str]:
    m = YEAR_RE.match(year_str)
    if not m:
        return [f"invalid year format: {year_str!r}"]
    errors = []
    start = int(m.group(1))
    end = int(m.group(3)) if m.group(3) else start
    if m.group(3):
        errors.append(f"SereneDB copyright must be a single year, not a range: {year_str!r}")
    if start < SERENEDB_FIRST_YEAR:
        errors.append(f"SereneDB year must be >= {SERENEDB_FIRST_YEAR}, got {start}")
    if start > CURRENT_YEAR:
        errors.append(f"year {start} is in the future (current: {CURRENT_YEAR})")
    return errors


def check_serenedb_company(company: str) -> list[str]:
    """Only flag if the company name looks like a misspelling of SereneDB."""
    if company == EXPECTED_SERENEDB_COMPANY:
        return []
    ratio = similar(company, EXPECTED_SERENEDB_COMPANY)
    if ratio > 0.85:
        return [f"company name looks like a typo of SereneDB (similarity {ratio:.0%}):\n    got:      {company!r}\n    expected: {EXPECTED_SERENEDB_COMPANY!r}"]
    return []  # unrelated company, not our concern


def check_copyright_line(line: str) -> list[str]:
    """Parse and validate a '/// Copyright ...' line."""
    m = COPYRIGHT_LINE_RE.match(line)
    if not m:
        return [f"not a copyright line: {line!r}"]
    rest = m.group(1)
    # "holder is <company>" format
    if rest.startswith("holder is "):
        company = rest[len("holder is "):]
        return check_serenedb_company(company)
    # "<year(s)> <company>" format
    cm = COMPANY_RE.match(rest)
    if not cm:
        return [f"cannot parse copyright: {rest!r}"]
    company = cm.group(3)
    errors = check_serenedb_company(company)
    if company == EXPECTED_SERENEDB_COMPANY:
        errors += check_year(cm.group(1))
    return errors


def check_boilerplate_line(actual: str, expected: str, lineno: int) -> str | None:
    if actual == expected:
        return None
    ratio = similar(actual, expected)
    label = f"typo (similarity {ratio:.0%})" if ratio > 0.7 else "wrong line"
    return f"line {lineno} {label}:\n    got:      {actual!r}\n    expected: {expected!r}"


def parse_disclaimer(lines: list[str], start: int) -> dict | None:
    """
    Parse disclaimer block starting at `start`. Returns a dict with keys:
      border_open, copyright_lines, sep, boilerplate, holder_line, border_close
    or None if structure is unrecognizable.
    """
    i = start
    if i >= len(lines) or lines[i] != BORDER:
        return None
    i += 1
    if i >= len(lines) or lines[i] != "/// DISCLAIMER":
        return None
    i += 1
    if i >= len(lines) or lines[i] != "///":
        return None
    i += 1

    # One or more copyright lines
    copyright_lines = []
    while i < len(lines) and COPYRIGHT_LINE_RE.match(lines[i]) and "holder" not in lines[i]:
        copyright_lines.append((i + 1, lines[i]))
        i += 1

    if not copyright_lines:
        return None

    if i >= len(lines) or lines[i] != "///":
        return None
    i += 1

    # Boilerplate
    boilerplate = []
    for _ in EXPECTED_BOILERPLATE:
        if i >= len(lines):
            break
        boilerplate.append((i + 1, lines[i]))
        i += 1

    if i >= len(lines) or lines[i] != "///":
        return None
    i += 1

    # Copyright holder line
    if i >= len(lines) or not lines[i].startswith("/// Copyright holder"):
        return None
    holder = (i + 1, lines[i])
    i += 1

    # Skip optional trailing lines (e.g. @author) until closing border
    while i < len(lines) and lines[i] != BORDER:
        i += 1

    if i >= len(lines):
        return None

    return {
        "copyright_lines": copyright_lines,
        "boilerplate": boilerplate,
        "holder": holder,
    }


def check_file(path: str) -> list[str]:
    try:
        text = Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        return [f"cannot read: {e}"]

    lines = text.splitlines()

    try:
        disclaimer_line = next(i for i, l in enumerate(lines) if l == "/// DISCLAIMER")
    except StopIteration:
        return ["missing disclaimer"]

    start = disclaimer_line - 1
    block = parse_disclaimer(lines, start)
    if block is None:
        return ["disclaimer block has unexpected structure"]

    errors = []

    for lineno, line in block["copyright_lines"]:
        for e in check_copyright_line(line):
            errors.append(f"line {lineno}: {e}")

    for (lineno, actual), expected in zip(block["boilerplate"], EXPECTED_BOILERPLATE):
        err = check_boilerplate_line(actual, expected, lineno)
        if err:
            errors.append(err)

    lineno, holder_line = block["holder"]
    for e in check_copyright_line(holder_line):
        errors.append(f"line {lineno}: {e}")

    return errors


GLOBAL_EXCLUDE_RE = re.compile(r"^(third_party|resources)/")


def find_source_files(root: str) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--", "*.cpp", "*.cc", "*.c", "*.hpp", "*.hh", "*.h", "*.ipp", "*.tpp"],
        cwd=root,
        capture_output=True,
        text=True,
    )
    return [p for p in result.stdout.splitlines() if not GLOBAL_EXCLUDE_RE.match(p)]


def main() -> int:
    root = Path(__file__).parent.parent
    if len(sys.argv) > 1:
        files = sys.argv[1:]
    else:
        files = find_source_files(str(root))

    failed = 0
    for path in files:
        if path in EXCEPTIONS:
            continue
        abs_path = str(root / path) if not Path(path).is_absolute() else path
        errors = check_file(abs_path)
        if errors:
            print(f"{path}:")
            for e in errors:
                for line in e.splitlines():
                    print(f"  {line}")
            failed += 1

    if failed:
        print(f"\n{failed} file(s) with disclaimer issues.")
    else:
        print("All disclaimers OK.")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
