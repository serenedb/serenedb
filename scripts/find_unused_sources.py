#!/usr/bin/env python3
"""Find unused C/C++ source files.

A file is considered "used" if it is either (a) a compiled translation unit
listed in compile_commands.json, or (b) transitively reachable via quoted
`#include "..."` directives from such a translation unit.

Everything else (inside the project but not under third_party) is reported.

Usage:
    scripts/find_unused_sources.py [--build-dir build] [--delete]

Without --delete the script only prints the unused files, one per line.
With --delete it removes them.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
THIRD_PARTY = REPO_ROOT / "third_party"
# Any directory under the repo root that we should never descend into when
# looking for "project" sources. build*/, .cache/, generated output, Rust target
# dirs, etc.
EXCLUDED_ROOTS = {
    REPO_ROOT / "build_perf",
    REPO_ROOT / "build_clangd",
    REPO_ROOT / "third_party",
    REPO_ROOT / "build",
    REPO_ROOT / "build_clangd",
    REPO_ROOT / "build_data",
    REPO_ROOT / ".cache",
    REPO_ROOT / ".git",
    REPO_ROOT / "out",
    REPO_ROOT / "logs",
    REPO_ROOT / "serenedb-data",
    REPO_ROOT / "serene-ui" / "node_modules",
}

SRC_EXTS = {".cpp", ".cc", ".cxx", ".c"}
HDR_EXTS = {".hpp", ".hh", ".hxx", ".h"}
ALL_EXTS = SRC_EXTS | HDR_EXTS

INCLUDE_RE = re.compile(
    r'^\s*#\s*include\s*(?:"([^"]+)"|<([^>]+)>)', re.MULTILINE
)


def is_excluded(p: Path) -> bool:
    for root in EXCLUDED_ROOTS:
        try:
            p.relative_to(root)
            return True
        except ValueError:
            continue
    return False


def in_third_party(p: Path) -> bool:
    try:
        p.relative_to(THIRD_PARTY)
        return True
    except ValueError:
        return False


def in_repo(p: Path) -> bool:
    try:
        p.relative_to(REPO_ROOT)
        return True
    except ValueError:
        return False


def parse_include_dirs(command: str) -> list[Path]:
    """Extract -I include paths from a compile command string."""
    try:
        tokens = shlex.split(command)
    except ValueError:
        tokens = command.split()
    dirs: list[Path] = []
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if t == "-I" and i + 1 < len(tokens):
            dirs.append(Path(tokens[i + 1]))
            i += 2
            continue
        if t.startswith("-I"):
            dirs.append(Path(t[2:]))
        elif t == "-isystem" and i + 1 < len(tokens):
            dirs.append(Path(tokens[i + 1]))
            i += 2
            continue
        i += 1
    return dirs


def load_compile_commands(path: Path):
    with path.open() as f:
        return json.load(f)


def resolve_include(inc: str, source_dir: Path, include_dirs: list[Path]) -> Path | None:
    """Try to resolve a quoted include against the source file's dir and -I paths."""
    candidates = [source_dir / inc] + [d / inc for d in include_dirs]
    for c in candidates:
        if c.is_file():
            return c.resolve()
    return None


def read_includes(file_path: Path) -> list[str]:
    try:
        text = file_path.read_text(errors="replace")
    except OSError:
        return []
    out: list[str] = []
    for quoted, angled in INCLUDE_RE.findall(text):
        out.append(quoted or angled)
    return out


def gather_used(compile_commands) -> set[Path]:
    """BFS over #include edges starting from compiled translation units."""
    # Map file -> include dirs from its compile entry.
    entry_include_dirs: dict[Path, list[Path]] = {}
    for entry in compile_commands:
        f = Path(entry["file"]).resolve()
        entry_include_dirs[f] = parse_include_dirs(entry.get("command", ""))

    # Union of all include dirs as a fallback (used when we traverse headers
    # that weren't themselves compile units).
    all_include_dirs: list[Path] = []
    seen_dirs: set[Path] = set()
    for dirs in entry_include_dirs.values():
        for d in dirs:
            if d not in seen_dirs:
                seen_dirs.add(d)
                all_include_dirs.append(d)

    used: set[Path] = set()
    queue: list[Path] = []
    for f in entry_include_dirs:
        if in_repo(f) and not is_excluded(f):
            if f not in used:
                used.add(f)
                queue.append(f)

    while queue:
        current = queue.pop()
        inc_dirs = entry_include_dirs.get(current, all_include_dirs)
        for inc in read_includes(current):
            resolved = resolve_include(inc, current.parent, inc_dirs) or \
                       resolve_include(inc, current.parent, all_include_dirs)
            if resolved is None:
                continue
            if not in_repo(resolved) or is_excluded(resolved):
                continue
            if resolved in used:
                continue
            used.add(resolved)
            # Only traverse further through files that might have includes.
            if resolved.suffix in ALL_EXTS:
                queue.append(resolved)
    return used


def gather_all_sources() -> set[Path]:
    """All cpp/hpp/h files in the project, excluding third_party and build dirs."""
    out: set[Path] = set()
    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        d = Path(dirpath).resolve()
        if is_excluded(d):
            dirnames[:] = []
            continue
        dirnames[:] = [x for x in dirnames
                       if not is_excluded((d / x).resolve())
                       and x not in {"node_modules", "_deps", "CMakeFiles"}]
        for fn in filenames:
            p = d / fn
            if p.suffix in ALL_EXTS:
                out.add(p.resolve())
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--build-dir", default="build",
                    help="Build directory containing compile_commands.json "
                         "(default: build)")
    ap.add_argument("--delete", action="store_true",
                    help="Delete the unused files. Without this flag the "
                         "script only prints them.")
    ap.add_argument("--headers-only", action="store_true",
                    help="Only report/delete unused .hpp/.hh/.hxx/.h files.")
    ap.add_argument("--sources-only", action="store_true",
                    help="Only report/delete unused .cpp/.cc/.cxx/.c files.")
    args = ap.parse_args()

    cc_path = (REPO_ROOT / args.build_dir / "compile_commands.json").resolve()
    if not cc_path.is_file():
        print(f"error: {cc_path} not found", file=sys.stderr)
        return 1

    commands = load_compile_commands(cc_path)
    used = gather_used(commands)
    all_sources = gather_all_sources()
    unused = sorted(all_sources - used)

    if args.headers_only:
        unused = [p for p in unused if p.suffix in HDR_EXTS]
    elif args.sources_only:
        unused = [p for p in unused if p.suffix in SRC_EXTS]

    for p in unused:
        print(p.relative_to(REPO_ROOT))

    if args.delete:
        for p in unused:
            try:
                p.unlink()
            except OSError as e:
                print(f"warning: failed to delete {p}: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
