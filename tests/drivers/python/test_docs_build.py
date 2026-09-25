from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SCRIPTS = REPO / "scripts"


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, *args], capture_output=True,
                          text=True, timeout=300)


def test_index_embedding_prints_every_file_by_size(tmp_path: Path) -> None:
    index = tmp_path / "index"
    index.mkdir()
    sizes = {"small": 10, "large": 3 * 1024 * 1024 // 2, "medium": 2048}
    for name, size in sizes.items():
        (index / name).write_bytes(b"x" * size)
    out = tmp_path / "docs_index_data.cpp"
    r = _run(str(SCRIPTS / "generate_docs_index.py"), str(index), str(out))
    assert r.returncode == 0, r.stderr
    lines = r.stdout.splitlines()
    assert lines[0] == (f"embedded docs index: 3 files, 1.5 MiB "
                        f"({sum(sizes.values())} bytes) -> {out}")
    assert [line.split() for line in lines[1:]] == [
        ["large", "1.5", "MiB"], ["medium", "2.0", "KiB"], ["small", "10", "B"]]
    assert "#embed" in out.read_text()


def test_docs_generation_reports_the_embedded_text(tmp_path: Path) -> None:
    out = tmp_path / "docs_data.cpp"
    r = _run(str(SCRIPTS / "generate_docs.py"), str(REPO / "docs"), str(out),
             "--tests-dir", str(REPO / "tests" / "sqllogic"))
    assert r.returncode == 0, r.stderr
    summary = r.stdout.splitlines()[-1]
    assert summary.startswith(f"generated {out}: "), summary
    match = re.search(
        r", [0-9.]+ (?:B|KiB|MiB) \(([0-9]+) bytes\) of embedded text$",
        summary)
    assert match, summary
    assert 0 < int(match.group(1)) < out.stat().st_size


def test_docs_generation_turns_admonitions_into_quotes(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "notes.md").write_text(
        "---\ntitle: Notes\nsplit: page\n---\n"
        "Intro.\n\n"
        ":::note Keep `this` title\nBody line.\n\n"
        "| a | b |\n| --- | --- |\n| 1 | 2 |\n:::\n\n"
        ":::caution\nCareful.\n:::\n",
        encoding="utf-8")
    out = tmp_path / "docs_data.cpp"
    r = _run(str(SCRIPTS / "generate_docs.py"), str(docs), str(out),
             "--tests-dir", str(REPO / "tests" / "sqllogic"))
    assert r.returncode == 0, r.stderr
    assert ('R"sdbdoc(Intro.\n\n'
            "> **Note: Keep `this` title**\n>\n> Body line.\n>\n"
            "> | a | b |\n> | --- | --- |\n> | 1 | 2 |\n\n"
            '> **Caution**\n>\n> Careful.\n)sdbdoc"') in out.read_text()
