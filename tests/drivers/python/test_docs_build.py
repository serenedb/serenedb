from __future__ import annotations

import re
import struct
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SCRIPTS = REPO / "scripts"
REGION_TAG = b"sdb-docs-index-1"


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, *args], capture_output=True,
                          text=True, timeout=300)


def _write_macho(path: Path, capacity: int, regions: int = 1) -> None:
    region = (REGION_TAG + struct.pack("<QQ", capacity, 0) + bytes(32) +
              bytes(capacity))
    path.write_bytes(b"\xcf\xfa\xed\xfe" + bytes(60) + region * regions +
                     b"tail")


def _write_elf(path: Path, last: bool = True) -> None:
    names = b"\0.text\0.sdb_docs\0.comment\0.aligned\0.shstrtab\0"
    loads = [(1, 5, 0, 0, 0, 0x140, 0x140, 0x1000),
             (1, 4, 0x140, 0x1140, 0x1140, 64, 64, 0x1000)]
    if not last:
        loads.append((1, 4, 0, 0x3000, 0x3000, 0x40, 0x40, 0x1000))
    sections = [
        (0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        (names.index(b".text"), 1, 6, 0x100, 0x100, 0x40, 0, 0, 16, 0),
        (names.index(b".sdb_docs"), 1, 2, 0x1140, 0x140, 64, 0, 0, 64, 0),
        (names.index(b".comment"), 1, 0x30, 0, 0x180, 11, 0, 0, 1, 1),
        (names.index(b".aligned"), 1, 0, 0, 0x190, 16, 0, 0, 8, 0),
        (names.index(b".shstrtab"), 3, 0, 0, 0x1a0, len(names), 0, 0, 1, 0),
    ]
    shoff = 0x1a0 + len(names) + -(0x1a0 + len(names)) % 8
    data = bytearray(shoff + 64 * len(sections))
    struct.pack_into("<16sHHIQQQIHHHHHH", data, 0, b"\x7fELF\x02\x01\x01", 3,
                     62, 1, 0x100, 64, shoff, 0, 64, 56, len(loads), 64,
                     len(sections), len(sections) - 1)
    for i, load in enumerate(loads):
        struct.pack_into("<IIQQQQQQ", data, 64 + 56 * i, *load)
    data[0x100:0x140] = b"\x90" * 0x40
    data[0x140:0x180] = (REGION_TAG + bytes(16)).ljust(64, b"\0")
    data[0x180:0x18b] = b"sdb comment"
    data[0x190:0x1a0] = b"8-aligned-bytes!"
    data[0x1a0:0x1a0 + len(names)] = names
    for i, section in enumerate(sections):
        struct.pack_into("<IIQQQQIIQQ", data, shoff + 64 * i, *section)
    path.write_bytes(data)


def _read_elf(path: Path) -> tuple[dict[str, tuple[int, int, int]],
                                   list[tuple[int, ...]], bytes]:
    data = path.read_bytes()
    header = struct.unpack_from("<16sHHIQQQIHHHHHH", data, 0)
    phoff, shoff, phnum, shnum, shstrndx = (header[5], header[6], header[10],
                                            header[12], header[13])
    headers = [struct.unpack_from("<IIQQQQIIQQ", data, shoff + 64 * i)
               for i in range(shnum)]
    names = headers[shstrndx][4]
    sections = {
        data[names + h[0]:data.index(b"\0", names + h[0])].decode():
            (h[4], h[5], h[8])
        for h in headers[1:]
    }
    loads = [struct.unpack_from("<IIQQQQQQ", data, phoff + 56 * i)
             for i in range(phnum)]
    return sections, loads, data


def _read_region(path: Path) -> tuple[int, dict[str, list[tuple[str, bytes]]]]:
    data = path.read_bytes()
    at = data.index(REGION_TAG)
    _, size = struct.unpack_from("<QQ", data, at + len(REGION_TAG))
    payload = data[at + 64:at + 64 + size]
    offset = 0
    indexes: dict[str, list[tuple[str, bytes]]] = {}
    for index in ("docs", "objects") if size else ():
        (count,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        files = []
        for _ in range(count):
            (name_size,) = struct.unpack_from("<I", payload, offset)
            offset += 4
            name = payload[offset:offset + name_size].decode()
            offset += name_size
            (file_size,) = struct.unpack_from("<Q", payload, offset)
            offset += 8 + -(offset + 8) % 64
            files.append((name, payload[offset:offset + file_size]))
            offset += file_size
        indexes[index] = files
    return size, indexes


def _write_index(index: Path, sizes: dict[str, int]) -> None:
    for name, size in sizes.items():
        (index / name).parent.mkdir(parents=True, exist_ok=True)
        (index / name).write_bytes(name.encode()[:1] * size)


def test_index_embedding_grows_the_elf_section_to_fit(tmp_path: Path) -> None:
    index = tmp_path / "index"
    sizes = {"docs/small": 10, "docs/large": 3 * 1024 * 1024 // 2,
             "objects/medium": 2048}
    _write_index(index, sizes)
    binary = tmp_path / "serened"
    _write_elf(binary)
    before = binary.stat().st_size
    r = _run(str(SCRIPTS / "embed_docs_index.py"), str(index), str(binary))
    assert r.returncode == 0, r.stderr
    lines = r.stdout.splitlines()
    assert lines[0] == (f"embedded docs index: 3 files, 1.5 MiB "
                        f"({sum(sizes.values())} bytes) -> {binary}")
    assert [line.split() for line in lines[1:]] == [
        ["docs/large", "1.5", "MiB"], ["objects/medium", "2.0", "KiB"],
        ["docs/small", "10", "B"]]
    size, indexes = _read_region(binary)
    assert indexes == {
        "docs": [("large", b"d" * sizes["docs/large"]),
                 ("small", b"d" * sizes["docs/small"])],
        "objects": [("medium", b"o" * sizes["objects/medium"])]}
    sections, loads, data = _read_elf(binary)
    offset, section_size, _ = sections[".sdb_docs"]
    assert (offset, section_size) == (0x140, 64 + size)
    assert loads[1][2:] == (0x140, 0x1140, 0x1140, 64 + size, 64 + size,
                            0x1000)
    assert 0 <= binary.stat().st_size - before - size < 8
    for name, content in ((".comment", b"sdb comment"),
                          (".aligned", b"8-aligned-bytes!")):
        at, length, alignment = sections[name]
        assert data[at:at + length] == content and at % alignment == 0


def test_index_embedding_needs_the_elf_section_last(tmp_path: Path) -> None:
    index = tmp_path / "index"
    _write_index(index, {"docs/segments_1": 1, "objects/segments_1": 1})
    binary = tmp_path / "serened"
    _write_elf(binary, last=False)
    before = binary.read_bytes()
    r = _run(str(SCRIPTS / "embed_docs_index.py"), str(index), str(binary))
    assert r.returncode == 1
    assert ".sdb_docs must be alone in the last loadable segment" in r.stderr
    assert binary.read_bytes() == before


def test_index_embedding_fills_the_macos_region_in_place(
        tmp_path: Path) -> None:
    index = tmp_path / "index"
    _write_index(index, {"docs/segments_1": 100, "objects/segments_1": 1})
    binary = tmp_path / "serened"
    _write_macho(binary, 4096)
    before = binary.stat().st_size
    r = _run(str(SCRIPTS / "embed_docs_index.py"), str(index), str(binary))
    assert r.returncode == 0, r.stderr
    _, indexes = _read_region(binary)
    assert indexes == {"docs": [("segments_1", b"d" * 100)],
                       "objects": [("segments_1", b"o")]}
    assert binary.stat().st_size == before
    assert binary.read_bytes().endswith(b"tail")


def test_index_embedding_needs_both_indexes(tmp_path: Path) -> None:
    index = tmp_path / "index"
    _write_index(index, {"docs/segments_1": 1})
    binary = tmp_path / "serened"
    _write_elf(binary)
    r = _run(str(SCRIPTS / "embed_docs_index.py"), str(index), str(binary))
    assert r.returncode == 1
    assert "no index files under" in r.stderr and "objects" in r.stderr


def test_index_embedding_fails_when_the_index_outgrows_the_macos_region(
        tmp_path: Path) -> None:
    index = tmp_path / "index"
    _write_index(index, {"docs/segments_1": 2048, "objects/segments_1": 1})
    binary = tmp_path / "serened"
    _write_macho(binary, 1024)
    r = _run(str(SCRIPTS / "embed_docs_index.py"), str(index), str(binary))
    assert r.returncode == 1
    assert "1024-byte region; raise kCapacity" in r.stderr
    assert _read_region(binary) == (0, {})


def test_index_embedding_needs_exactly_one_macos_region(
        tmp_path: Path) -> None:
    index = tmp_path / "index"
    _write_index(index, {"docs/segments_1": 1, "objects/segments_1": 1})
    for regions in (0, 2):
        binary = tmp_path / f"serened_{regions}"
        _write_macho(binary, 1024, regions)
        r = _run(str(SCRIPTS / "embed_docs_index.py"), str(index), str(binary))
        assert r.returncode == 1
        assert "expected exactly one docs index region" in r.stderr


def test_docs_generation_writes_the_corpus(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "page.md").write_text(
        "---\ntitle: Page\nsplit: page\n---\nHello corpus.\n",
        encoding="utf-8")
    out = tmp_path / "docs_data.cpp"
    corpus = tmp_path / "docs_corpus.bin"
    r = _run(str(SCRIPTS / "generate_docs.py"), str(docs), str(out),
             "--tests-dir", str(REPO / "tests" / "sqllogic"),
             "--corpus", str(corpus))
    assert r.returncode == 0, r.stderr
    data = corpus.read_bytes()
    rows = []
    offset = 0
    while offset < len(data):
        row = []
        for _ in range(4):
            (size,) = struct.unpack_from("<I", data, offset)
            offset += 4
            row.append(data[offset:offset + size].decode())
            offset += size
        rows.append(row)
    rows_reported = int(re.search(r", ([0-9]+) rows, ", r.stdout).group(1))
    assert len(rows) == rows_reported
    assert any(row[1] == "Page" and "Hello corpus." in row[3] for row in rows)


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


def test_docs_generation_keeps_code_that_starts_with_import_or_export(
        tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "code.md").write_text(
        "---\ntitle: Code\nsplit: page\n---\n"
        'import Tabs from "@theme/Tabs";\n'
        "export const answer = 42;\n\n"
        "export endpoints directly.\n\n"
        '```sh\nexport PATH="/opt/bin:$PATH"\n```\n\n'
        "```python\nimport psycopg\n```\n\n"
        "<Tabs />\n"
        "  indented line\n",
        encoding="utf-8")
    out = tmp_path / "docs_data.cpp"
    r = _run(str(SCRIPTS / "generate_docs.py"), str(docs), str(out),
             "--tests-dir", str(REPO / "tests" / "sqllogic"))
    assert r.returncode == 0, r.stderr
    assert ('R"sdbdoc(export endpoints directly.\n\n'
            '```sh\nexport PATH="/opt/bin:$PATH"\n```\n\n'
            "```python\nimport psycopg\n```\n\n"
            '  indented line\n)sdbdoc"') in out.read_text()


def test_docs_generation_renders_test_directives_as_results(
        tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "retry.md").write_text(
        "---\ntitle: Retry\nsplit: page\n---\n"
        '<SqlLogicTest id="retry/example" />\n',
        encoding="utf-8")
    site_docs = tmp_path / "tests" / "sdb" / "pg" / "site_docs"
    site_docs.mkdir(parents=True)
    (site_docs / "retry.test").write_text(
        "# DOCS_TEST: example\n\n# DOCS_TEST_BODY\n\n"
        "statement ok retry 40 backoff 250ms\nCREATE TABLE t (a INT);\n\n"
        "statement count 2\nINSERT INTO t VALUES (1), (2);\n\n"
        "query\nSELECT count(*) AS n FROM t;\n----\nn\n2\n\n"
        "# DOCS_TEST_END\n\n"
        "statement ok\nDROP TABLE t;\n",
        encoding="utf-8")
    out = tmp_path / "docs_data.cpp"
    r = _run(str(SCRIPTS / "generate_docs.py"), str(docs), str(out),
             "--tests-dir", str(tmp_path / "tests"))
    assert r.returncode == 0, r.stderr
    assert ('R"sdbdoc(```sql\nCREATE TABLE t (a INT);\n\n'
            "INSERT INTO t VALUES (1), (2);\n\n"
            "SELECT count(*) AS n FROM t;\n```\n\n"
            '```\nn\n2\n```\n)sdbdoc"') in out.read_text()


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


def _generate_page(tmp_path: Path, body: str,
                   tests_dir: Path = REPO / "tests" / "sqllogic") -> str:
    """The generated source for a docs tree of one page holding body."""
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "page.md").write_text(
        "---\ntitle: Page\nsplit: page\n---\n" + body, encoding="utf-8")
    out = tmp_path / "docs_data.cpp"
    r = _run(str(SCRIPTS / "generate_docs.py"), str(docs), str(out),
             "--tests-dir", str(tests_dir))
    assert r.returncode == 0, r.stderr
    return out.read_text()


def test_docs_generation_drops_div_wrappers_but_keeps_what_they_hold(
        tmp_path: Path) -> None:
    generated = _generate_page(
        tmp_path,
        "Intro.\n\n"
        '<div className="docs-table-properties">\n\n'
        "| Name | Type |\n| --- | --- |\n| `id` | `INTEGER` |\n\n"
        "</div>\n\n"
        '<div className="docs-spacer"></div>\n\n'
        "<div>\nLoose text.\n</div>\n\n"
        '```html\n<div className="kept">code</div>\n```\n')
    assert ('R"sdbdoc(Intro.\n\n'
            "| Name | Type |\n| --- | --- |\n| `id` | `INTEGER` |\n\n"
            "Loose text.\n\n"
            '```html\n<div className="kept">code</div>\n```\n)sdbdoc"'
            ) in generated


def test_docs_generation_turns_doc_callouts_into_quotes(
        tmp_path: Path) -> None:
    site_docs = tmp_path / "tests" / "sdb" / "pg" / "site_docs"
    site_docs.mkdir(parents=True)
    (site_docs / "callout.test").write_text(
        "# DOCS_TEST: example\n\n# DOCS_TEST_BODY\n\n"
        "query\nSELECT 1 AS n;\n----\nn\n1\n\n"
        "# DOCS_TEST_END\n",
        encoding="utf-8")
    generated = _generate_page(
        tmp_path,
        'import DocCallout from "@site/src/components/DocCallout";\n\n'
        '<DocCallout type="attention">\n\n'
        "First paragraph.\n\n\nSecond paragraph.\n\n"
        "</DocCallout>\n\n"
        '<DocCallout type="tip" title="Forgot the password?">\n'
        "Reset it with `ALTER ROLE`.\n"
        "</DocCallout>\n\n"
        '<DocCallout type="bestPractice">\n'
        "    Indented, with <code>code</code> in it.\n"
        "</DocCallout>\n\n"
        '<DocCallout type="pin">\n- one\n- two\n</DocCallout>\n\n'
        '<DocCallout type="note">\n\nRun it:\n\n'
        '<SqlLogicTest id="callout/example" />\n\n'
        "</DocCallout>\n\n"
        ":::note\nOld style.\n:::\n",
        tmp_path / "tests")
    assert ('R"sdbdoc('
            "> **Attention**\n>\n> First paragraph.\n>\n> Second paragraph.\n\n"
            "> **Forgot the password?**\n>\n> Reset it with `ALTER ROLE`.\n\n"
            "> **Best practice**\n>\n> Indented, with `code` in it.\n\n"
            "> **Note**\n>\n> - one\n> - two\n\n"
            "> **Note**\n>\n> Run it:\n>\n"
            "> ```sql\n> SELECT 1 AS n;\n> ```\n>\n> ```\n> n\n> 1\n> ```\n\n"
            '> **Note**\n>\n> Old style.\n)sdbdoc"') in generated


def test_docs_generation_links_each_image_once(tmp_path: Path) -> None:
    generated = _generate_page(
        tmp_path,
        "Before.\n\n"
        '<img src={useBaseUrl("/images/plan-light.svg")} alt="Query plan" '
        'width="600" className="lightmode-img"/>\n'
        '<img src={useBaseUrl("/images/plan-dark.svg")} alt="Query plan" '
        'width="600" className="darkmode-img"/>\n\n'
        '<img src="/images/zones-light.svg"\n'
        '     alt="Two [time] zones"\n'
        '     class="lightmode-img"\n'
        "     />\n"
        '<img src="/images/zones-dark.svg"\n'
        '     alt="Two [time] zones"\n'
        '     class="darkmode-img"\n'
        "     />\n\n"
        '<img src={require("@site/static/images/matrix.png").default} '
        'title="Cast matrix"/>\n\n'
        '<img src="https://example.com/logo.png">\n\n'
        '```html\n<img src="/images/kept.png" alt="code"/>\n```\n')
    assert ('R"sdbdoc(Before.\n\n'
            "[Image: Query plan](https://serenedb.com/docs/images/plan-light.svg)\n\n"
            "[Image: Two \\[time\\] zones]"
            "(https://serenedb.com/docs/images/zones-light.svg)\n\n"
            "[Image: Cast matrix](https://serenedb.com/docs/images/matrix.png)\n\n"
            "[Image: logo.png](https://example.com/logo.png)\n\n"
            '```html\n<img src="/images/kept.png" alt="code"/>\n```\n)sdbdoc"'
            ) in generated


def test_docs_generation_turns_inline_html_into_markdown(
        tmp_path: Path) -> None:
    generated = _generate_page(
        tmp_path,
        "Download <a href={useBaseUrl(\"/files/docs/flights.csv\")} download>"
        "`flights.csv`</a> or <a href=\"/files/docs/todos.json\" download>"
        "todos</a>.\n"
        'Read the <a href="https://example.com/guide">guide</a>, stored '
        "<strong>unencrypted</strong>, <em>really</em>.\n"
        "Plain <code>'...'</code> quotes; as code: `<a href=\"x\">y</a>`.\n"
        "Tick: <code>a`b</code>.\n"
        "Tags as code: <code>&lt;em&gt;x&lt;/em&gt;</code>, "
        '<a href="#x"><code>&lt;/a&gt;</code></a>, '
        "<code>`a` &lt;b&gt;</code>.\n\n"
        "| Operator | Example |\n| --- | --- |\n"
        '| <a href="#concat"><code>a &#124;&#124; b</code></a> '
        "| <code>'x' &#124;&#124; 'y'</code> |\n\n"
        "## Concatenation {#concat}\n")
    assert ('R"sdbdoc(Download '
            "[`flights.csv`](https://serenedb.com/docs/files/docs/flights.csv)"
            " or [todos](https://serenedb.com/docs/files/docs/todos.json).\n"
            "Read the [guide](https://example.com/guide), stored "
            "**unencrypted**, *really*.\n"
            "Plain `'...'` quotes; as code: `<a href=\"x\">y</a>`.\n"
            "Tick: ``a`b``.\n"
            "Tags as code: `<em>x</em>`, [`</a>`](#x), `` `a` <b> ``.\n\n"
            "| Operator | Example |\n| --- | --- |\n"
            "| [`a \\|\\| b`](#concat) | `'x' \\|\\| 'y'` |\n\n"
            '## Concatenation\n)sdbdoc"') in generated


def test_docs_generation_points_links_to_heading_ids_at_their_sections(
        tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    (docs / "copy").mkdir(parents=True)
    (docs / "guide" / "deep").mkdir(parents=True)
    (docs / "copy" / "index.md").write_text(
        "---\ntitle: COPY\nsplit: headings\n---\n# COPY {#top}\n\n"
        "## `COPY ... FROM` {#copy-from}\n\nFrom.\n\n"
        "## `COPY FROM DATABASE ... TO` {#copy-from-database-to}\n\nTo.\n\n"
        "## `ts_highlight(text)` {#ts_highlight}\n\n"
        "## `f(text)` {#f}\n\n## `f(column)`\n\n"
        "## Twice {#twice-over}\n\n## Again {#twice-over}\n\n"
        "## Operators\n\n"
        "| Operator | Meaning |\n| --- | --- |\n"
        "| [`a \\|\\| b`](#a--b-or) | or |\n\n"
        "#### `a || b` {#a--b-or}\n\n#### `a && b` {#a--b-and}\n\n"
        "#### `a ## b` {#a--b-phrase}\n\n"
        "See [AND](#a--b-and), [FROM](#copy-from), "
        "[`ts_highlight`](#ts_highlight), [f](#f), [top](#top) and "
        "[twice](#twice-over).\n", encoding="utf-8")
    (docs / "guide" / "deep" / "page.md").write_text(
        "---\ntitle: Page\nsplit: page\n---\n## Notes {#notes}\n\n"
        "[FROM](../../copy/index.md#copy-from), "
        "[TO](./../../copy/index.md#copy-from-database-to), "
        "[phrase](../../copy/index.md#a--b-phrase), "
        "[other](../../copy/index.md#database), [notes](#notes), "
        "[site](https://serenedb.com/docs/copy#copy-from).\n\n"
        "As code: `[FROM](../../copy/index.md#copy-from)`.\n\n"
        "```md\n[code](../../copy/index.md#copy-from)\n```\n",
        encoding="utf-8")
    (docs / "lower.md").write_text(
        "---\ntitle: copy from\nsplit: headings\n---\n"
        "## copy_from {#cf}\n\nSee [it](#cf).\n", encoding="utf-8")
    out = tmp_path / "docs_data.cpp"
    r = _run(str(SCRIPTS / "generate_docs.py"), str(docs), str(out),
             "--tests-dir", str(REPO / "tests" / "sqllogic"))
    assert r.returncode == 0, r.stderr
    generated = out.read_text()
    assert ('| [`a \\|\\| b`](#COPY#Operators#a_\\|\\|_b "#a--b-or") | or |\n\n'
            "#### `a || b`\n\n#### `a && b`\n\n#### `a ## b`\n\n"
            'See [AND](#COPY#Operators#a_\\&\\&_b "#a--b-and"), '
            '[FROM](#copy--from "#copy-from"), '
            '[`ts_highlight`](#ts_highlight), [f](#ftext "#f"), '
            '[top](#copy "#top") and [twice](#twice "#twice-over").\n'
            ) in generated
    assert ('R"sdbdoc(## Notes\n\n'
            '[FROM](../../copy/index.md#copy--from "#copy-from"), '
            "[TO](./../../copy/index.md#copy-from-database--to "
            '"#copy-from-database-to"), '
            "[phrase](../../copy/index.md#COPY#Operators#a_\\\\#\\\\#_b "
            '"#a--b-phrase"), '
            "[other](../../copy/index.md#database), [notes](#notes), "
            "[site](https://serenedb.com/docs/copy#copy-from).\n\n"
            "As code: `[FROM](../../copy/index.md#copy-from)`.\n\n"
            '```md\n[code](../../copy/index.md#copy-from)\n```\n)sdbdoc"'
            ) in generated
    assert ('R"sdbdoc(See [it](#copy_from#copy_from "#cf").\n)sdbdoc"'
            in generated)


# Markup of the site that the shell would print as it is written: an HTML tag
# or a JSX component, a JSX attribute, a heading id or an admonition fence.
SITE_MARKUP_RE = re.compile(
    r"</?[A-Za-z][\w.-]*(?=[\s/>]|$)|useBaseUrl\(|className=|\{#[^}]*\}"
    r"|^(> ?)*:::")
CODE_SPAN_RE = re.compile(r"(?<!`)(`+)(?!`).+?(?<!`)\1(?!`)")
FENCE_RE = re.compile(r"^(>\s?)*\s*(```|~~~)")


def test_docs_corpus_keeps_no_site_markup() -> None:
    """Every page as it is embedded holds no site markup outside code. The
    cases above cover what the generator converts; this catches a page that
    writes something it does not know yet."""
    sys.path.insert(0, str(SCRIPTS))
    import generate_docs
    import sqllogic_snippets
    units = generate_docs.collect(
        REPO / "docs", sqllogic_snippets.load(REPO / "tests" / "sqllogic"),
        sqllogic_snippets.Report())
    leaks = set()
    for unit in units:
        fenced = False
        for line in unit.content.split("\n"):
            if FENCE_RE.match(line):
                fenced = not fenced
            elif not fenced and SITE_MARKUP_RE.search(
                    CODE_SPAN_RE.sub("", line)):
                leaks.add(f"{unit.path.split('#', 1)[0]}: {line}")
    assert units
    assert not leaks, sorted(leaks)[:5]


DESTINATION_ESCAPE_RE = re.compile(r"\\([!-/:-@\[-`{-~])")
HEADING_LINE_RE = re.compile(r"^(#{1,6}) (.*?)(?:\s*\{#([^}]*)\})?\s*$")


def _ascii_lower(text: str) -> str:
    return "".join(c.lower() if c.isascii() else c for c in text)


def _shell_slug(title: str) -> str:
    return "".join(c.lower() if c.isascii() and (c.isalnum() or c in "-_")
                   else "-" if c == " " else "" if c.isascii() else c
                   for c in title)


def _shell_opens(pages: dict, base: str, href: str) -> "str | None":
    target, _, anchor = DESTINATION_ESCAPE_RE.sub(r"\1", href).partition("#")
    if target and not target.endswith((".md", ".mdx")):
        return None
    parts = []
    for part in (base.split("/")[:-1] + target.split("/") if target
                 else base.split("/")):
        if part == "..":
            parts = parts[:-1]
        elif part not in ("", "."):
            parts.append(part)
    page = "/".join(parts)
    sections = sorted((unit for unit in pages.get(page, [])
                       if unit.path.startswith(page + "#")),
                      key=lambda unit: unit.path.encode("utf-8"))
    if any(unit.path == f"{page}#{anchor}" for unit in sections):
        return f"{page}#{anchor}"
    wanted = _ascii_lower(anchor)
    best, best_rank = page, 3
    for unit in sections:
        slug, title = _shell_slug(unit.title), _ascii_lower(unit.title)
        rank = (0 if slug == wanted
                else 1 if title == wanted or title.startswith(wanted + "(")
                else 2 if slug.startswith(wanted) else 3)
        if rank < best_rank:
            best, best_rank = unit.path, rank
    return best


def test_docs_corpus_opens_the_section_of_every_heading_id() -> None:
    sys.path.insert(0, str(SCRIPTS))
    import generate_docs
    import sqllogic_snippets
    snippets = sqllogic_snippets.load(REPO / "tests" / "sqllogic")
    units = generate_docs.collect(REPO / "docs", snippets,
                                  sqllogic_snippets.Report())
    pages = {}
    for unit in units:
        pages.setdefault(unit.path.split("#", 1)[0], []).append(unit)
    headed = {page: page_units for page, page_units in pages.items()
              if "#" in page_units[0].path}
    sections = {page: generate_docs.shell_sections(page_units)
                for page, page_units in headed.items()}
    keys = {}
    expected = {}
    for page, page_units in headed.items():
        meta, body = generate_docs.split_frontmatter(
            (REPO / "docs" / page).read_text(encoding="utf-8"))
        cleaned = generate_docs.clean(sqllogic_snippets.inline(
            body, snippets, page, sqllogic_snippets.Report()))
        keys[page] = generate_docs.split_units(
            page, meta.get("title") or Path(page).stem,
            *generate_docs.heading_ids(cleaned))[1]
        heads = []
        fenced = False
        for line in cleaned.split("\n"):
            if line.startswith("```"):
                fenced = not fenced
            elif not fenced and (match := HEADING_LINE_RE.match(line)):
                heads.append(match.group(3))
        targets = [unit.path for unit in page_units[1:]]
        if len(heads) == len(targets) + 1:
            targets.insert(0, page_units[0].path)
        assert len(heads) == len(targets), page
        for anchor, path in zip(heads, targets):
            if anchor:
                expected.setdefault((page, anchor), path)
    wrong = []
    for (page, anchor), path in expected.items():
        for base, target in ((page, ""), ("zz/yy/links.md", f"../../{page}")):
            link = generate_docs.follow_heading_ids(
                base, f"[x]({target}#{anchor})", sections, keys)
            href, _, title = link[len("[x]("):-1].partition(' "')
            opened = _shell_opens(headed, base, href)
            site = title[1:-1] if title else href.partition("#")[2]
            if opened != path or site != anchor:
                wrong.append(f"{base} -> {page}#{anchor}: {link} opens "
                             f"{opened} and the site at #{site}")
    assert expected
    assert not wrong, wrong[:5]
