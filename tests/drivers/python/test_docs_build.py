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
    sizes = {"docs/small": 10, "docs/large": 3 * 1024 * 1024 // 2,
             "objects/medium": 2048}
    for name, size in sizes.items():
        (index / name).parent.mkdir(parents=True, exist_ok=True)
        (index / name).write_bytes(b"x" * size)
    out = tmp_path / "docs_index_data.cpp"
    r = _run(str(SCRIPTS / "generate_docs_index.py"), str(index), str(out))
    assert r.returncode == 0, r.stderr
    lines = r.stdout.splitlines()
    assert lines[0] == (f"embedded docs index: 3 files, 1.5 MiB "
                        f"({sum(sizes.values())} bytes) -> {out}")
    assert [line.split() for line in lines[1:]] == [
        ["docs/large", "1.5", "MiB"], ["objects/medium", "2.0", "KiB"],
        ["docs/small", "10", "B"]]
    generated = out.read_text()
    assert "#embed" in generated
    assert "GetDocsIndex()" in generated and "GetObjectsIndex()" in generated


def test_index_embedding_needs_both_indexes(tmp_path: Path) -> None:
    index = tmp_path / "index"
    (index / "docs").mkdir(parents=True)
    (index / "docs" / "segments_1").write_bytes(b"x")
    r = _run(str(SCRIPTS / "generate_docs_index.py"), str(index),
             str(tmp_path / "docs_index_data.cpp"))
    assert r.returncode == 1
    assert "no index files under" in r.stderr and "objects" in r.stderr


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
