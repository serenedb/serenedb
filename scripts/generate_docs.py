#!/usr/bin/env python3
import argparse
from builtins import tuple
import collections
import html
import pathlib
import posixpath
import re
import sys

import sqllogic_snippets

DELIMITER = "sdbdoc"
EXTENSIONS = {".md", ".mdx"}
# Where the site serves the docs, as kDocsSite in
# server/docs/docs_shell_backend.cpp. The site's static files (images and
# downloads) are served under it too: that is what useBaseUrl() resolves to.
DOCS_SITE = "https://serenedb.com/docs"

FRONTMATTER_RE = re.compile(r"\A---\n(.*?)\n---\n", re.S)
MDX_ESM_RE = re.compile(
    r"^(import\s.+\sfrom\s+['\"]|import\s+['\"]|export\s+(const|default|function|let|var)\b)")
FENCE_RE = re.compile(r"^\s*(```|~~~)")
COMPONENT_OPEN_RE = re.compile(r"^\s*<([A-Z][A-Za-z.]*)(\s[^>]*)?>\s*$")
COMPONENT_CLOSE_RE = re.compile(r"^\s*</([A-Z][A-Za-z.]*)>\s*$")
COMPONENT_SELF_CLOSING_RE = re.compile(r"<[A-Z][A-Za-z.]*(\s[^>]*)?/>")
HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.S)
# A line holding only <div ...>, </div> or an empty <div ...></div>: the
# wrappers and spacers the site styles a page with. What a wrapper holds (a
# table, say) is Markdown and stays.
DIV_LINE_RE = re.compile(r"^\s*(<div(\s[^>]*)?>(\s*</div>)?|</div>)\s*$")
WRAPPER_TAG_RE = re.compile(r"</?(details|summary)>")
JSX_STYLE_RE = re.compile(r"\s*style=\{\{[^}]*\}\}")
ADMONITION_OPEN_RE = re.compile(r"^:::(\w+)(?:[ \t]+(.*\S))?[ \t]*$")
ADMONITION_CLOSE_RE = re.compile(r"^:::[ \t]*$")
CALLOUT_OPEN_RE = re.compile(r"^\s*<DocCallout(\s[^>]*)?>\s*$")
CALLOUT_CLOSE_RE = re.compile(r"^\s*</DocCallout>\s*$")
# The label the site's DocCallout shows for a type when it has no title
# (serene_site: docusaurus/src/components/DocCallout); any other type shows
# "Note".
CALLOUT_LABELS = {
    "info": "Info", "tip": "Tip", "attention": "Attention", "bestPractice": "Best practice"}
# One attribute of a JSX tag: name="value", name='value' or name={expression}.
# A bare name such as download has no value and is not matched.
JSX_ATTR_RE = re.compile(r"""([A-Za-z][\w:-]*)\s*=\s*(?:"([^"]*)"|'([^']*)'|\{([^}]*)\})""")
# The {expression} the site uses for a static file: useBaseUrl("/images/a.png")
# or require("@site/static/images/a.png").default. Group 1 is the path.
ASSET_CALL_RE = re.compile(r"""^\s*(?:useBaseUrl|require)\(\s*["']([^"']*)["']\s*\)(?:\.default)?\s*$""")
IMG_OPEN_RE = re.compile(r"^\s*<img\s")
# A whole <img ...> or <img .../> tag, its lines joined. Group 1 is the
# attributes.
IMG_TAG_RE = re.compile(r"^\s*<img\s([^>]*?)/?>\s*$")
LINK_TAG_RE = re.compile(r"<a\s([^>]*)>(.*?)</a>")
STRONG_TAG_RE = re.compile(r"<strong>\s*(.*?)\s*</strong>")
EM_TAG_RE = re.compile(r"<em>\s*(.*?)\s*</em>")
# Code as a page writes it, whichever starts first: a Markdown code span (a run
# of backticks, then anything up to a run of the same length) or a <code> tag
# (group 2 is what it holds). Tags inside a code span are text and stay.
CODE_RE = re.compile(r"(?<!`)(`+)(?!`).+?(?<!`)\1(?!`)|<code>(.*?)</code>")
HIDDEN_SPAN_RE = re.compile(r"\0(\d+)\0")
BACKTICKS_RE = re.compile(r"`+")
# "## Setup {#setup}" -> "## Setup". #{1,6} is one to six literal hashes (the
# heading level); \{ and \} are literal braces; [^}]* is anything up to the
# closing brace. Group 1 is the heading without the anchor.
HEADING_ANCHOR_RE = re.compile(r"^(#{1,6}\s.*?)\s*\{#[^}]*\}\s*$")
# The same heading, split into its text (group 1) and its id (group 2).
PINNED_HEADING_RE = re.compile(r"^#{1,6}\s+(.*?)\s*\{#([^}]*)\}\s*$")
# A link with an anchor, "](../copy/index.md#copy-to)": group 1 is the page
# (empty for one on the same page), group 2 the anchor.
ANCHOR_LINK_RE = re.compile(r"\]\(([^)\s#]*)#([^)\s]+)\)")
# A fence, also one quoted by an admonition or a callout.
QUOTED_FENCE_RE = re.compile(r"^(>\s?)*\s*(```|~~~)")
BLANK_RUN_RE = re.compile(r"\n{3,}")


def split_frontmatter(text: str) -> tuple[dict[str, str], str]:
    match = FRONTMATTER_RE.match(text)
    if not match:
        return {}, text
    meta = {}
    for line in match.group(1).splitlines():
        key, sep, value = line.partition(":")
        if sep:
            meta[key.strip()] = value.strip().strip('"').strip("'")
    return meta, text[match.end():]


def jsx_attrs(tag: str) -> dict[str, str]:
    """The attributes of a JSX tag. An {expression} that names a static file
    (useBaseUrl or require) gives its path, any other expression nothing."""
    attrs = {}
    for name, double, single, expression in JSX_ATTR_RE.findall(tag):
        if expression:
            call = ASSET_CALL_RE.match(expression)
            attrs[name] = call.group(1) if call else ""
        else:
            attrs[name] = double or single
    return attrs


def site_url(path: str) -> str:
    """Where a link or image of a page points once the site has resolved it.
    The server only links .md pages itself, so a static file needs the full
    URL to reach the shell's Links list."""
    path = path.removeprefix("@site/static")
    if path.startswith("/") and not path.startswith("//"):
        return DOCS_SITE + path
    return path


def image_line(tag: str) -> str:
    """An <img> tag as a link to the image: a terminal cannot show it, but the
    text around it may refer to it, and the link opens it. The dark-theme twin
    of a light/dark pair is the same picture: it gives the empty string and its
    line is dropped. Anything but a whole tag is returned as it is."""
    match = IMG_TAG_RE.match(tag)
    if not match:
        return tag
    attrs = jsx_attrs(match.group(1))
    if "darkmode-img" in (attrs.get("className") or attrs.get("class") or "").split():
        return ""
    src = attrs.get("src", "")
    label = attrs.get("alt") or attrs.get("title") or src.rsplit("/", 1)[-1] or "image"
    label = "Image: " + label.replace("[", "\\[").replace("]", "\\]")
    return f"[{label}]({site_url(src)})" if src else label


def code_span(text: str, table: bool) -> str:
    """text as a Markdown code span. In a table row a | would end the cell,
    so it is escaped; GFM drops the backslash again."""
    if table:
        text = text.replace("|", "\\|")
    ticks = "`" * (max(map(len, BACKTICKS_RE.findall(text)), default=0) + 1)
    pad = " " if text.startswith("`") or text.endswith("`") else ""
    return f"{ticks}{pad}{text}{pad}{ticks}"


def markdown_link(match: re.Match) -> str:
    href = jsx_attrs(match.group(1)).get("href")
    return f"[{match.group(2)}]({site_url(href)})" if href else match.group(2)


def inline_markdown(line: str) -> str:
    """The inline HTML the pages use (<a>, <code>, <strong>, <em>) as Markdown:
    the shell prints HTML as it is written. Code is set aside first, a <code>
    tag already turned into a code span, so the other rules never touch what
    code shows and a page can still show a tag as code."""
    if "<" not in line:
        return line
    table = line.lstrip().startswith("|")
    spans = []

    def hide(match: re.Match) -> str:
        code = match.group(2)
        spans.append(match.group(0) if code is None else code_span(html.unescape(code), table))
        return f"\0{len(spans) - 1}\0"

    line = CODE_RE.sub(hide, line)
    line = STRONG_TAG_RE.sub(lambda m: f"**{m.group(1)}**" if m.group(1) else "", line)
    line = EM_TAG_RE.sub(lambda m: f"*{m.group(1)}*" if m.group(1) else "", line)
    line = LINK_TAG_RE.sub(markdown_link, line)
    # What was set aside came from the line as the page wrote it, so it holds
    # no placeholder and one pass puts everything back.
    return HIDDEN_SPAN_RE.sub(lambda m: spans[int(m.group(1))], line)


def clean(body: str) -> str:
    out = []
    depth = 0
    # What the lines are quoted for: None, "admonition" (:::note ... :::) or
    # "callout" (<DocCallout> ... </DocCallout>, at component depth callout).
    quote = None
    callout = 0
    fence = False
    image = []
    for line in HTML_COMMENT_RE.sub("", body).split("\n"):
        if FENCE_RE.match(line):
            fence = not fence
        elif fence:
            out.append(f"> {line}".rstrip() if quote else line.rstrip())
            continue
        elif MDX_ESM_RE.match(line) or DIV_LINE_RE.match(line):
            continue
        elif image and not line.strip():
            # No tag spans a blank line: what was collected is not an <img>
            # tag and goes on as text.
            out += [f"> {part}" if quote else part for part in image]
            image = []
        elif image or IMG_OPEN_RE.match(line):
            # An <img> tag may run over several lines: collect them up to its
            # ">".
            image.append(line.strip())
            if not line.rstrip().endswith(">"):
                continue
            line, image = image_line(" ".join(image)), []
            if not line:
                continue
        if not quote and (match := ADMONITION_OPEN_RE.match(line)):
            label = match.group(1).capitalize()
            if match.group(2):
                label += ": " + match.group(2)
            out += [f"> **{label}**", ">"]
            quote = "admonition"
            continue
        if not quote and (match := CALLOUT_OPEN_RE.match(line)) and not line.rstrip().endswith("/>"):
            # The same quote as an admonition, under the label the site shows.
            attrs = jsx_attrs(match.group(1) or "")
            label = attrs.get("title") or CALLOUT_LABELS.get(attrs.get("type"), "Note")
            out += [f"> **{label}**", ">"]
            depth += 1
            quote, callout = "callout", depth
            continue
        if (quote == "admonition" and ADMONITION_CLOSE_RE.match(line)) or (
                quote == "callout" and depth == callout and CALLOUT_CLOSE_RE.match(line)):
            while out[-1] == ">":
                out.pop()
            out.append("")
            if quote == "callout":
                depth -= 1
            quote = None
            continue
        if COMPONENT_OPEN_RE.match(line) and not line.rstrip().endswith("/>"):
            depth += 1
            continue
        if COMPONENT_CLOSE_RE.match(line):
            depth = max(depth - 1, 0)
            continue
        line = COMPONENT_SELF_CLOSING_RE.sub("", line)
        line = WRAPPER_TAG_RE.sub("", line)
        line = JSX_STYLE_RE.sub("", line)
        line = inline_markdown(line)
        line = HEADING_ANCHOR_RE.sub(r"\1", line)
        if depth > 0:
            line = line.strip()
        if line.strip() in ("", ">"):
            line = ""
        if not quote:
            out.append(line.rstrip())
        elif line or out[-1] != ">":
            # One ">" line stands for any run of blank lines in a quote.
            out.append(f"> {line}".rstrip())
    out += [f"> {part}" if quote else part for part in image]
    # Removing lines leaves holes: squeeze blank runs, trim blank edges, then
    # end with exactly one newline unless nothing is left at all.
    text = BLANK_RUN_RE.sub("\n\n", "\n".join(out)).strip("\n")
    return text + "\n" if text else ""


def shell_slug(title: str) -> str:
    """Slug() in server/docs/docs_search.cpp, what the shell matches a link's
    anchor against: ASCII letters and digits lowercased, "-", "_" and
    non-ASCII bytes kept, a space made "-", anything else dropped."""
    slug = bytearray()
    for byte in title.encode("utf-8"):
        if byte >= 0x80 or chr(byte).isalnum() or chr(byte) in "-_":
            slug.append(ord(chr(byte).lower()) if byte < 0x80 else byte)
        elif byte == ord(" "):
            slug.append(ord("-"))
    return slug.decode("utf-8")


def pinned_slugs(body: str) -> dict[str, str]:
    """Each heading id the page pins, "{#copy-from}", mapped to the shell's
    slug of that heading's title, "copy--from". An id is left out when
    another heading on the page has the same slug ("a || b" and "a && b" are
    both a--b): the shell would open the first of them."""
    slugs = {}
    seen = collections.Counter()
    fence = False
    for line in body.split("\n"):
        if FENCE_RE.match(line):
            fence = not fence
        elif not fence and (match := HEADING_RE.match(line)):
            pinned = PINNED_HEADING_RE.match(line)
            slug = shell_slug(plain_heading(inline_markdown(pinned.group(1) if pinned else match.group(2))))
            seen[slug] += 1
            if pinned:
                slugs[pinned.group(2)] = slug
    return {pin: slug for pin, slug in slugs.items() if seen[slug] == 1}


def linked_page(page: str, target: str, pages: dict) -> "str | None":
    """The page of pages a link from page to target opens, trying the
    suffixes the shell tries, or None for a link outside the docs."""
    if not target:
        return page
    if "://" in target or target.startswith(("/", "mailto:")):
        return None
    path = posixpath.normpath(posixpath.join(posixpath.dirname(page), target))
    candidates = [path] + [path + suffix for suffix in (".md", ".mdx", "/index.md", "/index.mdx")]
    return next((candidate for candidate in candidates if candidate in pages), None)


def follow_pinned_ids(page: str, content: str, pinned: dict[str, dict[str, str]]) -> str:
    """Points each link to a pinned heading id at the shell's slug of that
    heading. clean() strips the ids, and the shell finds a section only by the
    slug of its title, so #copy-from would open the top of the page (or,
    by its slug prefix, "COPY FROM DATABASE ... TO") instead of "COPY ...
    FROM"."""
    def follow(match: re.Match) -> str:
        target, anchor = match.groups()
        slug = pinned.get(linked_page(page, target, pinned) or "", {}).get(anchor)
        return f"]({target}#{slug})" if slug else match.group(0)

    lines = content.split("\n")
    fence = False
    for i, line in enumerate(lines):
        if QUOTED_FENCE_RE.match(line):
            fence = not fence
        elif not fence:
            lines[i] = ANCHOR_LINK_RE.sub(follow, line)
    return "\n".join(lines)


SPLIT_MODES = ("headings", "page")
HEADING_RE = re.compile(r"^(#{1,6}) (.*)$")
# Inline formatting that must not leak into keys and titles: `code`, **bold**,
# *em*, [text](url). Underscores stay: date_part and datepart are different.
INLINE_CODE_RE = re.compile(r"`([^`]*)`")
LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")
EMPHASIS_RE = re.compile(r"\*\*|\*")


def plain_heading(text: str) -> str:
    text = INLINE_CODE_RE.sub(r"\1", text)
    text = LINK_RE.sub(r"\1", text)
    return EMPHASIS_RE.sub("", text).strip()


def key_segment(title: str) -> str:
    return title.replace("#", "\\#").replace(" ", "_")


class Unit:
    def __init__(self, path, title, breadcrumb, content):
        self.path, self.title, self.breadcrumb, self.content = path, title, breadcrumb, content

    def fields(self):
        return (self.path, self.title, self.breadcrumb, self.content)


def split_units(page: str, title: str, body: str) -> list[Unit]:
    # Heading rows only, each holding everything nested under it. The page
    # title acts as the H1: "page#Title" carries the whole page and every
    # heading key hangs under it; a body H1 repeating the title is folded in.
    lines = body.split("\n")
    heads = []
    in_fence = False
    for i, line in enumerate(lines):
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        match = None if in_fence else HEADING_RE.match(line)
        if match:
            heads.append((i, len(match.group(1)), plain_heading(match.group(2))))
    root = page + "#" + key_segment(title)
    units = [Unit(root, title, "", body)]
    stack = []
    for n, (i, level, heading) in enumerate(heads):
        if level == 1 and heading == title:
            continue
        end = next((j for j, l, _ in heads[n + 1:] if l <= level), len(lines))
        content = "\n".join(lines[i + 1:end]).strip("\n")
        stack = [s for s in stack if s[0] < level] + [(level, heading)]
        key = root + "".join("#" + key_segment(h) for _, h in stack)
        breadcrumb = " / ".join([title] + [h for _, h in stack[:-1]])
        units.append(Unit(key, heading, breadcrumb, content + "\n" if content else ""))
    return units


def collect(docs_dir: pathlib.Path, snippets: dict, report) -> list[Unit]:
    units = []
    errors = []
    # Every page is cleaned first: a link is pointed at a heading id only
    # once the page pinning it is read.
    pages = []
    pinned = {}
    for path in sorted(docs_dir.rglob("*")):
        if not path.is_file() or path.suffix not in EXTENSIONS:
            continue
        rel = path.relative_to(docs_dir).as_posix()
        meta, body = split_frontmatter(path.read_text(encoding="utf-8"))
        if meta.get("draft") == "true":
            continue
        if "slug" in meta:
            errors.append(f"{rel}: frontmatter key 'slug' is not supported, links to the site follow the path "
                          f"(name the page index.md to serve its folder)")
            continue
        title = meta.get("title") or path.stem
        split = meta.get("split")
        if split not in SPLIT_MODES:
            errors.append(f"{rel}: frontmatter key 'split' must be one of {', '.join(SPLIT_MODES)} (got {split!r})")
            continue
        body = sqllogic_snippets.inline(body, snippets, rel, report)
        pinned[rel] = pinned_slugs(body)
        pages.append((rel, title, split, clean(body)))
    for rel, title, split, content in pages:
        content = follow_pinned_ids(rel, content, pinned)
        page_units = split_units(rel, title, content) if split == "headings" else [Unit(rel, title, "", content)]
        seen = set()
        for unit in page_units:
            if unit.path in seen:
                errors.append(f"{rel}: duplicate section key {unit.path!r}")
            seen.add(unit.path)
            for field in unit.fields():
                if f"){DELIMITER}\"" in field:
                    errors.append(f"{rel}: contains the raw string delimiter ){DELIMITER}\"")
        units += page_units
    if errors:
        sys.exit("\n".join(errors))
    return units


def raw(text: str) -> str:
    return f'R"{DELIMITER}({text}){DELIMITER}"'


def human_size(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KiB"
    return f"{size / (1024 * 1024):.1f} MiB"


def render(units: list[Unit]) -> str:
    out = ['#include "docs/builder/docs_data.h"', "", "namespace sdb::docs {"]
    if units:
        out += ["namespace {", "", "constexpr Doc kDocs[] = {"]
        for u in units:
            # f-string: {{ and }} emit the literal braces of the C++ initializer.
            out.append(f"  {{{raw(u.path)}, {raw(u.title)}, {raw(u.breadcrumb)}, {raw(u.content)}}},")
        out += ["};", "", "}  // namespace", ""]
        out.append("std::span<const Doc> GetDocs() { return kDocs; }")
    else:
        out += ["", "std::span<const Doc> GetDocs() { return {}; }"]
    out += [
        "",
        "}  // namespace sdb::docs",
        "",
    ]
    return "\n".join(out)


def report_snippets(report) -> None:
    for page, ids in sorted(report.empty.items()):
        print(f"{page}: {len(ids)} SqlLogicTest tags resolved to an empty snippet", file=sys.stderr)
    missing = sorted((page, ids) for page, ids in report.missing.items())
    total = sum(len(ids) for _, ids in missing)
    if not total:
        return
    print(f"warning: {total} SqlLogicTest ids have no matching test marker", file=sys.stderr)
    for page, ids in missing:
        print(f"  {page}: {', '.join(sorted(ids))}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("docs_dir", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--tests-dir", type=pathlib.Path, default=None)
    args = parser.parse_args()
    if not args.docs_dir.is_dir():
        sys.exit(f"{args.docs_dir}: not a directory")
    tests_dir = args.tests_dir or args.docs_dir.parent / "tests" / "sqllogic"
    if not tests_dir.is_dir():
        sys.exit(f"{tests_dir}: not a directory (pass --tests-dir)")
    snippets = sqllogic_snippets.load(tests_dir)
    if not snippets:
        sys.exit(f"{tests_dir}: no DOCS_TEST markers found")
    report = sqllogic_snippets.Report()
    units = collect(args.docs_dir, snippets, report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(units), encoding="utf-8")
    pages = len({u.path.split("#", 1)[0] for u in units})
    size = sum(len(field.encode("utf-8")) for u in units for field in u.fields())
    report_snippets(report)
    print(f"generated {args.output}: {pages} pages, {len(units)} rows, {report.inlined} snippets, "
          f"{human_size(size)} ({size} bytes) of embedded text")


if __name__ == "__main__":
    main()
